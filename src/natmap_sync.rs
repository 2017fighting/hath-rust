//! NAT port-sync background task.
//!
//! `natmap` (running on the router) publishes a NAT mapping whose external port
//! can change over time. The central H@H tracker records a single port per
//! client and rejects settings changes while the client is active. This task
//! periodically polls the natmap API, compares its external port against the
//! port the tracker currently has on record, and when they differ runs the
//! proven suspend → update-web-settings → re-notify-start flow so the tracker's
//! recorded port tracks the live external port.
//!
//! It is a faithful Rust port of the previous `natmap-sidecar` Python service,
//! reusing `RPCClient` for the RPC actions and adding only the router poll and
//! the web-settings POST.

use std::{collections::HashMap, sync::Arc, time::Duration};

use log::{error, info, warn};
use reqwest::{
    Client,
    header::{CONTENT_TYPE, COOKIE},
};
use serde::Deserialize;
use tokio::time::{MissedTickBehavior, interval};

use crate::{
    metrics::{LABEL_NATMAP_SYNC_APPLIED, LABEL_NATMAP_SYNC_ERROR, LABEL_NATMAP_SYNC_NO_CHANGE, Metrics},
    rpc::RPCClient,
    util::create_http_client,
};

/// Central settings page. The `cid` query parameter is the H@H Client ID,
/// sourced at runtime from `RPCClient::id` rather than hardcoded.
const SETTINGS_URL: &str = "https://e-hentai.org/hentaiathome.php";

/// All configuration the port-sync task needs. When any field is unset at
/// startup the task is not spawned, so this feature is purely opt-in.
#[derive(Clone)]
pub struct NatmapConfig {
    /// Base URL of the router-side natmap API, e.g. `http://192.168.9.1/natmap`.
    pub api_endpoint: String,
    /// natmap instance id; its mapping is fetched from `<api_endpoint>/<instance>.json`.
    pub instance: String,
    /// Forum cookie `ipb_member_id`, used to authenticate the settings POST.
    pub ipb_member_id: String,
    /// Forum cookie `ipb_pass_hash`, used to authenticate the settings POST.
    pub ipb_pass_hash: String,
    /// How often to poll natmap and reconcile.
    pub poll_interval: Duration,
}

/// A NAT mapping record as published by natmap.
#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct NatmapMapping {
    pub ip: String,
    pub port: u16,
    pub inner_port: u16,
    pub protocol: String,
}

/// The subset of the tracker's `client_settings` response that the settings
/// page needs, with the byte→KB/GB/MB conversions from the Python sidecar
/// preserved (`/1000` for KB, `/1024^3` for GB, `/1024^2` for MB).
#[derive(Debug, PartialEq)]
pub struct HathWebSettings {
    pub port: u16,
    pub throttle_kb: f64,
    pub disklimit_gb: f64,
    pub enable_bwm: bool,
    pub mthbw_cap: u64,
    pub diskremaining_mb: f64,
}

impl HathWebSettings {
    /// Build from the `client_settings` key=value map, overriding the port with
    /// the new external port. Mirrors `load_from_rpc_client_settings` +
    /// `mthbw_cap=0` default from the Python sidecar. Missing fields default to
    /// 0/false so a partial server response cannot abort the update.
    pub fn from_settings_map(map: &HashMap<String, String>, port: u16) -> Self {
        let u64_of = |k: &str| map.get(k).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
        let disable_bwm = map.get("disable_bwm").and_then(|v| v.parse::<bool>().ok()).unwrap_or(false);

        let throttle_bytes = u64_of("throttle_bytes") as f64;
        let disklimit_bytes = u64_of("disklimit_bytes") as f64;
        let diskremaining_bytes = u64_of("diskremaining_bytes") as f64;

        Self {
            port,
            throttle_kb: throttle_bytes / 1000.0,
            disklimit_gb: disklimit_bytes / 1024.0 / 1024.0 / 1024.0,
            enable_bwm: !disable_bwm,
            mthbw_cap: 0,
            diskremaining_mb: diskremaining_bytes / 1024.0 / 1024.0,
        }
    }

    /// Build the form body for the settings POST. Mirrors
    /// `dump_for_update_websettings` from the Python sidecar, including the
    /// `f_enable_bwm=on` checkbox only when bandwidth management is enabled.
    pub fn to_form(&self) -> Vec<(String, String)> {
        let mut form = vec![
            ("settings".to_string(), "1".to_string()),
            ("f_port".to_string(), self.port.to_string()),
            ("f_cname".to_string(), format!("hath-{}", self.port)),
            ("f_throttle_KB".to_string(), format!("{}", self.throttle_kb)),
            ("f_disklimit_GB".to_string(), format!("{}", self.disklimit_gb)),
            ("p_mthbwcap".to_string(), self.mthbw_cap.to_string()),
            ("f_diskremaining_MB".to_string(), format!("{}", self.diskremaining_mb)),
            ("subbmit".to_string(), "Apply Settings".to_string()),
        ];
        if self.enable_bwm {
            form.push(("f_enable_bwm".to_string(), "on".to_string()));
        }
        form
    }
}

/// The long-running port-sync task. Spawn one per process (or none, when the
/// config is incomplete).
pub struct NatmapSync {
    config: NatmapConfig,
    client: Arc<RPCClient>,
    http: Client,
    metrics: Arc<Metrics>,
}

impl NatmapSync {
    pub fn new(config: NatmapConfig, client: Arc<RPCClient>, metrics: Arc<Metrics>) -> Self {
        // Dedicated HTTP client: short timeout, no proxy. The natmap API is on
        // the LAN and the settings page is reached directly from a no-proxy node.
        let http = create_http_client(Duration::from_secs(10), None);
        Self {
            config,
            client,
            http,
            metrics,
        }
    }

    /// Run the poll loop until the RPC client stops running.
    pub async fn run(self) {
        // First tick fires immediately, so a stale port is corrected at startup.
        let mut interval = interval(self.config.poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if !self.client.is_running() {
                break;
            }
            self.cycle().await;
        }
    }

    /// One reconciliation pass.
    async fn cycle(&self) {
        // 1. Read the live NAT mapping.
        let mapping = match self.fetch_natmap().await {
            Some(mapping) => mapping,
            None => {
                warn!("natmap: instance `{}` not registered or unreachable; skipping", self.config.instance);
                self.inc_sync(LABEL_NATMAP_SYNC_ERROR);
                return;
            }
        };
        self.metrics.natmap_port.set(mapping.port as u64);

        // 2. Read the port the tracker currently has on record.
        let map = match self.client.fetch_client_settings().await {
            Some(map) => map,
            None => {
                self.inc_sync(LABEL_NATMAP_SYNC_ERROR);
                return;
            }
        };
        let registered_port = map.get("port").and_then(|v| v.parse::<u16>().ok());
        if let Some(port) = registered_port {
            self.metrics.natmap_registered_port.set(port as u64);
        } else {
            warn!("natmap: `port` missing from client_settings response");
        }

        // 3. Equal → nothing to do.
        if registered_port == Some(mapping.port) {
            info!("natmap: external port {} matches registered port; no change", mapping.port);
            self.inc_sync(LABEL_NATMAP_SYNC_NO_CHANGE);
            return;
        }

        // 4. Differ → suspend, update the settings page, re-notify start.
        info!(
            "natmap: external port {} differs from registered port {}; updating",
            mapping.port,
            registered_port.map(|p| p.to_string()).unwrap_or_else(|| "?".to_string())
        );
        let settings = HathWebSettings::from_settings_map(&map, mapping.port);

        // The settings page rejects changes while the client is active, so the
        // order is mandatory: suspend first, then POST.
        self.client.suspend().await;
        if !self.update_port(&settings).await {
            // POST failed: leave the client suspended and let the next cycle
            // retry, matching the sidecar's behavior. The RPC client itself
            // stays alive (is_running is untouched).
            error!("natmap: settings page update failed; will retry next cycle");
            self.inc_sync(LABEL_NATMAP_SYNC_ERROR);
            return;
        }
        self.client.client_start().await;
        self.metrics.natmap_registered_port.set(mapping.port as u64);
        self.inc_sync(LABEL_NATMAP_SYNC_APPLIED);
        info!("natmap: registered port updated to {}", mapping.port);
    }

    /// GET `<api_endpoint>/<instance>.json`. Returns `None` on 404 (instance not
    /// registered) or any transport/parse error.
    async fn fetch_natmap(&self) -> Option<NatmapMapping> {
        let url = format!("{}/{instance}.json", self.config.api_endpoint.trim_end_matches('/'), instance = self.config.instance);
        let resp = match self.http.get(&url).send().await {
            Ok(resp) => resp,
            Err(err) => {
                warn!("natmap: GET {} failed: {}", url, err);
                return None;
            }
        };
        if resp.status().as_u16() == 404 {
            return None;
        }
        if let Err(err) = resp.error_for_status_ref() {
            warn!("natmap: {} returned error status: {}", url, err);
            return None;
        }
        match resp.text().await {
            Ok(body) => match serde_json::from_str::<NatmapMapping>(&body) {
                Ok(mapping) => Some(mapping),
                Err(err) => {
                    warn!("natmap: failed to parse mapping JSON: {}", err);
                    None
                }
            },
            Err(err) => {
                warn!("natmap: failed to read mapping body: {}", err);
                None
            }
        }
    }

    /// POST the new settings to the central settings page. Returns false on any
    /// failure so the caller leaves the client suspended for a next-cycle retry.
    async fn update_port(&self, settings: &HathWebSettings) -> bool {
        let url = format!("{}?cid={}&act=settings", SETTINGS_URL, self.client.id());
        let cookie = format!("ipb_member_id={}; ipb_pass_hash={}", self.config.ipb_member_id, self.config.ipb_pass_hash);
        // Encode the form manually: this reqwest build uses default-features = false,
        // so RequestBuilder::form(...) is unavailable.
        let body = serde_urlencoded::to_string(settings.to_form()).unwrap_or_default();
        match self
            .http
            .post(&url)
            .header(COOKIE, cookie)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
        {
            Ok(resp) => {
                if resp.status().is_success() {
                    true
                } else {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    error!("natmap: settings POST returned status={} body={}", status, body);
                    false
                }
            }
            Err(err) => {
                error!("natmap: settings POST failed: {}", err);
                false
            }
        }
    }

    fn inc_sync(&self, labels: crate::metrics::NatmapSyncLabels) {
        self.metrics.natmap_sync_total.get_or_create(&labels).inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rpc_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn web_settings_convert_units_and_override_port() {
        let map = rpc_map(&[
            ("port", "12345"),
            ("throttle_bytes", "1000000"),        // 1000 KB
            ("disklimit_bytes", "5368709120"),    // 5 GiB
            ("diskremaining_bytes", "524288000"), // 500 MiB
            ("disable_bwm", "false"),
        ]);
        let s = HathWebSettings::from_settings_map(&map, 9999);
        assert_eq!(s.port, 9999); // overridden, not 12345
        assert_eq!(s.throttle_kb, 1000.0);
        assert_eq!(s.disklimit_gb, 5.0);
        assert_eq!(s.diskremaining_mb, 500.0);
        assert!(s.enable_bwm);
        assert_eq!(s.mthbw_cap, 0);
    }

    #[test]
    fn web_settings_enable_bwm_inverts_disable() {
        let map = rpc_map(&[("disable_bwm", "true")]);
        let s = HathWebSettings::from_settings_map(&map, 1);
        assert!(!s.enable_bwm);
    }

    #[test]
    fn web_settings_missing_fields_default_zero() {
        let s = HathWebSettings::from_settings_map(&HashMap::new(), 7);
        assert_eq!(s.port, 7);
        assert_eq!(s.throttle_kb, 0.0);
        assert_eq!(s.disklimit_gb, 0.0);
        assert_eq!(s.diskremaining_mb, 0.0);
        assert!(s.enable_bwm); // disable_bwm missing → false → enable_bwm true
    }

    #[test]
    fn web_settings_form_has_required_fields_and_bwm() {
        let s = HathWebSettings {
            port: 9999,
            throttle_kb: 1000.0,
            disklimit_gb: 5.0,
            enable_bwm: true,
            mthbw_cap: 0,
            diskremaining_mb: 500.0,
        };
        let form: HashMap<String, String> = s.to_form().into_iter().collect();
        assert_eq!(form.get("settings"), Some(&"1".to_string()));
        assert_eq!(form.get("f_port"), Some(&"9999".to_string()));
        assert_eq!(form.get("f_cname"), Some(&"hath-9999".to_string()));
        assert_eq!(form.get("f_throttle_KB"), Some(&"1000".to_string()));
        assert_eq!(form.get("f_disklimit_GB"), Some(&"5".to_string()));
        assert_eq!(form.get("f_diskremaining_MB"), Some(&"500".to_string()));
        assert_eq!(form.get("p_mthbwcap"), Some(&"0".to_string()));
        assert_eq!(form.get("f_enable_bwm"), Some(&"on".to_string()));
        assert_eq!(form.get("subbmit"), Some(&"Apply Settings".to_string()));
    }

    #[test]
    fn web_settings_form_omits_bwm_when_disabled() {
        let s = HathWebSettings {
            port: 1,
            throttle_kb: 0.0,
            disklimit_gb: 0.0,
            enable_bwm: false,
            mthbw_cap: 0,
            diskremaining_mb: 0.0,
        };
        let form: HashMap<String, String> = s.to_form().into_iter().collect();
        assert!(!form.contains_key("f_enable_bwm"));
    }

    #[test]
    fn natmap_mapping_parses_json() {
        let json = r#"{"ip":"203.0.113.10","port":12345,"inner_port":12345,"protocol":"tcp"}"#;
        let m: NatmapMapping = serde_json::from_str(json).unwrap();
        assert_eq!(m.ip, "203.0.113.10");
        assert_eq!(m.port, 12345);
        assert_eq!(m.inner_port, 12345);
        assert_eq!(m.protocol, "tcp");
    }
}
