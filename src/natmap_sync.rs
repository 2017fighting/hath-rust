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
//!
//! Two mapping sources exist, in this order:
//!
//! 1. the **punch file** (`HATH_PUNCH_FILE`): the `punch.json` the punch
//!    controller mounts from the `<name>-punch` ConfigMap. This is the source
//!    the declared punch writes (ADR-0044); the file is re-read every pass, so
//!    a deleted ConfigMap (punch down) is visible to a running client.
//! 2. the **natmap HTTP API** (`HATH_NATMAP_API_ENDPOINT`/`HATH_NATMAP_INSTANCE`):
//!    kept for compatibility with the router package the controller replaces.
//!
//! The file wins when it yields a mapping; otherwise the HTTP source is polled.
//! A missing or unparsable file is *no mapping from that source this pass*, not
//! a fatal error: it is logged (rate-limited to a change of state, not every
//! tick) and the retry loop of the caller handles it.

use std::{
    collections::HashMap,
    error::Error,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use log::{error, info, warn};
use parking_lot::Mutex;
use reqwest::{
    Client, Proxy,
    header::{CONTENT_TYPE, COOKIE},
};
use serde::Deserialize;
use tokio::time::{MissedTickBehavior, interval, sleep};

use crate::{
    metrics::{LABEL_NATMAP_SYNC_APPLIED, LABEL_NATMAP_SYNC_ERROR, LABEL_NATMAP_SYNC_NO_CHANGE, Metrics},
    rpc::RPCClient,
    util::create_http_client,
};

/// Central settings page. The `cid` query parameter is the H@H Client ID,
/// sourced at runtime from `RPCClient::id` rather than hardcoded.
const SETTINGS_URL: &str = "https://e-hentai.org/hentaiathome.php";

/// Initial delay before the first pre-startup retry. Doubles each attempt,
/// up to `STARTUP_SYNC_MAX`.
const STARTUP_SYNC_INITIAL: Duration = Duration::from_secs(5);
/// Upper bound for the pre-startup retry backoff.
const STARTUP_SYNC_MAX: Duration = Duration::from_secs(300);

/// All configuration the port-sync task needs. It is built only when at least
/// one mapping source is configured; when none is, the task is not spawned and
/// this feature stays off.
#[derive(Clone)]
pub struct NatmapConfig {
    /// Path of the punch file (`punch.json`) written by the punch controller and
    /// mounted from the `<name>-punch` ConfigMap. Read first on every pass; when
    /// it is absent or unparsable the HTTP source below is polled instead.
    pub punch_file: Option<PathBuf>,
    /// Base URL of the router-side natmap API, e.g. `http://192.168.9.1/natmap`.
    /// Only a source together with `instance`.
    pub api_endpoint: Option<String>,
    /// natmap instance id; its mapping is fetched from `<api_endpoint>/<instance>.json`.
    pub instance: Option<String>,
    /// Forum cookie `ipb_member_id`, used to authenticate the settings POST.
    pub ipb_member_id: String,
    /// Forum cookie `ipb_pass_hash`, used to authenticate the settings POST.
    pub ipb_pass_hash: String,
    /// How often to poll the mapping source and reconcile.
    pub poll_interval: Duration,
}

impl NatmapConfig {
    /// The natmap HTTP source, when both halves of it are configured. A half
    /// configured pair is not a source: it cannot be polled.
    fn http_source(&self) -> Option<(&str, &str)> {
        match (self.api_endpoint.as_deref(), self.instance.as_deref()) {
            (Some(endpoint), Some(instance)) => Some((endpoint, instance)),
            _ => None,
        }
    }

    /// The URL the HTTP source polls, when it is configured.
    fn http_url(&self) -> Option<String> {
        let (endpoint, instance) = self.http_source()?;
        Some(format!("{}/{instance}.json", endpoint.trim_end_matches('/')))
    }

    fn punch_file_display(&self) -> String {
        match &self.punch_file {
            Some(path) => path.display().to_string(),
            None => "<unset>".to_string(),
        }
    }

    /// Where the mapping is read from, in precedence order, for logs. Says the
    /// same thing the source selection does, so a log line and the behaviour
    /// cannot drift apart.
    pub fn source_summary(&self) -> String {
        match (self.punch_file.is_some(), self.http_url()) {
            (true, Some(url)) => format!("punch file {} first, then the natmap API {url}", self.punch_file_display()),
            (true, None) => format!("punch file {}", self.punch_file_display()),
            (false, Some(url)) => format!("natmap API {url}"),
            (false, None) => "no mapping source configured".to_string(),
        }
    }
}

/// Which source produced a pass's mapping. Logged on change, so a switch
/// between the file and the HTTP API is visible without a line per tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MappingSource {
    PunchFile,
    NatmapHttp,
}

/// Why the punch file produced no mapping this pass. Every variant means the
/// same thing to the caller ("no mapping from the file"); they differ only in
/// what the rate-limited warning says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PunchFileProblem {
    Missing,
    Unreadable,
    Malformed,
}

impl PunchFileProblem {
    fn describe(self) -> &'static str {
        match self {
            PunchFileProblem::Missing => "is absent",
            PunchFileProblem::Unreadable => "could not be read",
            PunchFileProblem::Malformed => "is not a punch document",
        }
    }
}

/// What the last pass logged, so a steady state is quiet: one line when the
/// source changes, one line when the punch file starts or stops producing a
/// mapping.
#[derive(Default)]
struct SourceLog {
    source: Option<MappingSource>,
    file_problem: Option<PunchFileProblem>,
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

    /// Build the form body for the settings POST. Field names and the shape
    /// match the browser form exactly (verified against a captured POST:
    /// `submit`, not `subbmit`, and `f_enable_bwm=on` only when bandwidth
    /// management is enabled). The server silently ignores malformed forms —
    /// it re-renders the page with HTTP 200 — so any deviation is invisible
    /// without the response verification in `update_port`.
    pub fn to_form(&self) -> Vec<(String, String)> {
        let mut form = vec![
            ("settings".to_string(), "1".to_string()),
            ("f_port".to_string(), self.port.to_string()),
            ("f_cname".to_string(), format!("hath-{}", self.port)),
            ("f_throttle_KB".to_string(), format!("{}", self.throttle_kb)),
            ("f_disklimit_GB".to_string(), format!("{}", self.disklimit_gb)),
            ("p_mthbwcap".to_string(), self.mthbw_cap.to_string()),
            ("f_diskremaining_MB".to_string(), format!("{}", self.diskremaining_mb)),
            ("submit".to_string(), "Apply Settings".to_string()),
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
    /// No-proxy client (LAN natmap API fetch).
    http: Client,
    /// Proxy-enabled client (e-hentai.org settings POST).
    http_proxy: Client,
    metrics: Arc<Metrics>,
    /// What was logged last pass — see `SourceLog`.
    log_state: Mutex<SourceLog>,
}

/// Outcome of a single reconciliation pass. `reconcile_once` does not call
/// `client_start` itself — the caller decides whether a re-notify is needed
/// (the background loop does; the pre-startup sync does not, since
/// `connect_check` runs next).
enum ReconcileResult {
    /// Registered port already matched the external port; nothing changed.
    NoChange,
    /// Registered port differed and was updated successfully.
    Applied,
    /// The pass did not complete (natmap unreachable, settings fetch failed,
    /// or the settings POST was rejected). Retryable.
    Failed,
}

impl NatmapSync {
    pub fn new(
        config: NatmapConfig,
        client: Arc<RPCClient>,
        metrics: Arc<Metrics>,
        proxy: Option<Proxy>,
        local_addr: Option<IpAddr>,
    ) -> Self {
        // Two HTTP clients:
        //   http       – no proxy, for the LAN-only natmap API fetch.
        //   http_proxy – with proxy (optional), for the e-hentai.org settings POST.
        if proxy.is_some() {
            info!("natmap: using proxy for e-hentai settings POST");
        }
        let http = create_http_client(Duration::from_secs(10), None, local_addr);
        let http_proxy = create_http_client(Duration::from_secs(10), proxy, local_addr);
        Self {
            config,
            client,
            http,
            http_proxy,
            metrics,
            log_state: Mutex::new(SourceLog::default()),
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

    /// Run reconciliation passes until one succeeds (`NoChange` or `Applied`),
    /// retrying with exponential backoff. Called before `connect_check` so the
    /// client comes online with the correct registered port. Never gives up:
    /// every failure mode is retried. Responds to Ctrl+C during sleep so the
    /// process can be interrupted during a stalled retry loop.
    ///
    /// Returns `true` when the sync completed successfully, `false` if the
    /// user interrupted with Ctrl+C before it finished.
    pub async fn sync_until_ready(&self) -> bool {
        info!("natmap: running pre-startup port-sync...");
        let mut delay = STARTUP_SYNC_INITIAL;
        loop {
            match self.reconcile_once().await {
                ReconcileResult::NoChange | ReconcileResult::Applied => {
                    info!("natmap: pre-startup port-sync complete");
                    return true;
                }
                ReconcileResult::Failed => {
                    warn!("natmap: pre-startup sync did not complete; retrying in {:?}", delay);
                    let interrupted = tokio::select! {
                        _ = sleep(delay) => false,
                        _ = tokio::signal::ctrl_c() => {
                            info!("natmap: pre-startup sync interrupted by Ctrl+C");
                            true
                        }
                    };
                    if interrupted {
                        return false;
                    }
                    delay = delay.saturating_mul(2).min(STARTUP_SYNC_MAX);
                }
            }
        }
    }

    /// One reconciliation pass. Reads the live NAT mapping and the tracker's
    /// registered port; if they differ, suspends and updates the settings
    /// page. Does NOT call `client_start` — the caller decides.
    async fn reconcile_once(&self) -> ReconcileResult {
        // 1. Read the live NAT mapping (punch file first, natmap API second).
        let mapping = match self.fetch_mapping().await {
            Some(mapping) => mapping,
            None => {
                warn!("natmap: no mapping this pass from {}; skipping", self.config.source_summary());
                self.inc_sync(LABEL_NATMAP_SYNC_ERROR);
                return ReconcileResult::Failed;
            }
        };
        self.metrics.natmap_port.set(mapping.port as u64);

        // 2. Read the port the tracker currently has on record.
        let map = match self.client.fetch_client_settings().await {
            Some(map) => map,
            None => {
                self.inc_sync(LABEL_NATMAP_SYNC_ERROR);
                return ReconcileResult::Failed;
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
            return ReconcileResult::NoChange;
        }

        // 4. Differ → suspend, update the settings page.
        info!(
            "natmap: external port {} differs from registered port {}; updating",
            mapping.port,
            registered_port.map(|p| p.to_string()).unwrap_or_else(|| "?".to_string())
        );
        let settings = HathWebSettings::from_settings_map(&map, mapping.port);

        // The settings page rejects changes while the client is active, so the
        // order is mandatory: suspend first, then POST. Pre-startup this also
        // clears a stale server-side session from an unclean shutdown.
        self.client.suspend().await;
        if !self.update_port(&settings).await {
            // POST failed: leave the client suspended and let the caller retry.
            // The RPC client itself stays alive (is_running is untouched).
            error!("natmap: settings page update failed; will retry");
            self.inc_sync(LABEL_NATMAP_SYNC_ERROR);
            return ReconcileResult::Failed;
        }
        self.metrics.natmap_registered_port.set(mapping.port as u64);
        self.inc_sync(LABEL_NATMAP_SYNC_APPLIED);
        info!("natmap: registered port updated to {}", mapping.port);
        ReconcileResult::Applied
    }

    /// One reconciliation pass followed by a re-notify-start when the port was
    /// changed. Used by the background poll loop.
    async fn cycle(&self) {
        if matches!(self.reconcile_once().await, ReconcileResult::Applied) {
            // We suspended an active client to update its port; bring it back.
            self.client.client_start().await;
        }
    }

    /// The mapping for one pass: the punch file wins, the natmap HTTP API is
    /// the fallback. `None` when neither source produced a mapping — which the
    /// caller treats as a failed pass (retried, never fatal).
    async fn fetch_mapping(&self) -> Option<NatmapMapping> {
        let file_mapping = match &self.config.punch_file {
            Some(path) => match self.read_punch_file(path).await {
                Ok(mapping) => {
                    self.note_file_problem(None, "");
                    Some(mapping)
                }
                Err((problem, detail)) => {
                    self.note_file_problem(Some(problem), &detail);
                    None
                }
            },
            None => None,
        };
        // Only poll the HTTP source when the file did not answer — the file is
        // the source of a declared punch, and a fallback that runs anyway would
        // make a healthy file indistinguishable from a slow one in the logs.
        let http_mapping = match (file_mapping.is_none(), self.config.http_source()) {
            (true, Some(_)) => self.fetch_natmap().await,
            _ => None,
        };
        match select_mapping(file_mapping, http_mapping) {
            Some((mapping, source)) => {
                self.note_source(source);
                Some(mapping)
            }
            None => None,
        }
    }

    /// Read the punch file and parse it. `Err` is "no mapping from this source
    /// this pass": the kubelet swaps the ConfigMap file atomically, so a read
    /// or parse failure means the file is absent or being replaced — never a
    /// reason to panic, and never a partially-filled mapping (a defaulted port
    /// is a wrong port).
    async fn read_punch_file(&self, path: &Path) -> Result<NatmapMapping, (PunchFileProblem, String)> {
        let body = match tokio::fs::read_to_string(path).await {
            Ok(body) => body,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err((PunchFileProblem::Missing, String::new()));
            }
            Err(err) => return Err((PunchFileProblem::Unreadable, err.to_string())),
        };
        parse_punch_mapping(&body).map_err(|err| (PunchFileProblem::Malformed, err))
    }

    /// Log the source a pass used, once per change.
    fn note_source(&self, source: MappingSource) {
        let mut state = self.log_state.lock();
        if state.source == Some(source) {
            return;
        }
        state.source = Some(source);
        match source {
            MappingSource::PunchFile => {
                info!("natmap: mapping source is the punch file {}", self.config.punch_file_display())
            }
            MappingSource::NatmapHttp => {
                info!("natmap: mapping source is the natmap HTTP API {}", self.config.http_url().unwrap_or_default())
            }
        }
    }

    /// Log the punch file not yielding a mapping, once per change of state, and
    /// clear it when the file answers again. `detail` is only used when a
    /// warning is actually emitted.
    fn note_file_problem(&self, problem: Option<PunchFileProblem>, detail: &str) {
        let mut state = self.log_state.lock();
        if state.file_problem == problem {
            return;
        }
        state.file_problem = problem;
        match problem {
            Some(problem) => {
                let path = self.config.punch_file_display();
                if detail.is_empty() {
                    warn!("natmap: punch file {path} {}; no mapping from it this pass", problem.describe());
                } else {
                    warn!("natmap: punch file {path} {} ({detail}); no mapping from it this pass", problem.describe());
                }
            }
            None => info!("natmap: punch file {} yields a mapping again", self.config.punch_file_display()),
        }
    }

    /// GET `<api_endpoint>/<instance>.json`. Returns `None` on 404 (instance not
    /// registered), any transport/parse error, or when the HTTP source is not
    /// configured (the file-only deployment).
    async fn fetch_natmap(&self) -> Option<NatmapMapping> {
        let (endpoint, instance) = self.config.http_source()?;
        let url = format!("{}/{instance}.json", endpoint.trim_end_matches('/'));
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

    /// POST the new settings to the central settings page, then verify the
    /// change actually applied. Returns false on any failure — including a
    /// HTTP 200 whose re-rendered form still shows the old port — so the
    /// caller leaves the client suspended for a next-cycle retry. Uses the
    /// proxy-enabled client since e-hentai.org requires a proxy/VPN from some
    /// networks.
    async fn update_port(&self, settings: &HathWebSettings) -> bool {
        let url = format!("{}?cid={}&act=settings", SETTINGS_URL, self.client.id());
        let cookie = format!("ipb_member_id={}; ipb_pass_hash={}", self.config.ipb_member_id, self.config.ipb_pass_hash);
        // Encode the form manually: this reqwest build uses default-features = false,
        // so RequestBuilder::form(...) is unavailable.
        let body = serde_urlencoded::to_string(settings.to_form()).unwrap_or_default();
        match self
            .http_proxy
            .post(&url)
            .header(COOKIE, cookie)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                if !status.is_success() {
                    let snippet: String = body.chars().take(2000).collect();
                    error!("natmap: settings POST returned status={status} body={snippet}");
                    return false;
                }
                // The settings page answers 200 with the form re-rendered
                // whether it applied the change or silently rejected it (e.g.
                // "port cannot be changed while the client is running"), so
                // HTTP 200 alone is not proof. The re-rendered form carries
                // the server's current values — require f_port to show the
                // port we just posted.
                match parse_form_port(&body) {
                    Some(port) if port == settings.port => true,
                    Some(port) => {
                        error!(
                            "natmap: settings POST returned 200 but the form still shows \
                             f_port={port} (expected {}); the server rejected the change — \
                             is the client still marked as running?",
                            settings.port
                        );
                        false
                    }
                    None => {
                        error!(
                            "natmap: settings POST response had no f_port input — not the \
                             settings form (bad cookies or login redirect?)"
                        );
                        false
                    }
                }
            }
            Err(err) => {
                error!("natmap: settings POST failed: {err}");
                let mut source = err.source();
                while let Some(s) = source {
                    error!("  caused by: {s}");
                    source = s.source();
                }
                false
            }
        }
    }

    fn inc_sync(&self, labels: crate::metrics::NatmapSyncLabels) {
        self.metrics.natmap_sync_total.get_or_create(&labels).inc();
    }
}

/// Extract the value of the `f_port` input from a rendered settings form,
/// e.g. `<input type="text" name="f_port" value="47362" />` → `47362`.
/// Returns `None` when the input is missing or its value is not a port —
/// `update_port` treats that as a failed POST (login redirect, garbage
/// response, …).
fn parse_form_port(body: &str) -> Option<u16> {
    let name = body.find("name=\"f_port\"")?;
    let rest = &body[name..];
    let value = rest.find("value=\"")? + "value=\"".len();
    let end = rest[value..].find('"')? + value;
    rest[value..end].parse().ok()
}

/// Pick the mapping for a pass: the punch file wins, the natmap HTTP API is
/// the fallback, and no data at all is no mapping. Pure, so the precedence is
/// testable without a filesystem or a network.
fn select_mapping(file: Option<NatmapMapping>, http: Option<NatmapMapping>) -> Option<(NatmapMapping, MappingSource)> {
    match (file, http) {
        (Some(mapping), _) => Some((mapping, MappingSource::PunchFile)),
        (None, Some(mapping)) => Some((mapping, MappingSource::NatmapHttp)),
        (None, None) => None,
    }
}

/// Parse the punch file's content — natmap's document (`punch.json`), which the
/// punch controller writes as `{ip, port, inner_port, protocol}`.
///
/// Whitespace-tolerant (a ConfigMap payload usually ends with a newline), but
/// strict about shape: every field must be present, so a partial document can
/// never become a mapping with a defaulted port. A document without a usable
/// public tuple (empty `ip`, `port` 0) is not a mapping either — registering it
/// would point the tracker at nothing.
fn parse_punch_mapping(body: &str) -> Result<NatmapMapping, String> {
    let mapping: NatmapMapping = serde_json::from_str(body.trim()).map_err(|err| format!("invalid punch document: {err}"))?;
    if mapping.ip.trim().is_empty() {
        return Err("punch document has an empty `ip`".to_string());
    }
    if mapping.port == 0 {
        return Err("punch document has `port` 0".to_string());
    }
    Ok(mapping)
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
        // `submit` — matching the browser form. The historical `subbmit` typo
        // was never verified against the server (every real POST died in
        // transport before reaching it); a captured browser POST uses
        // `submit=Apply Settings` and is known to apply.
        assert_eq!(form.get("submit"), Some(&"Apply Settings".to_string()));
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

    /// The exact markup shape e-hentai renders (captured 2026-10-31).
    #[test]
    fn parse_form_port_reads_real_form() {
        let html = "<p>Current Value: <span style=\"font-weight:bold\">47362</span></p>\n\t\t<p>New Value: <input type=\"text\" name=\"f_port\" value=\"47362\" /></p>";
        assert_eq!(parse_form_port(html), Some(47362));
    }

    /// An unchanged (rejected) POST re-renders with the old port — the parser
    /// must read that value back so `update_port` can flag the rejection.
    #[test]
    fn parse_form_port_reads_rejected_value() {
        let html = "<input type=\"text\" name=\"f_port\" value=\"39628\" />";
        assert_eq!(parse_form_port(html), Some(39628));
    }

    #[test]
    fn parse_form_port_missing_when_form_absent() {
        assert_eq!(parse_form_port("<html>please log in</html>"), None);
    }

    #[test]
    fn parse_form_port_rejects_non_numeric() {
        assert_eq!(parse_form_port("<input name=\"f_port\" value=\"abc\" />"), None);
    }

    /// The exact document the punch controller writes (compact JSON, single key
    /// `punch.json`) — the file source must read the same shape the HTTP source
    /// does.
    #[test]
    fn punch_file_document_parses_into_natmap_mapping() {
        let body = r#"{"ip":"120.244.157.23","port":38698,"inner_port":44388,"protocol":"tcp"}"#;
        let mapping = parse_punch_mapping(body).expect("controller document must parse");
        assert_eq!(
            mapping,
            NatmapMapping {
                ip: "120.244.157.23".to_string(),
                port: 38698,
                inner_port: 44388,
                protocol: "tcp".to_string(),
            }
        );
    }

    /// A trailing newline (what a ConfigMap mounted file typically carries) and
    /// surrounding whitespace are not part of the document.
    #[test]
    fn punch_file_parsing_trims_surrounding_whitespace() {
        let body = " \n{\"ip\":\"203.0.113.7\",\"port\":27500,\"inner_port\":44388,\"protocol\":\"tcp\"}\n\n";
        assert_eq!(parse_punch_mapping(body).map(|m| m.port), Ok(27500));
    }

    /// (a) A valid file is preferred over the HTTP source, whatever port the
    /// HTTP source reports.
    #[test]
    fn punch_file_is_preferred_over_http() {
        let file = parse_punch_mapping(r#"{"ip":"120.244.157.23","port":38698,"inner_port":44388,"protocol":"tcp"}"#).ok();
        let http = parse_punch_mapping(r#"{"ip":"120.244.157.23","port":11111,"inner_port":27257,"protocol":"tcp"}"#).ok();
        let (mapping, source) = select_mapping(file, http).expect("a mapping is available");
        assert_eq!(mapping.port, 38698);
        assert_eq!(source, MappingSource::PunchFile);
    }

    /// (b) An absent file is not an error: the HTTP source is used instead.
    #[test]
    fn absent_punch_file_falls_back_to_http() {
        let http = parse_punch_mapping(r#"{"ip":"120.244.157.23","port":11111,"inner_port":27257,"protocol":"tcp"}"#).ok();
        let (mapping, source) = select_mapping(None, http).expect("the HTTP source answers");
        assert_eq!(mapping.port, 11111);
        assert_eq!(source, MappingSource::NatmapHttp);
    }

    /// (c) A malformed file is no data: with no HTTP source configured the pass
    /// has no mapping at all (and is retried), and the port the broken document
    /// mentions must never reach the tracker.
    #[test]
    fn malformed_punch_file_is_no_mapping() {
        // Garbage, a truncated write, and a document missing the fields the
        // contract requires (this one carries a plausible port — the exact way
        // a wrong port would slip through if the parse were lenient).
        for body in ["", "{not json", r#"{"ip":"120.244.157.23","port":38698}"#] {
            let file = parse_punch_mapping(body).ok();
            assert!(file.is_none(), "{body:?} must not parse into a mapping");
            assert!(select_mapping(file, None).is_none(), "{body:?} must be no mapping this pass");
        }
    }

    /// A document without a usable public tuple is not a mapping: the tracker
    /// would be pointed at nothing.
    #[test]
    fn punch_file_rejects_empty_ip_and_zero_port() {
        assert!(parse_punch_mapping(r#"{"ip":"","port":38698,"inner_port":44388,"protocol":"tcp"}"#).is_err());
        assert!(parse_punch_mapping(r#"{"ip":"120.244.157.23","port":0,"inner_port":44388,"protocol":"tcp"}"#).is_err());
    }
}
