//! NAT port-sync background task.
//!
//! The router holds a mapping open for this client and the punch controller
//! publishes it as a file — `HATH_PUNCH_FILE`, the `punch.json` key of the
//! `<name>-punch` ConfigMap (ADR-0044). That file is the only mapping source.
//! The central H@H tracker records one port per client and rejects settings
//! changes while the client is active, so this task polls the file, compares its
//! public port with the port the tracker has on record, and when they differ
//! runs the proven suspend → update-settings → re-register flow.
//!
//! THE FILE IS ALSO A DECLARATION, and the declaration is the *content*, not the
//! presence. A `down` punch writes a document that says so (`port: 0`), and a
//! client whose port nothing forwards to is worse than a client that is not
//! running: the tracker keeps pointing downloaders at a hole that is not there.
//! So **no mapping means HALTED** — at runtime and at startup alike:
//!
//! * `client_suspend` (the same primitive the port-change flow already uses), the
//!   file re-read on every tick, and re-registration as soon as a mapping is
//!   back. `decide` is the whole of that state machine, and it is pure.
//! * The process never exits over it. Exiting would be a CrashLoop whenever the
//!   pod restarts while the punch is down — which is the ordinary shape, not the
//!   rare one — and the tracker treats a startup loop as an offence
//!   (`FAIL_STARTUP_LOOP`: a 15-minute wait). A halted client waits for free.
//!
//! WHY PRESENCE IS NOT THE SIGNAL: kubelet never removes a mounted ConfigMap's
//! files. When the ConfigMap is deleted under a running pod, the volume keeps its
//! last payload and kubelet only logs `MountVolume.SetUp failed … not found`
//! (measured 2026-11-26 on this cluster: the file was byte-identical 2.5 minutes
//! after the ConfigMap was gone). "The file disappeared" therefore never reaches
//! an app; "the document says down" does, because an *update* is projected
//! (~60–90s). The punch contract was changed to say so (home-ops ADR-0044/0045).
//!
//! While halted, `still_alive` is not sent: the keepalive task checks the shared
//! `halted` flag. The session is suspended, so the check is either meaningless
//! or answers `FAIL_NOT_LOGGED_IN`, which shuts the process down — turning a
//! cheap halt into a restart, the exact thing the halt exists to avoid.
//!
//! Every log line here is rate-limited to a change of state, not printed once
//! per tick: one when the punch file stops yielding a mapping, one when it
//! starts again, one per halt, one per resume.

use std::{
    collections::HashMap,
    error::Error,
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use log::{debug, error, info, warn};
use parking_lot::Mutex;
use reqwest::{
    Client, Proxy,
    header::{CONTENT_TYPE, COOKIE},
};
use serde::Deserialize;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep};

use crate::{
    metrics::{LABEL_NATMAP_SYNC_APPLIED, LABEL_NATMAP_SYNC_ERROR, LABEL_NATMAP_SYNC_HALTED, LABEL_NATMAP_SYNC_NO_CHANGE, Metrics},
    rpc::RPCClient,
    util::create_http_client,
};

/// Central settings page. The `cid` query parameter is the H@H Client ID,
/// sourced at runtime from `RPCClient::id` rather than hardcoded.
const SETTINGS_URL: &str = "https://e-hentai.org/hentaiathome.php";

/// First delay before retrying a failed pre-startup pass or a failed resume.
/// Matches the punch agent's own cadence, so a transient punch outage is
/// re-tried on the tick after it heals rather than on its own clock.
const RETRY_INITIAL: Duration = Duration::from_secs(15);
/// Upper bound for both backoffs.
const RETRY_MAX: Duration = Duration::from_secs(300);
/// All configuration the port-sync task needs. It is built only when a punch
/// file is configured; when none is, the task is not spawned and this feature
/// stays off.
#[derive(Clone)]
pub struct NatmapConfig {
    /// Path of the punch file (`punch.json`) written by the punch controller and
    /// mounted from the `<name>-punch` ConfigMap. The only mapping source: it is
    /// re-read on every tick, and its absence means the punch is down.
    pub punch_file: std::path::PathBuf,
    /// Forum cookie `ipb_member_id`, used to authenticate the settings POST.
    pub ipb_member_id: String,
    /// Forum cookie `ipb_pass_hash`, used to authenticate the settings POST.
    pub ipb_pass_hash: String,
    /// How often to re-read the punch file and reconcile.
    pub poll_interval: Duration,
}

impl NatmapConfig {
    fn punch_file_display(&self) -> String {
        self.punch_file.display().to_string()
    }

    /// The mapping source, for logs.
    pub fn source_summary(&self) -> String {
        format!("punch file {}", self.punch_file_display())
    }
}

/// Why the punch file produced no mapping this pass. Every variant means the
/// same thing to the caller ("no mapping"); they differ in whether the file is
/// there at all, in what the rate-limited line says, and in whether it is a
/// state to worry about (`Down` is not: it is how a punch says it is down).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PunchFileProblem {
    Missing,
    Unreadable,
    Malformed,
    /// The document is well-formed and says the punch has no mapping
    /// (`{"ip":"","port":0,…}`) — the normal shape of a `down` punch.
    Down,
}

impl PunchFileProblem {
    fn describe(self) -> &'static str {
        match self {
            PunchFileProblem::Missing => "is absent",
            PunchFileProblem::Unreadable => "could not be read",
            PunchFileProblem::Malformed => "is not a punch document for this client",
            PunchFileProblem::Down => "says the punch is down",
        }
    }
}

/// Why a document is not a mapping. `Down` is the punch saying "nothing is
/// mapped" — a state, not a defect — and it is kept apart from `Invalid` so the
/// log line and the metrics can tell "the punch is down" from "this file is
/// broken".
#[derive(Clone, Debug, PartialEq, Eq)]
enum PunchDocError {
    Down,
    Invalid(String),
}

/// What one pass should do, decided from the mapping and the client's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    /// A mapping is known and the tracker already has that port; keep running.
    NoChange,
    /// A mapping is known but the tracker has another port: suspend, update the
    /// settings page, re-register.
    UpdatePort,
    /// A mapping is known, the tracker already has that port, but the client is
    /// halted: re-register, nothing to change on the settings page.
    Resume,
    /// No mapping: suspend the client and report it halted.
    Suspend,
    /// No mapping and already halted: nothing to do.
    StayHalted,
}

/// The halt state machine, in one place. `registered_port` is the port the
/// tracker currently has on record — `None` when it could not be read, which is
/// treated as "not equal", i.e. the settings page is rewritten.
///
/// A port mismatch outranks the halt: a halted client still needs the tracker's
/// port corrected before it can be re-registered, and `UpdatePort` skips its own
/// suspend when the client is already halted.
fn decide(mapping: Option<&NatmapMapping>, registered_port: Option<u16>, halted: bool) -> Action {
    match (mapping, halted) {
        (None, false) => Action::Suspend,
        (None, true) => Action::StayHalted,
        (Some(mapping), _) if registered_port != Some(mapping.port) => Action::UpdatePort,
        (Some(_), true) => Action::Resume,
        (Some(_), false) => Action::NoChange,
    }
}

/// One pass's outcome. The public/external callers act on the halt variants
/// (the pre-startup loop applies its budget, the background loop just keeps
/// ticking).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PassResult {
    NoChange,
    Applied,
    /// The client is (now) halted because the punch yields no mapping.
    Halted,
    /// The pass could not complete (tracker unreachable, settings POST rejected).
    Failed,
}

/// Pre-startup outcome: distinct from `PassResult` because "no mapping" is the
/// one failure the startup budget applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PrestartupOutcome {
    Ready,
    NoMapping,
    Failed,
}

/// What the last pass logged, so a steady state is quiet: one line when the
/// punch file starts or stops producing a mapping.
#[derive(Default)]
struct FileLog {
    problem: Option<PunchFileProblem>,
}

/// Backoff for re-registering a halted client. A rejected `client_start` (most
/// likely `FAIL_CONNECT_TEST` while the DNAT has not caught up) must not be
/// retried every tick: each attempt asks the tracker to run a connectivity test
/// against us and fail it.
#[derive(Default)]
struct ResumeGate {
    /// The next attempt must not start before this instant.
    not_before: Option<Instant>,
    /// The delay used last time; doubles on every failure up to `RETRY_MAX`.
    delay: Duration,
}

impl ResumeGate {
    fn blocked(&self) -> bool {
        matches!(self.not_before, Some(at) if Instant::now() < at)
    }

    /// Record a failed attempt and return the delay before the next one.
    fn failed(&mut self) -> Duration {
        self.delay = if self.delay.is_zero() {
            RETRY_INITIAL
        } else {
            (self.delay * 2).min(RETRY_MAX)
        };
        self.not_before = Some(Instant::now() + self.delay);
        self.delay
    }

    fn reset(&mut self) {
        *self = ResumeGate::default();
    }
}

/// A NAT mapping record as published by the punch controller.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
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
    /// the new public port. Mirrors `load_from_rpc_client_settings` +
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

/// The long-running port-sync task. Spawn one per process (or none, when no
/// punch file is configured).
pub struct NatmapSync {
    config: NatmapConfig,
    client: Arc<RPCClient>,
    /// Proxy-enabled client (e-hentai.org settings POST).
    http_proxy: Client,
    metrics: Arc<Metrics>,
    /// The port this client serves content on. A punch must forward to it, so a
    /// document naming another inner port forwards to nothing and is no mapping.
    listen_port: u16,
    /// Shared with the keepalive task: while halted, `still_alive` is not sent.
    halted: Arc<AtomicBool>,
    /// Backoff for re-registering a halted client.
    resume_gate: Mutex<ResumeGate>,
    /// What was logged last pass — see `FileLog`.
    file_log: Mutex<FileLog>,
}

impl NatmapSync {
    pub fn new(
        config: NatmapConfig,
        client: Arc<RPCClient>,
        metrics: Arc<Metrics>,
        proxy: Option<Proxy>,
        local_addr: Option<IpAddr>,
        listen_port: u16,
        halted: Arc<AtomicBool>,
    ) -> Self {
        if proxy.is_some() {
            info!("natmap: using proxy for e-hentai settings POST");
        }
        let http_proxy = create_http_client(Duration::from_secs(10), proxy, local_addr);
        Self {
            config,
            client,
            http_proxy,
            metrics,
            listen_port,
            halted,
            resume_gate: Mutex::new(ResumeGate::default()),
            file_log: Mutex::new(FileLog::default()),
        }
    }

    /// Run the poll loop until the RPC client stops running.
    pub async fn run(self) {
        // First tick fires immediately, so a stale port is corrected at startup.
        let mut ticker = interval(self.config.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if !self.client.is_running() {
                break;
            }
            self.cycle().await;
        }
    }

    /// Run pre-startup reconciliation passes until one succeeds, retrying with
    /// exponential backoff. Called before `connect_check` so the client comes
    /// online with the correct registered port.
    ///
    /// "No mapping" is handled exactly as it is at runtime: the client halts
    /// (suspends) and keeps polling. It does NOT exit. Exiting here was the
    /// first design, on the theory that the required `<name>-punch` mount would
    /// gate the restart — but that gate is only sound while "no mapping" means
    /// "no ConfigMap", and it does not: a `down` punch now writes a document
    /// that says so, and the common shape becomes "punch down, pod restarted".
    /// A process that exits there is a CrashLoop, and the tracker treats a
    /// startup loop as an offence (`FAIL_STARTUP_LOOP` → a 15-minute wait), so a
    /// client with nowhere to register waits rather than churns. Responds to
    /// Ctrl+C during sleep.
    pub async fn sync_until_ready(&self) -> Result<(), Box<dyn Error>> {
        info!("natmap: running pre-startup port-sync...");
        let mut delay = RETRY_INITIAL;
        loop {
            let wait = match self.prestartup_once().await {
                PrestartupOutcome::Ready => {
                    info!("natmap: pre-startup port-sync complete");
                    return Ok(());
                }
                PrestartupOutcome::NoMapping => {
                    // The same halt the background loop applies, at the one moment
                    // there is no session to suspend yet (the RPC tolerates that).
                    if !self.is_halted() {
                        info!("natmap: no mapping from the punch file; halting until it returns");
                        self.client.suspend().await;
                        self.set_halted(true);
                    }
                    self.inc_sync(LABEL_NATMAP_SYNC_HALTED);
                    warn!("natmap: no mapping from {}; retrying in {delay:?}", self.config.source_summary());
                    delay
                }
                PrestartupOutcome::Failed => {
                    warn!("natmap: pre-startup sync did not complete; retrying in {delay:?}");
                    delay
                }
            };
            let interrupted = tokio::select! {
                _ = sleep(wait) => false,
                _ = tokio::signal::ctrl_c() => {
                    info!("natmap: pre-startup sync interrupted by Ctrl+C");
                    true
                }
            };
            if interrupted {
                return Err("natmap pre-startup sync interrupted by Ctrl+C".into());
            }
            delay = (delay * 2).min(RETRY_MAX);
        }
    }

    /// One pre-startup pass: make the tracker's registered port match the
    /// punch, and drop the halt if the punch has a mapping again. Does not
    /// re-notify the server — `connect_check` runs next and is what registers
    /// this client for the first time.
    async fn prestartup_once(&self) -> PrestartupOutcome {
        let Some(mapping) = self.read_mapping().await else {
            return PrestartupOutcome::NoMapping;
        };
        self.metrics.natmap_port.set(mapping.port as u64);
        // The halt is over: the mapping is back, so the keepalive may speak for
        // this client again once connect_check has registered it.
        self.set_halted(false);
        self.resume_gate.lock().reset();

        let Some(map) = self.fetch_settings().await else {
            return PrestartupOutcome::Failed;
        };
        let registered_port = map.get("port").and_then(|v| v.parse::<u16>().ok());
        if let Some(port) = registered_port {
            self.metrics.natmap_registered_port.set(port as u64);
        }
        if registered_port == Some(mapping.port) {
            info!("natmap: public port {} matches registered port; no change", mapping.port);
            self.inc_sync(LABEL_NATMAP_SYNC_NO_CHANGE);
            return PrestartupOutcome::Ready;
        }
        info!("natmap: public port {} differs from registered port {}; updating", mapping.port, display_port(registered_port));
        // The settings page rejects changes while the client is active. Before
        // startup this also clears a stale server-side session left by an
        // unclean shutdown, so it runs unless the halt already suspended us.
        if !self.is_halted() {
            self.client.suspend().await;
        }
        if !self.update_port(&HathWebSettings::from_settings_map(&map, mapping.port)).await {
            error!("natmap: settings page update failed; will retry");
            self.inc_sync(LABEL_NATMAP_SYNC_ERROR);
            return PrestartupOutcome::Failed;
        }
        self.metrics.natmap_registered_port.set(mapping.port as u64);
        self.inc_sync(LABEL_NATMAP_SYNC_APPLIED);
        info!("natmap: registered port updated to {}", mapping.port);
        PrestartupOutcome::Ready
    }

    /// One pass of the background loop. A pass postponed by the resume backoff
    /// is a halted pass and is counted as one.
    async fn cycle(&self) {
        if self.resume_gate.lock().blocked() {
            self.inc_sync(LABEL_NATMAP_SYNC_HALTED);
            return;
        }
        self.reconcile_once().await;
    }

    /// One reconciliation pass: read the punch file, compare with the port the
    /// tracker has on record, and run whatever `decide` says.
    async fn reconcile_once(&self) -> PassResult {
        let mapping = self.read_mapping().await;
        if let Some(mapping) = &mapping {
            self.metrics.natmap_port.set(mapping.port as u64);
        }
        // The tracker's settings are only needed when there is a mapping to
        // compare against — the halt decision does not depend on them, so a
        // halted client never asks the tracker anything.
        let settings = match &mapping {
            Some(_) => match self.fetch_settings().await {
                Some(map) => Some(map),
                None => return PassResult::Failed,
            },
            None => None,
        };
        let registered_port = settings
            .as_ref()
            .and_then(|map| map.get("port").and_then(|v| v.parse::<u16>().ok()));
        if settings.is_some() {
            if let Some(port) = registered_port {
                self.metrics.natmap_registered_port.set(port as u64);
            } else {
                warn!("natmap: `port` missing from client_settings response");
            }
        }

        let halted = self.is_halted();
        let action = decide(mapping.as_ref(), registered_port, halted);
        match action {
            Action::NoChange => {
                // Steady state: the counter is the record, a line per tick is not.
                let port = mapping.map(|m| m.port).unwrap_or_default();
                debug!("natmap: public port {port} matches registered port; no change");
                self.inc_sync(LABEL_NATMAP_SYNC_NO_CHANGE);
                PassResult::NoChange
            }
            Action::StayHalted => {
                // The transition and the file problem were already logged.
                self.inc_sync(LABEL_NATMAP_SYNC_HALTED);
                PassResult::Halted
            }
            Action::Suspend => {
                info!("natmap: no mapping from the punch file; halting the client until it returns");
                self.client.suspend().await;
                self.set_halted(true);
                self.inc_sync(LABEL_NATMAP_SYNC_HALTED);
                PassResult::Halted
            }
            Action::UpdatePort => {
                let mapping = mapping.expect("UpdatePort implies a mapping");
                let settings = settings.expect("UpdatePort implies tracker settings");
                info!("natmap: public port {} differs from registered port {}; updating", mapping.port, display_port(registered_port));
                let web = HathWebSettings::from_settings_map(&settings, mapping.port);
                // A halted client is already suspended server-side; suspending
                // again would only log a non-OK answer.
                if !halted {
                    self.client.suspend().await;
                }
                if !self.update_port(&web).await {
                    error!("natmap: settings page update failed; will retry");
                    self.inc_sync(LABEL_NATMAP_SYNC_ERROR);
                    return PassResult::Failed;
                }
                self.metrics.natmap_registered_port.set(mapping.port as u64);
                self.inc_sync(LABEL_NATMAP_SYNC_APPLIED);
                info!("natmap: registered port updated to {}", mapping.port);
                if !self.start_client().await {
                    return PassResult::Failed;
                }
                PassResult::Applied
            }
            Action::Resume => {
                let port = mapping.map(|m| m.port).unwrap_or_default();
                if !self.start_client().await {
                    return PassResult::Failed;
                }
                info!("natmap: the punch yields a mapping again; client re-registered on port {port}");
                self.inc_sync(LABEL_NATMAP_SYNC_APPLIED);
                PassResult::Applied
            }
        }
    }

    /// Re-notify the tracker that this client is running. A rejection leaves the
    /// client halted and schedules the next attempt with backoff.
    async fn start_client(&self) -> bool {
        if self.client.client_start().await {
            self.set_halted(false);
            self.resume_gate.lock().reset();
            true
        } else {
            self.set_halted(true);
            let delay = self.resume_gate.lock().failed();
            warn!("natmap: the tracker did not accept client_start; retrying in {delay:?}");
            self.inc_sync(LABEL_NATMAP_SYNC_ERROR);
            false
        }
    }

    fn is_halted(&self) -> bool {
        self.halted.load(Ordering::Relaxed)
    }

    /// Set the halt flag, exporting the halt as a gauge on the transition only.
    fn set_halted(&self, halted: bool) {
        if self.halted.swap(halted, Ordering::Relaxed) != halted {
            self.metrics.natmap_halted.set(u64::from(halted));
        }
    }

    /// The mapping from the punch file, or `None` when it yields none. Sets
    /// `natmap_punch_file_present` here, at the one place that knows whether the
    /// file is there.
    async fn read_mapping(&self) -> Option<NatmapMapping> {
        match self.read_punch_file().await {
            Ok(mapping) => {
                self.metrics.natmap_punch_file_present.set(1);
                self.note_file_problem(None, "");
                Some(mapping)
            }
            Err((problem, detail)) => {
                // An unreadable or malformed file is still a file that is there:
                // the punch exists and something is wrong with it.
                let present = u64::from(problem != PunchFileProblem::Missing);
                self.metrics.natmap_punch_file_present.set(present);
                self.note_file_problem(Some(problem), &detail);
                None
            }
        }
    }

    /// Read the punch file and parse it. `Err` is "no mapping from it this
    /// pass": the kubelet swaps the ConfigMap file atomically, so a read or
    /// parse failure means the file is absent or being replaced — never a
    /// reason to panic, and never a partially-filled mapping (a defaulted port
    /// is a wrong port).
    async fn read_punch_file(&self) -> Result<NatmapMapping, (PunchFileProblem, String)> {
        let body = match tokio::fs::read_to_string(&self.config.punch_file).await {
            Ok(body) => body,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err((PunchFileProblem::Missing, String::new()));
            }
            Err(err) => return Err((PunchFileProblem::Unreadable, err.to_string())),
        };
        parse_punch_mapping(&body, self.listen_port).map_err(|err| match err {
            PunchDocError::Down => (PunchFileProblem::Down, String::new()),
            PunchDocError::Invalid(detail) => (PunchFileProblem::Malformed, detail),
        })
    }

    /// The tracker's `client_settings` map, or `None` when it could not be read.
    async fn fetch_settings(&self) -> Option<HashMap<String, String>> {
        let map = self.client.fetch_client_settings().await;
        if map.is_none() {
            self.inc_sync(LABEL_NATMAP_SYNC_ERROR);
        }
        map
    }

    /// Log the punch file not yielding a mapping, once per change of state, and
    /// clear it when the file answers again. `detail` is only used when a
    /// warning is actually emitted.
    fn note_file_problem(&self, problem: Option<PunchFileProblem>, detail: &str) {
        let mut state = self.file_log.lock();
        if state.problem == problem {
            return;
        }
        state.problem = problem;
        match problem {
            // A down punch is the state this task exists to react to, not a fault.
            Some(PunchFileProblem::Down) => {
                info!("natmap: punch file {} says the punch is down; no mapping from it", self.config.punch_file_display())
            }
            Some(problem) => {
                let path = self.config.punch_file_display();
                if detail.is_empty() {
                    warn!("natmap: punch file {path} {}; no mapping from it", problem.describe());
                } else {
                    warn!("natmap: punch file {path} {} ({detail}); no mapping from it", problem.describe());
                }
            }
            None => info!("natmap: punch file {} yields a mapping again", self.config.punch_file_display()),
        }
    }

    /// POST the new settings to the central settings page, then verify the
    /// change actually applied. Returns false on any failure — including a
    /// HTTP 200 whose re-rendered form still shows the old port — so the
    /// caller leaves the client suspended for a next-pass retry. Uses the
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

/// Render the tracker's registered port for a log line; `None` is `?`.
fn display_port(port: Option<u16>) -> String {
    port.map(|p| p.to_string()).unwrap_or_else(|| "?".to_string())
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

/// Parse the punch file's content — natmap's document (`punch.json`), which the
/// punch controller writes as `{ip, port, inner_port, protocol}`.
///
/// Whitespace-tolerant (a ConfigMap payload usually ends with a newline), but
/// strict about shape: every field must be present, so a partial document can
/// never become a mapping with a defaulted port. A document that does not
/// describe a working punch is not a mapping either — registering it would point
/// the tracker at nothing:
///
/// * an empty `ip` or a `port` of 0 is not a public tuple;
/// * `protocol` must be TCP — the carrier maps TCP and UDP independently, so a
///   UDP punch would be a second public port no announce can express (ADR-0044);
/// * `inner_port` must be the port this client serves content on. A document
///   naming another port describes a redirect to somewhere else, so the port it
///   announces would never reach us.
fn parse_punch_mapping(body: &str, listen_port: u16) -> Result<NatmapMapping, PunchDocError> {
    let invalid = |msg: String| PunchDocError::Invalid(msg);
    let mapping: NatmapMapping = serde_json::from_str(body.trim()).map_err(|err| invalid(format!("invalid punch document: {err}")))?;
    // The punch saying "down" — both fields at once, because that is what the
    // controller writes for a `down` or `pending` punch. Half of it (an empty ip
    // beside a live port, or the reverse) is a broken document, not a state.
    let empty_ip = mapping.ip.trim().is_empty();
    let zero_port = mapping.port == 0;
    if empty_ip && zero_port {
        return Err(PunchDocError::Down);
    }
    if empty_ip {
        return Err(invalid("punch document has an empty `ip`".to_string()));
    }
    if zero_port {
        return Err(invalid("punch document has `port` 0".to_string()));
    }
    if !mapping.protocol.eq_ignore_ascii_case("tcp") {
        return Err(invalid(format!("punch document has protocol {:?}, not tcp", mapping.protocol)));
    }
    if mapping.inner_port != listen_port {
        return Err(invalid(format!("punch document has inner_port {}, but this client serves on {listen_port}", mapping.inner_port)));
    }
    Ok(mapping)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rpc_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn mapping(port: u16) -> NatmapMapping {
        NatmapMapping {
            ip: "120.244.157.23".to_string(),
            port,
            inner_port: 44388,
            protocol: "tcp".to_string(),
        }
    }

    /// The document the punch controller writes (compact JSON), which the client
    /// must read as the same shape natmap's JSON has.
    const DOCUMENT: &str = r#"{"ip":"120.244.157.23","port":38698,"inner_port":44388,"protocol":"tcp"}"#;

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

    /// A trailing newline (what a ConfigMap mounted file typically carries) and
    /// surrounding whitespace are not part of the document.
    #[test]
    fn punch_file_parsing_trims_surrounding_whitespace() {
        let body = " \n{\"ip\":\"203.0.113.7\",\"port\":27500,\"inner_port\":44388,\"protocol\":\"tcp\"}\n\n";
        assert_eq!(parse_punch_mapping(body, 44388).map(|m| m.port), Ok(27500));
    }

    #[test]
    fn punch_file_document_parses_into_a_mapping() {
        assert_eq!(parse_punch_mapping(DOCUMENT, 44388), Ok(mapping(38698)));
    }

    /// Garbage, a truncated write, and a document missing the fields the
    /// contract requires must all be no mapping — a partial document that
    /// carried a plausible port is exactly how a wrong port would slip through.
    #[test]
    fn broken_punch_documents_are_no_mapping() {
        for body in ["", "{not json", r#"{"ip":"120.244.157.23","port":38698}"#] {
            assert!(parse_punch_mapping(body, 44388).is_err(), "{body:?} must not parse");
        }
    }

    /// The `down` shape the punch controller writes for a `down` or `pending` punch: both
    /// fields empty at once. It is a STATE, not a defect, and the log line says so — a
    /// half-empty document beside a live field is the opposite, a broken file.
    #[test]
    fn down_document_is_a_state_and_a_half_one_is_a_defect() {
        let down = r#"{"ip":"","port":0,"inner_port":44388,"protocol":"tcp"}"#;
        assert_eq!(parse_punch_mapping(down, 44388), Err(PunchDocError::Down));

        for body in [
            r#"{"ip":"","port":38698,"inner_port":44388,"protocol":"tcp"}"#,
            r#"{"ip":"120.244.157.23","port":0,"inner_port":44388,"protocol":"tcp"}"#,
        ] {
            assert!(
                matches!(parse_punch_mapping(body, 44388), Err(PunchDocError::Invalid(_))),
                "{body} must be a defect, not a down punch"
            );
        }
    }

    /// Strictness that matters: a punch forwarding to another port announces a
    /// port that never reaches this client, and a UDP punch is a mapping this
    /// client cannot serve at all.
    #[test]
    fn punch_document_must_describe_this_client() {
        let other_port = r#"{"ip":"120.244.157.23","port":38698,"inner_port":44389,"protocol":"tcp"}"#;
        let udp = r#"{"ip":"120.244.157.23","port":38698,"inner_port":44388,"protocol":"udp"}"#;
        assert!(parse_punch_mapping(other_port, 44388).is_err());
        assert!(parse_punch_mapping(udp, 44388).is_err());
        // The protocol comparison is case-insensitive: the CRD's enum is
        // `TCP`, natmap's document is lowercase.
        assert!(parse_punch_mapping(r#"{"ip":"120.244.157.23","port":38698,"inner_port":44388,"protocol":"TCP"}"#, 44388).is_ok());
    }

    /// The halt state machine, exhaustively: the port comparison outranks the
    /// halt, and "no mapping" is the only input that suspends.
    #[test]
    fn decide_covers_every_state() {
        let m = mapping(38698);
        let cases = [
            // (mapping, registered, halted, expected)
            (None, None, false, Action::Suspend),
            (None, Some(38698), false, Action::Suspend),
            (None, None, true, Action::StayHalted),
            (None, Some(38698), true, Action::StayHalted),
            (Some(&m), Some(38698), false, Action::NoChange),
            (Some(&m), Some(38698), true, Action::Resume),
            (Some(&m), Some(11111), false, Action::UpdatePort),
            (Some(&m), Some(11111), true, Action::UpdatePort),
            (Some(&m), None, false, Action::UpdatePort),
            (Some(&m), None, true, Action::UpdatePort),
        ];
        for (mapping, registered, halted, expected) in cases {
            assert_eq!(decide(mapping, registered, halted), expected, "mapping={mapping:?} registered={registered:?} halted={halted}");
        }
    }
}
