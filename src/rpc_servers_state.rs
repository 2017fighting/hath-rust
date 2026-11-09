//! Durable copy of the RPC server address list pushed by the server.
//!
//! The server's `rpc_server_ip` setting (a `;`-separated address list) is the
//! only authoritative source for the RPC hosts used after the bootstrap DNS
//! hop: DNS answers cover 3 of the 5 addresses we see in practice, so a
//! DNS-derived list silently misses two. Until now that list existed only in
//! memory (`RPCClient::rpc_servers`) plus one `debug!` line, and debug logs
//! rotate after roughly three hours — too short to be read reliably from
//! outside the process.
//!
//! So every update of the list also rewrites a small state file, for consumers
//! outside the process (e.g. egress derivation reading it from a shared volume):
//!
//! ```text
//! 37.48.81.219\n37.48.81.200\n212.7.200.99\n212.7.202.50\n5.79.104.109\n
//! ```
//!
//! Format is fixed and line oriented: one normalized address per line, IPv4
//! mapped IPv6 prefixes (`::ffff:`) stripped, LF terminated including the last
//! line. Only addresses are written — no ids, keys or other settings. Writes
//! are atomic (temp file in the same directory + rename) and never propagate an
//! error into the RPC path: a missing state file degrades observability, it must
//! not break the client.

use std::{
    ffi::OsString,
    fs,
    io::{self, Write},
    net::IpAddr,
    path::{Path, PathBuf},
    str::FromStr,
};

use log::{debug, error};

/// Default location of the state file. Companions mount the data volume here
/// (`/hath/data` in the image, see `Dockerfile`), which is also where
/// `client_login` lives. Overridable with `--rpc-servers-state-file` /
/// `HATH_RPC_SERVERS_STATE_FILE`.
pub const DEFAULT_PATH: &str = "/hath/data/rpc_servers";

/// Parse a raw `rpc_server_ip` value into normalized address strings.
///
/// Entries are `;`-separated. Empty and unparseable entries are dropped (the
/// server's own formatting is trusted for everything else), and the order the
/// server sent is preserved verbatim — this is the same parse the in-memory
/// `rpc_servers` list has always used, so the file and the live list cannot
/// disagree.
pub fn parse(raw: &str) -> Vec<String> {
    raw.split(';')
        .filter_map(|entry| IpAddr::from_str(entry).ok())
        .map(|ip| match ip {
            IpAddr::V4(ip) => ip.to_string(),
            IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or_else(|| ip.to_string(), |ip| ip.to_string()),
        })
        .collect()
}

/// Render the address list in the on-disk format: one address per line, each
/// line LF terminated (including the last one), input order preserved.
pub fn format(servers: &[String]) -> String {
    let mut out = String::with_capacity(servers.iter().map(|s| s.len() + 1).sum());
    for server in servers {
        out.push_str(server);
        out.push('\n');
    }
    out
}

/// Persist the current address list to `path`.
///
/// Best effort by design: any failure is logged and swallowed, because the file
/// is for external observers and must never affect RPC. An empty list is not
/// written at all — leaving the previous file in place is strictly better than
/// publishing "there are no addresses".
pub fn persist(path: &Path, servers: &[String]) {
    if servers.is_empty() {
        debug!("rpc_servers state file {} not updated: empty address list", path.display());
        return;
    }

    match write_atomic(path, &format(servers)) {
        Ok(()) => debug!("rpc_servers state file written: {} ({} addresses)", path.display(), servers.len()),
        Err(err) => error!("Failed to write rpc_servers state file {}: {}", path.display(), err),
    }
}

/// Write `contents` to `path` atomically: a temp file next to the target is
/// flushed to disk, then renamed over it. A reader therefore always sees either
/// the old or the new content, never a partial write.
fn write_atomic(path: &Path, contents: &str) -> io::Result<()> {
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "state file path has no file name"))?;
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    fs::create_dir_all(dir)?;

    // Hidden temp name in the same directory (rename is only atomic within a
    // filesystem): `.<name>.tmp`, so a leftover from a crash is not mistaken for
    // the state file by anything scanning the directory.
    let mut tmp_name = OsString::from(".");
    tmp_name.push(file_name);
    tmp_name.push(".tmp");
    let tmp = dir.join(tmp_name);

    {
        let mut file = fs::File::create(&tmp)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
    }

    fs::rename(&tmp, path).inspect_err(|_| {
        let _ = fs::remove_file(&tmp);
    })
}

/// Resolve the state file path from the configured value, falling back to
/// [`DEFAULT_PATH`] when nothing was configured.
pub fn resolve(configured: Option<&str>) -> PathBuf {
    configured
        .filter(|p| !p.is_empty())
        .map_or_else(|| PathBuf::from(DEFAULT_PATH), PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    #[test]
    fn parse_splits_on_semicolons() {
        assert_eq!(parse("37.48.81.219;37.48.81.200;212.7.200.99"), strings(&["37.48.81.219", "37.48.81.200", "212.7.200.99"]));
        // Single entry, no separator.
        assert_eq!(parse("212.7.202.50"), strings(&["212.7.202.50"]));
        // Real payload shape, see docs/research/egress-split/02.
        assert_eq!(
            parse("::ffff:37.48.81.219;::ffff:37.48.81.200;::ffff:212.7.200.99;::ffff:212.7.202.50;::ffff:5.79.104.109"),
            strings(&["37.48.81.219", "37.48.81.200", "212.7.200.99", "212.7.202.50", "5.79.104.109"])
        );
    }

    #[test]
    fn parse_strips_ipv4_mapped_ipv6_prefix() {
        assert_eq!(parse("::ffff:212.7.200.99"), strings(&["212.7.200.99"]));
        // Uppercase hex form of the same prefix.
        assert_eq!(parse("::FFFF:1.2.3.4"), strings(&["1.2.3.4"]));
        // A plain IPv4 is passed through unchanged.
        assert_eq!(parse("1.2.3.4"), strings(&["1.2.3.4"]));
        // A real IPv6 address has no IPv4 form; it is kept as-is rather than dropped.
        assert_eq!(parse("2001:db8::1"), strings(&["2001:db8::1"]));
    }

    #[test]
    fn parse_drops_empty_and_invalid_entries() {
        // Leading/trailing/doubled separators and junk all vanish.
        assert_eq!(parse("1.2.3.4;;;5.6.7.8;"), strings(&["1.2.3.4", "5.6.7.8"]));
        assert_eq!(parse(";1.2.3.4;"), strings(&["1.2.3.4"]));
        assert_eq!(parse("not-an-ip;1.2.3.4;999.1.1.1"), strings(&["1.2.3.4"]));
        assert_eq!(parse(""), strings(&[]));
        assert_eq!(parse(";;"), strings(&[]));
    }

    #[test]
    fn format_is_one_line_per_address_with_trailing_lf() {
        assert_eq!(format(&strings(&["1.2.3.4", "5.6.7.8"])), "1.2.3.4\n5.6.7.8\n");
        // Last line is terminated too, so a line-oriented reader needs no special case.
        assert!(format(&strings(&["1.2.3.4"])).ends_with('\n'));
        // Empty list renders nothing at all (and `persist` then writes nothing).
        assert_eq!(format(&[]), "");
        // No CR, ever: consumers split on LF only.
        assert!(!format(&strings(&["1.2.3.4"])).contains('\r'));
    }

    #[test]
    fn format_preserves_input_order() {
        // Order stability: the file must reflect the server's order, not a
        // re-sorted or set-deduplicated one.
        let parsed = parse("5.79.104.109;37.48.81.219;212.7.202.50;::ffff:212.7.200.99");
        assert_eq!(format(&parsed), "5.79.104.109\n37.48.81.219\n212.7.202.50\n212.7.200.99\n");
        // Same input, same bytes — repeated settings pushes produce identical
        // files (so a consumer can diff them).
        assert_eq!(format(&parse("5.79.104.109;37.48.81.219;212.7.202.50;::ffff:212.7.200.99")), format(&parsed));
        // Line count equals address count: the LF is a terminator, not a separator.
        assert_eq!(format(&parsed).lines().count(), parsed.len());
    }

    #[test]
    fn persist_writes_the_parsed_list_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data/rpc_servers");
        let servers = parse("::ffff:37.48.81.219;212.7.200.99");

        persist(&path, &servers);
        assert_eq!(fs::read_to_string(&path).unwrap(), "37.48.81.219\n212.7.200.99\n");
        // No temp file left behind.
        let leftovers: Vec<_> = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(leftovers, vec![OsString::from("rpc_servers")]);

        // Rewrite replaces content instead of appending.
        persist(&path, &parse("5.79.104.109"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "5.79.104.109\n");
    }

    #[test]
    fn persist_leaves_the_previous_file_when_the_list_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc_servers");
        persist(&path, &parse("1.2.3.4"));
        persist(&path, &parse(";;"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "1.2.3.4\n");
    }

    #[test]
    fn persist_failure_is_not_panicking() {
        // A directory where the file should be cannot be replaced by a rename;
        // the only observable effect is the logged error.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc_servers");
        fs::create_dir(&path).unwrap();
        persist(&path, &parse("1.2.3.4"));
        assert!(path.is_dir());
    }

    #[test]
    fn resolve_defaults_and_honors_override() {
        assert_eq!(resolve(None), PathBuf::from(DEFAULT_PATH));
        assert_eq!(resolve(Some("")), PathBuf::from(DEFAULT_PATH));
        assert_eq!(resolve(Some("/tmp/x")), PathBuf::from("/tmp/x"));
    }
}
