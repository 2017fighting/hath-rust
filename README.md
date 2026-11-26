# hath-rust
[![Build](../../actions/workflows/build.yml/badge.svg)](../../actions/workflows/build.yml) [![Docker Pulls](https://img.shields.io/docker/pulls/james58899/hath-rust)](https://hub.docker.com/r/james58899/hath-rust)

The unofficial Hentai@Home client written in Rust.

## Install
Read the [Wiki](https://github.com/james58899/hath-rust/wiki/Install)

## NAT port sync
hath must tell the H@H tracker which public port it is reachable on, so it runs a port-sync loop
that re-reads that port and updates the settings page when it changes. The port comes from one
source: **a file** — `HATH_PUNCH_FILE` (`--punch-file`), the path of the `punch.json` document a punch
controller mounts from its ConfigMap, e.g. `/punch/punch.json`. Content is
`{"ip": "<public ip>", "port": <public port>, "inner_port": <bind port>, "protocol": "tcp"}`, where
`inner_port` must be the port this client serves on. Without a punch file, port-sync stays off.

The document is read on every tick (`HATH_NATMAP_POLL_INTERVAL`, default 15s) and must describe a
working punch: a missing file, unparsable JSON, an empty `ip`, `port: 0`, a protocol other than TCP,
or an `inner_port` that is not this client's port are all **no mapping** this pass — never a mapping
with a defaulted field.

No mapping means the punch is down, so the client is **halted**: `client_suspend` is sent once, the
file keeps being read, and the client is re-registered as soon as a mapping is back (the settings
page is updated first if the port changed). While halted, no `still_alive` is sent — the session is
suspended, and its answer would shut the process down. Before startup, no mapping is bounded: after
three minutes the process exits, so a client that cannot register is visible instead of idling behind
a bound port.

`HATH_IPB_MEMBER_ID` and `HATH_IPB_PASS_HASH` (`--ipb-member-id`, `--ipb-pass-hash`) authenticate the
settings POST. The halt is exported as `hath_natmap_halted`, the file's presence as
`hath_natmap_punch_file_present`, and every pass is counted in the outcome counter — registered as
`natmap_sync_total` and exported as `hath_natmap_sync_total_total`, because prometheus-client appends
`_total` to a counter's name — with `outcome` one of `Applied`, `NoChange`, `Error`, `Halted`. The
vocabulary is in [GLOSSARY.md](GLOSSARY.md).

## Features
### New
Features not included in the official.
* Lower memory usage
* Parallel async cache scan
* Seamless certificate update
* Using ChaCha20 on hardware without AES acceleration
* Send filename to browser[^filename]
* Monitoring endpoints ([wiki](https://github.com/james58899/hath-rust/wiki/Monitoring))
* Strict SNI checking (Default off, may reduce quality)
* Post-quantum cryptography[^pqc]
* HTTP/3 (Experimental)
* Range request and ETag support[^range]

### Works
Features that are included in the official and are working.
* Cache and Proxy
* Gallery downloader
* Speed test
* Cache size management
* Logging
* Disk space check
* Download cache files through proxy
* Flood control

### No planned
* HTTP/2[^h2]
* Bandwidth limit

## Platform support
Please refer to the rust website for the platform name: https://doc.rust-lang.org/stable/rustc/platform-support.html

### Tier 1
Main supported platforms.  
Tested in real environments before release.

|          Platform         |
| ------------------------- |
| x86_64-unknown-linux-gnu  |
| x86_64-unknown-linux-musl |

### Tier 2
Secondary supported platforms.  
Due to the lack of hardware or real environment, it was not tested before release, relying on users to report bugs.

|            Platform            |
| ------------------------------ |
| aarch64-unknown-linux-gnu      |
| aarch64-unknown-linux-musl     |
| armv7-unknown-linux-gnueabihf  |
| armv7-unknown-linux-musleabihf |
| x86_64-pc-windows-msvc         |
| i686-pc-windows-msvc           |
| aarch64-pc-windows-msvc        |
| x86_64-apple-darwin            |
| aarch64-apple-darwin           |

### Tier 3
Experimental platform.  
Not guaranteed to work, may break at any time.

|         Platform        |
| ----------------------- |
| aarch64-linux-android   |
| armv7-linux-androideabi |
| i686-linux-android      |
| x86_64-linux-android    |


[^h2]: Multiplexing is useless for H@H, and a large number of connections will take up more system resources.
[^filename]: If the filename is not sent, some browsers may download using the wrong name.
[^pqc]: Support X25519MLKEM768 key agreement
[^range]: Allows the browser to resume interrupted image downloads.