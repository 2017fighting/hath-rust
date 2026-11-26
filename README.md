# hath-rust
[![Build](../../actions/workflows/build.yml/badge.svg)](../../actions/workflows/build.yml) [![Docker Pulls](https://img.shields.io/docker/pulls/james58899/hath-rust)](https://hub.docker.com/r/james58899/hath-rust)

The unofficial Hentai@Home client written in Rust.

## Install
Read the [Wiki](https://github.com/james58899/hath-rust/wiki/Install)

## NAT port sync
hath must tell the H@H tracker which public port it is reachable on, so it runs a port-sync loop
that re-reads that port and updates the settings page when it changes. The port is read from one of
two sources:

1. **a file** — `HATH_PUNCH_FILE` (`--punch-file`), the path of the `punch.json` document a punch
   controller mounts from its ConfigMap, e.g. `/punch/punch.json`. Content is
   `{"ip": "<public ip>", "port": <public port>, "inner_port": <bind port>, "protocol": "tcp"}`.
2. **the natmap HTTP API** — `HATH_NATMAP_API_ENDPOINT` (`--natmap-api-endpoint`, e.g.
   `http://192.168.9.1/natmap`) plus `HATH_NATMAP_INSTANCE` (`--natmap-instance`), polled at
   `<endpoint>/<instance>.json`.

The file wins: it is re-read on every tick and is the source a punch controller writes. A missing or
unparsable file is *no mapping from that source this pass* — logged once per change of state, never
fatal — and the HTTP source is polled as the fallback when one is configured. With neither source
configured, port-sync stays off.

Whichever source answers, the rest of the loop is unchanged: `HATH_IPB_MEMBER_ID` and
`HATH_IPB_PASS_HASH` (`--ipb-member-id`, `--ipb-pass-hash`) authenticate the settings POST, and
`HATH_NATMAP_POLL_INTERVAL` (`--natmap-poll-interval`, default 60) sets the tick.

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