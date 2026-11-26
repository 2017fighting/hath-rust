# hath-rust

An unofficial Hentai@Home client. Its identity with the tracker, the cache it serves, and the punch
it is reachable through are the three things it has to keep straight.

## The punch

**Punch**: The carrier mapping that makes one of this client's ports reachable from the internet
through a CGNAT, held open from the router. The public side is chosen by the carrier and moves; the
client only ever learns it. _Avoid_: port forward, NAT rule (that is the redirect behind the mapping,
not the mapping), hole punching (the hole is the effect, not the object).

**Punch File**: The document that publishes a Punch's current public tuple to this client — the
`punch.json` key of the mounted `<name>-punch` ConfigMap. Its *content* is the Punch's declaration: a
document with no public tuple means the Punch is down, and the file being there says nothing on its
own. _Avoid_: natmap.json, punch endpoint, "the ConfigMap" (the controller writes the ConfigMap; this
is the key the client reads), reading the file's presence as the signal.

**Mapping**: A Punch File that describes a working punch — a public tuple plus this client's Listen
Port. Anything else (absent, unparsable, wrong protocol, someone else's port) is no mapping.
_Avoid_: record, entry, document (when you mean a usable one).

**Public Port**: The port the carrier assigned to the Punch, and the one number the tracker must have
on record. Nothing can predict or choose it. _Avoid_: external port, WAN port, listen port.

**Bind Port**: The LAN-side port the router holds for the Punch and redirects to this client; the
Punch File publishes it as `inner_port`. It is not the number anything announces. _Avoid_: listen
port, internal port.

**Listen Port**: The port this client serves content on — what a Punch's Bind Port has to redirect
to for the punch to reach this client. _Avoid_: bind port, container port, "the port".

**Registered Port**: The port the tracker currently has on record for this client, changed through
the settings page. _Avoid_: "the port", announced port, current port.

**Halted**: The client's state while the Punch yields no mapping: suspended with the tracker, still
running, still reading the Punch File, and re-registering as soon as a mapping returns. It is not a
crash and not a restart. _Avoid_: stopped, down, crashed, paused.
