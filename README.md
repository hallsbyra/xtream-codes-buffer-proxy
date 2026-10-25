# xtream-codes-buffer-proxy

Buffering proxy between a TV box (Formuler / MyTVOnline) and an Xtream Codes
IPTV provider whose servers cannot keep up. Point the TV at the proxy instead
of the provider; the proxy relays the API and buffers the streams.

- **Movies and episodes** are downloaded up to a buffer window (default
  30 minutes, set on the status page) ahead of where the TV plays, also while
  it is paused. Pause until the status page turns green, then play. What has
  been played is removed from disk again (except the last five minutes and
  the file's first and last 16 MB), so a 25 GB 4K film fits in a few GB.
  Seeking works; the download moves to wherever the TV reads.
- **Only what was asked for last downloads.** Switching title or channel
  stops the previous download; its data is kept for `VOD_KEEP_MINUTES` in
  case you go back.
- **Live channels** are written to a disk buffer of the same window. Pausing
  the TV makes it fall behind live while the buffer keeps filling. If the TV reconnects after a
  pause, it continues where it was. "Hoppa till live" on the status page jumps
  back.
- **One provider connection at a time; the device that asked last wins.**
  Another device asking for the same title or channel shares it. A device
  that just lost the connection cannot take it back for 30 seconds, so
  players that reconnect on their own do not start a tug of war.
- **Status page** at `/`: on top, in plain words for the whole family, what
  is playing and how many minutes are buffered (green, yellow, red). Below
  it, details: VPN, provider speed, live speed against real time (`1.00×`),
  the files on disk.
- **VPN kill switch** when paired with Gluetun (see below): the proxy shares
  Gluetun's network, so it has no network at all without Gluetun, and
  Gluetun's firewall only lets traffic out through the tunnel. The proxy also
  refuses provider requests unless Gluetun reports the tunnel as running, and
  the status page shows tunnel state and exit IP.

HLS (`.m3u8`) and catch-up streams are relayed without buffering; the status
page shows such a channel as unbuffered. Choose MPEG-TS as stream format on
the TV.

## Setup on the TV

In MyTVOnline, add an Xtream Codes portal with the proxy as server, e.g.
`http://<docker-host>:8099`, and the normal provider username and password.
The proxy never stores credentials; they pass through from the TV.

## Running

```sh
cp .env.example .env   # set UPSTREAM_URL and CACHE_PATH
docker compose up -d --build
```

| Variable | Default | Meaning |
|---|---|---|
| `UPSTREAM_URL` | – | Provider base URL without credentials |
| `UPSTREAM_PROXY` | – | HTTP proxy for provider traffic |
| `GLUETUN_CONTROL_URL` | – | Gluetun control server, e.g. `http://127.0.0.1:8000`; provider traffic then requires the VPN to be up |
| `CACHE_PATH` | `./data` | Host directory for the cache (compose only) |
| `VOD_CACHE_GB` | `10` | Downloaded movies kept before the oldest is evicted |
| `MIN_FREE_GB` | `2` | Pause background downloads below this free disk space |
| `BUFFER_MINUTES` | `30` | Default buffer window; the status page overrides it (saved in `settings.json`) |
| `VOD_KEEP_MINUTES` | `30` | Keep a title's data this long after switching to something else |
| `LIVE_BUFFER_GB` | `3` | Disk buffer per live channel |
| `LIVE_IDLE_MINUTES` | `10` | Keep a channel buffering this long after the TV left |
| `LIVE_START_DELAY_SECONDS` | `0` | Start new live viewers this far behind live |

## Behind Gluetun (WireGuard)

`compose.gluetun.yaml` runs the proxy in Gluetun's network namespace. Put the
WireGuard config next to it as `wg0.conf` (IPv4 address only; Gluetun rejects
the IPv6 address in Mullvad's configs when Docker has no IPv6) and
`gluetun-auth.toml`, which opens the two read-only control server routes the
status page uses. The control server port is not published.

```sh
docker compose -f compose.gluetun.yaml up -d
```

Use a WireGuard key of its own; two tunnels sharing a key knock each other
out. Pin the Docker network to a subnet that overlaps no LAN or VLAN, or
replies to clients on that range are routed into the bridge. Replies to LAN
clients leave via `eth0` (Gluetun's `from <container ip> lookup 200` rule),
not through the tunnel; check with
`docker exec <gluetun> ip route get <client ip> from <container ip>`.

## Development

Tests and lint run in Docker:

```sh
docker build --target test -t xcbuffer-test . && docker run --rm xcbuffer-test
```

The tests use a fake provider that counts concurrent stream connections, so a
change that opens a second provider connection fails them.

## License

MIT, see [LICENSE](LICENSE).
