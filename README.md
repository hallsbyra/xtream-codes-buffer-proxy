# xtream-codes-buffer-proxy

A buffering proxy for Xtream Codes IPTV providers whose servers cannot keep
up. Your player connects to the proxy instead of the provider. The proxy
relays the Xtream Codes API, with server addresses rewritten to point at
itself, and buffers the streams on disk, so a short pause builds up minutes
of margin and playback stops stuttering.

It is meant for one household with one provider subscription that allows a
single connection at a time.

## How it works

- **Movies and episodes** are downloaded up to a buffer window ahead of where
  the player reads (30 minutes by default, adjustable on the status page). The
  download continues while the player is paused, so you can pause until the
  status page turns green and then watch without interruptions. Seeking works:
  the download moves to wherever the player reads. What has been played is
  removed from disk again, except the last five minutes and the first and last
  16 MB of the file where containers keep their headers. A 25 GB 4K film
  therefore needs only a few GB of disk.
- **Live channels** are written to a disk buffer of the same window. Pausing
  the player makes it fall behind live while the buffer keeps filling. If the
  player reconnects after the pause, it continues where it left off. The status
  page can jump a viewer back to live.
- **One provider connection at a time.** The device that asked last wins.
  Another device asking for the same title or channel shares the stream
  instead. A device that just lost the connection cannot take it back for
  30 seconds, so players that reconnect on their own do not start a tug of war.
- **Only the latest title downloads.** Switching title or channel stops the
  previous download; its data is kept for a while in case you go back.
- **Credentials are never stored.** Username and password pass through from
  the player; the proxy only needs the provider's base URL.

## Status page

Open `http://<host>:8099/`. The top shows, in plain words, what is playing,
how many minutes are buffered ahead of the viewer and a timeline of the title
(green: just watch, yellow: pause for a while, red: pause and wait). Below it
are the details: buffer length setting, VPN state, provider connection,
provider speed against real time (`1.00×` keeps up, below that it does not)
and the files on disk.

## Quick start

```sh
git clone https://github.com/hallsbyra/xtream-codes-buffer-proxy.git
cd xtream-codes-buffer-proxy
cp .env.example .env    # set UPSTREAM_URL, the provider URL without credentials
docker compose up -d --build
```

Then, in your IPTV player, add an Xtream Codes login with the proxy as server,
for example `http://<host>:8099`, and your normal provider username and
password. Choose **MPEG-TS** as stream format if the player offers it; see
Limitations.

## Configuration

Set these in `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `UPSTREAM_URL` | – | Provider base URL without credentials, e.g. `http://provider.example:8080` |
| `CACHE_PATH` | `./data` | Host directory for the cache, mounted at `/data`; needs no backup |
| `BUFFER_MINUTES` | `30` | Default buffer window; the status page overrides it and saves the choice |
| `VOD_CACHE_GB` | `10` | Disk for movies and episodes before the least recently watched is evicted |
| `VOD_KEEP_MINUTES` | `30` | How long a title's data is kept after switching to something else |
| `MIN_FREE_GB` | `2` | Stop downloading ahead when the disk has less free space than this |
| `LIVE_BUFFER_GB` | `3` | Disk limit per live channel, on top of the time window |
| `LIVE_IDLE_MINUTES` | `10` | Keep a live channel buffering this long after its last viewer left |
| `LIVE_START_DELAY_SECONDS` | `0` | Start new live viewers this far behind live |
| `PASSTHROUGH_LINGER_SECONDS` | `20` | Keep the connection reserved this long after an unbuffered stream ends |
| `UPSTREAM_PROXY` | – | HTTP proxy for all provider traffic |
| `GLUETUN_CONTROL_URL` | – | Gluetun control server; provider traffic then requires the VPN to be up |

## Running behind a VPN with a kill switch

`compose.gluetun.yaml` runs the proxy inside the network namespace of a
[Gluetun](https://github.com/qdm12/gluetun) container:

- The proxy has no network of its own, so without Gluetun it cannot reach
  anything, and Gluetun's firewall only lets traffic out through the tunnel.
- The proxy also polls Gluetun's control server and refuses provider requests
  unless the tunnel is running. Downloads wait and resume when it is back.
- The status page shows tunnel state and exit IP.

Put your WireGuard config next to the compose file as `wg0.conf` and start it:

```sh
docker compose -f compose.gluetun.yaml up -d --build
```

`gluetun-auth.toml` opens the two read-only control server routes the status
page uses. The control server port is not published.

Things that have bitten us:

- Give the tunnel a WireGuard key of its own; two tunnels sharing a key knock
  each other out.
- Gluetun rejects an IPv6 interface address when Docker has no IPv6. Remove it
  from the `Address` line of `wg0.conf`.
- Pin the Docker network (`172.16.252.0/24` in the compose files) to a subnet
  that overlaps none of your LANs or VLANs, or replies to clients on that range
  are routed into the bridge.

## Limitations

- **HLS is not buffered.** Playlists and segments are relayed (and their URLs
  rewritten to go through the proxy), but only MPEG-TS live streams get the
  disk buffer. The status page marks HLS channels as unbuffered.
- **Catch-up / timeshift** requests are relayed without buffering.
- **Disk space is freed with `fallocate` hole punching**, which needs a cache
  filesystem that supports it, such as ext4, XFS or Btrfs. Elsewhere played
  data stays on disk until the title is evicted.
- **A slow provider stays slow.** The buffer absorbs uneven delivery and lets
  you build margin by pausing; it cannot make a provider that averages below
  real time keep up forever.

## Development

Tests and lint run in Docker:

```sh
docker build --target test -t xcbuffer-test . && docker run --rm xcbuffer-test
```

The tests run against a fake provider that counts concurrent stream
connections, so a change that opens a second provider connection fails them.

## License

MIT, see [LICENSE](LICENSE).
