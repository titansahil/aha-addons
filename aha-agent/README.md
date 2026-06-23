# AHA Agent (Home Assistant add-on)

The AHA Agent runs **on the customer's box** (a Home Assistant machine such as
HA Green/Yellow, a Raspberry Pi, or an x86 mini-PC). It is the bridge between
your local cameras and the AHA app.

## What it does (the switchboard / P2P model)

On boot the agent:

1. **Auto-discovers cameras** by asking the box's local **go2rtc**
   (`GET {go2rtc_url}/api/streams`). Every go2rtc stream becomes one camera.
2. **Registers** the box and that camera list with the **AHA cloud**
   (`POST {backend_url}/register_box`).
3. Opens an **outbound WebSocket** to the cloud (`/ws/box/{device_id}`) and
   holds it open, reconnecting forever.

When you tap a camera in the AHA app, the cloud acts only as a **switchboard**:
it relays the app's WebRTC **offer** to this box over that WebSocket, and the
box forwards it to the matching go2rtc stream and returns the **answer**. After
that handshake, **live video flows phone ↔ box directly (peer-to-peer,
DTLS-SRTP) and never travels through the cloud.** By default there is no TURN
relay (pure P2P, STUN-only), so a minority of viewers behind very strict/CGNAT
networks may be unable to connect. The operator can OPTIONALLY enable a company
TURN relay on the cloud (served to viewers via `GET /ice-config`); a TURN relay
only forwards encrypted DTLS-SRTP media, so the cloud still never sees video — it
just costs bandwidth, hence it is opt-in.

The agent only ever makes **outbound** connections (to the cloud and to the
local go2rtc). It opens no inbound port of its own. (go2rtc itself still needs
UDP/TCP **8555** reachable for the P2P media to flow.)

---

## Install (local add-on)

1. Copy this `aha-agent` folder into your Home Assistant `/addons` share
   (Samba/SSH add-on → `/addons/aha-agent`).
2. **Settings → Add-ons → Add-on Store → ⋮ (top-right) → Reload.**
3. Under **Local add-ons**, open **AHA Agent → Install**.
4. Open the **Configuration** tab. The main option is **`box_name`** (a
   friendly label); the home is set by the **app** at claim time (the Tuya homeId),
   not here. Everything else is baked into the build: the AHA
   cloud URL, the bootstrap key, and go2rtc at the local `127.0.0.1:1984`; the box
   also auto-assigns its own id (ownership comes from the claim code). (If a box
   runs go2rtc on a non-standard port, set the `AHA_GO2RTC_PORT` env var — an
   escape hatch, not a UI option.)
5. On the **Info** tab enable **Start on boot** and **Watchdog**, then **Start**.
6. On the **Log** tab you should see lines like:

   ```
   [box] registered N camera(s)
   [box] connecting to ws://.../ws/box/aha-xxxxxxxxxxxx
   [box] connected — serving N camera(s) — waiting for requests
   ```

If you instead see `[box] waiting for go2rtc (...)`, the agent can't reach
go2rtc yet — check that go2rtc is running on the standard port 1984 (or set
`AHA_GO2RTC_PORT` if it listens elsewhere).

---

## Claiming the box (who owns it)

A freshly-installed box is **unclaimed** — it belongs to no account yet. On boot
the agent raises a **Home Assistant notification** (and logs the same) with a
one-time **claim code**:

```
THIS BOX IS NOT YET CLAIMED.
Open the AHA app, sign in, and enter claim code:  ABCD1234
```

In the AHA app: **Add a box** → enter that code. The box and its cameras are
then bound to that home (the Tuya homeId). The code is single-use,
and the notification clears once the box is claimed. **The owner only ever needs
the code — there is no device id to copy.** (This uses `homeassistant_api: true`,
already set in this add-on's config; running outside Home Assistant, the code
appears only in the Log.)

---

## Options

These keys map **exactly** to what the agent reads from `/data/options.json`.

| Option        | Type | Meaning |
|---------------|------|---------|
| `box_name`    | str  | Friendly name shown to the user in the AHA app (e.g. "Front House"). |
| `cameras`     | list? | **Optional (feature b).** Camera/NVR sources to write into the local go2rtc on boot, so onboarding is "enter your NVR login" instead of editing go2rtc YAML. Each entry: `name` (required) + EITHER a full `src` (advanced) OR parts `type` (onvif\|rtsp), `host`, `port`, `username`, `password`, `path`. Empty = serve whatever go2rtc already has. Credentials are never logged. |
| `camera_include` | str? | **Optional (feature b).** Comma-separated `camera_id`s to serve EXCLUSIVELY — lets several boxes share one go2rtc with disjoint sets. |
| `camera_exclude` | str? | **Optional (feature b).** Comma-separated `camera_id`s to drop (ignored if `camera_include` is set). |
| `onvif_username` | str? | **Optional (feature b+).** SHARED ONVIF username. When set, the agent WS-Discovers ONVIF cameras on the LAN, resolves each main stream, and auto-numbers them `cam1..camX` — no IPs/paths to type. Cameras on different creds can still be added via `cameras`. |
| `onvif_password` | password? | **Optional (feature b+).** SHARED ONVIF password (paired with `onvif_username`). |

> A box also generates a **per-box identity secret** on first boot
> (`/data/aha_box_secret`, feature a) and pushes **telemetry** when telemetry is
> enabled (feature c) — neither is an option; both are automatic.

> **`box_name` (and the optional camera fields above) are the only add-on options.** The home is set by the app at claim time (Tuya homeId), not here. Everything else is baked into the
> build:
> - **No `backend_url`/`register_key`** — the AHA cloud URL and bootstrap key are
>   hardcoded in `box_client.py` (the `AHA_BACKEND_URL` / `AHA_BOX_REGISTER_KEY`
>   env vars still override them for dev). The baked key gates registration but is
>   extractable from firmware — it is not per-box identity; see the per-box-keypair
>   roadmap item.
> - **No go2rtc setting** — go2rtc is always local at `127.0.0.1:1984` (the standard
>   add-on port). For the rare non-default port, set the `AHA_GO2RTC_PORT` env var
>   (an escape hatch, not a UI option).
>
> **No `device_id`/`user_id` options.** The box's id is assigned automatically
> (derived from the host NIC MAC, else machine-id, else a persisted random id —
> stable across reboots AND add-on reinstalls) and ownership is set by the claim
> code — so there is nothing
> identity-related to type. A manufacturer can still bake a fixed serial via the
> `AHA_DEVICE_ID` env var, but it is not required. (Earlier drafts also mentioned
> `stream_url`/`go2rtc_api`; those were never real options.)

---

## Repurpose to ANY box (how this scales)

There is **nothing camera-specific or identity-specific** baked into this
add-on. To bring a brand new box online:

1. Install this same add-on on that box.
2. Set a friendly `box_name` — that is the only option. The AHA cloud URL and
   bootstrap key are baked into `box_client.py` (dev override via the
   `AHA_BACKEND_URL` / `AHA_BOX_REGISTER_KEY` env vars); go2rtc is baked at the
   local `127.0.0.1:1984` (override via `AHA_GO2RTC_PORT`). There is no
   `device_id`/`user_id` to set: the box assigns its own id automatically and
   ownership is established later via the claim code.
3. Start it. The agent auto-discovers whatever cameras that go2rtc exposes and
   registers them — no per-camera configuration, no code changes.

That is the entire onboarding story: same image, point it at your cloud, claim
it in the app, many boxes.

---

## Prebuilt image vs. building on-device (the OOM lesson)

By default this add-on **builds the image on the box** from the `Dockerfile`
here. The AHA agent is tiny (pure Python + the `websockets` lib), so this is
fine for one box. **At fleet scale you should NOT build on-device** — building
heavier images on a weak box (HA Green, 4 GB) can OOM Home Assistant.

Instead, build a **multi-arch image once** and have boxes **pull** it:

1. Run the build/push script (edit the `REGISTRY` placeholder first):

   ```bash
   ../build-and-push.sh
   ```

   It uses `docker buildx` to build `linux/arm64,linux/amd64` and push to your
   registry. See that script for full comments.

2. Point this add-on at the prebuilt image by adding an `image:` key to
   `config.yaml` (and removing the local `Dockerfile` build). For example:

   ```yaml
   # config.yaml — pull a prebuilt image instead of building on the box.
   # {arch} is substituted by Supervisor (aarch64 / amd64 / armv7).
   image: "ghcr.io/YOUR_ORG/aha-agent-{arch}"
   version: "1.0.0"   # must match a tag you pushed
   ```

   With `image:` set, Supervisor **pulls** the matching-arch image — no build,
   no OOM risk on the box. (Make sure the `arch:` list here matches the
   platforms you actually pushed.)
