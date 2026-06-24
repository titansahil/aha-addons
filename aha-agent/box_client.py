"""
AHA Box agent — the UNIVERSAL, repurposable piece of the platform.
=================================================================

WHAT THIS IS
------------
This single file runs ON a customer's "box" (today: a Home Assistant device that
also runs go2rtc; tomorrow: literally any Linux box that can reach a go2rtc
instance). It is the *only* component that needs to live on the customer's
premises, and it is designed to be dropped onto ANY box with three settings and
no code changes.

It does three jobs, in order:

  1. DISCOVER  — asks the LOCAL go2rtc "what camera streams do you have?" so the
                 agent never needs a hand-maintained camera list. Whatever go2rtc
                 knows about, the box serves.
  2. REGISTER  — tells the cloud "I am box <device_id> and here are my cameras."
                 The box does NOT assert an owner; it registers UNCLAIMED and a
                 human binds it to their account later with the one-time claim
                 code (see register()). The cloud stores this in its registry so
                 the owner's app can list and select cameras.
  3. SIGNAL    — opens ONE outbound WebSocket to the cloud and holds it open
                 forever. The cloud uses that pipe to forward WebRTC "offers"
                 from a viewer's app. The agent hands each offer to the right
                 go2rtc stream and returns the "answer".

THE SWITCHBOARD / P2P MODEL (read this before touching anything)
---------------------------------------------------------------
The cloud is ONLY a switchboard: it does authentication, WebRTC *signaling*
relay, and the box/camera registry. It never sees a single video frame.

Signaling (the small SDP offer/answer handshake) travels:
        viewer app  ->  cloud  ->  this WebSocket  ->  go2rtc
        go2rtc      ->  this WebSocket  ->  cloud  ->  viewer app

After that handshake, the actual video is a DIRECT peer-to-peer WebRTC stream
(DTLS-SRTP) between the viewer's app and go2rtc on this box. Video does NOT flow
through this agent's process and does NOT flow through the cloud. By default the
platform is pure STUN/P2P, so ~15-20% of strict-NAT/CGNAT viewers may fail to
connect on STUN alone. An OPTIONAL company TURN relay (served to viewers via the
cloud's GET /ice-config) can recover those clients; even when TURN is used it
only relays DTLS-SRTP ciphertext, so the cloud still never sees video. None of
this changes the agent — video still never traverses this process.

WHY THE WEBSOCKET IS *OUTBOUND* (dials the cloud, not the other way around)
--------------------------------------------------------------------------
Customer boxes sit behind home NAT/firewalls with no public inbound port. By
having the box DIAL OUT to the cloud and hold the connection open, the cloud can
push signaling requests to the box at any time without the customer having to
open ports or run a public server. This is how the control plane stays cheap and
how the system scales to many boxes.

HOW TO REPURPOSE THIS ONTO ANY BOX (the whole point)
----------------------------------------------------
There is nothing box-specific in this code, and on consumer hardware there is
NOTHING for the customer to type. Identity is automatic:

    * device_id — ASSIGNED AUTOMATICALLY (see _resolve_device_id): derived from
                  the host machine-id, else a random id, persisted so it is stable.
                  A manufacturer may still bake a fixed serial via the device_id
                  option / AHA_DEVICE_ID env, but no human ever needs to.
    * owner     — NOT set here at all. The box starts UNCLAIMED; a human claims it
                  with the one-time claim code (see register()). There is no user_id.

The cloud to dial (BACKEND_URL), the bootstrap key (REGISTER_KEY), and go2rtc
(always local at 127.0.0.1:1984) are ALL HARDCODED as appliance defaults below —
not add-on options — so the customer never sees or types them. The main add-on
option is box_name (a friendly label). (Escape hatches for dev/edge cases:
AHA_BACKEND_URL, AHA_BOX_REGISTER_KEY, AHA_GO2RTC_PORT env vars override the baked
values.) Then start the agent: it auto-discovers whatever cameras that go2rtc has,
self-registers under its auto device_id, and serves. That is the repurpose story:
same image, one optional field, N boxes.

CONFIG RESOLUTION (priority order)
----------------------------------
  1. /data/options.json   — written by the Home Assistant add-on UI
  2. environment variables — when run standalone / for local testing
  3. built-in defaults     — last resort so the agent still boots

NOTE ON THE CLOUD CONTRACT
--------------------------
This agent speaks a fixed protocol with the cloud backend (main.py). The shapes
below MUST stay in lockstep with it:
  * POST /register_box body: {device_id, name, cameras:[{camera_id,name,src}]}
    (no user_id — ownership comes from /claim, not the agent. home_code is NOT sent
    by the agent — the app sets it at claim time. The cloud also accepts an optional
    legacy stream_url; the agent never sends it — derived from cameras[0].src.)
    Headers: X-AHA-Register-Key (interim shared bootstrap key) AND, NEW,
    X-AHA-Box-Secret (the per-box identity secret — the cloud binds it to this
    device_id on first sight (TOFU) and thereafter requires it). See feature (a).
  * WS  /ws/box/{device_id}:
      - signaling:       cloud -> {id, type:"request", payload:{type:"offer",sdp,
                         camera_id}};  box -> {id, type:"reply", result:{...}}.
      - configure:       cloud -> {id, type:"request", payload:{kind:"configure_cameras",
                         onvif_username, onvif_password}};  box -> {id, type:"reply",
                         result:{ok, cameras:N}}. The app pushes NVR creds at claim time;
                         the box persists them, runs ONVIF, re-registers. (config-push)
      - ptz:             cloud -> {id, type:"request", payload:{kind:"ptz", camera_id,
                         pan, tilt, zoom}}; box -> {id, type:"reply", result:{ok,action}}.
                         ONVIF pan/tilt/zoom; all-zero velocities = stop. (PTZ)
      - telemetry PULL:  cloud -> {id, type:"request", payload:{kind:"telemetry"}};
                         box -> {id, type:"reply", result:<snapshot>}.  (feature (c))
      - telemetry PUSH:  box -> {type:"telemetry", device_id, data:<snapshot>}
                         unsolicited, on connect + every AHA_TELEMETRY_INTERVAL s.
Do not change these without changing the cloud at the same time.

THREE OPT-IN UPGRADES (all default OFF / behavior-preserving):
  (b) NVR/camera AUTO-CONFIG — write camera credentials into the local go2rtc so
      onboarding is "enter your NVR login", not hand-edited go2rtc YAML. Optionally
      ONVIF-DISCOVER cameras on the LAN from just a shared username/password (auto-
      numbered cam1..camX, main stream each). Plus an include/exclude filter to split
      one go2rtc across several boxes.
  (c) TELEMETRY — relay as much HA data as we can (all entity states + config +
      Supervisor host/OS/add-on info) up the existing socket (push + pull).
  (a) PER-BOX IDENTITY — a secret generated ON THE BOX (never shipped in the image)
      that binds this device_id so it can't be hijacked.
"""
import asyncio
import hashlib
import http.server
import json
import logging
import os
import re
import secrets
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import websockets

# Silence noisy WARNINGS from python-zeep's async SOAP daemon — it routinely
# logs "could not find handler for: _handle_resolve / _handle_probe" when
# probing ONVIF devices. They're harmless dispatcher misses (we don't care
# about the unsolicited responses), but they clutter the agent log.
logging.getLogger("daemon").setLevel(logging.ERROR)
logging.getLogger("zeep").setLevel(logging.ERROR)
logging.getLogger("zeep.transports").setLevel(logging.ERROR)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def _load_ha_options() -> dict:
    """Load the Home Assistant add-on options file, if present.

    When this agent runs as an HA add-on, Home Assistant writes the user's
    chosen options to /data/options.json. When it runs standalone (plain
    Docker / a shell), that file does not exist and we fall back to env vars.
    A missing or malformed file is NOT fatal — we just return {} so the env-var
    / default path takes over. This is what lets the SAME file run in both
    environments unchanged.
    """
    try:
        with open("/data/options.json") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


_opts = _load_ha_options()


def _cfg(key: str, env: str, default: str) -> str:
    """Resolve one config value using the priority order documented above.

    Order: HA add-on option (`key`) -> environment variable (`env`) -> `default`.
    An option present but empty ("" or null) is treated as "not set" so a blank
    field in the add-on UI correctly falls through to the env var / default.
    """
    v = _opts.get(key)
    if v not in (None, ""):
        return str(v)
    return os.getenv(env, default)


# The cloud switchboard this box dials into (HTTP for REST, derived WS for
# signaling). NOT hardcoded — the public repo carries no server address. It is set
# at install time as the add-on option `backend_url` (the install script writes it),
# or via the AHA_BACKEND_URL env var for dev. Empty default = the agent has no cloud
# to talk to until provisioned, which is the intended "non-hardcoded" behaviour.
BACKEND_URL = _cfg("backend_url", "AHA_BACKEND_URL", "")
# Human-friendly label shown in the app.
BOX_NAME = _cfg("box_name", "AHA_BOX_NAME", "My Box")
# go2rtc ALWAYS runs on the HA box itself at the standard add-on port 1984, so BOTH
# the host (127.0.0.1) AND the port are baked in — nothing to configure for the
# normal case. AHA_GO2RTC_PORT stays as an escape hatch (env override only, NOT an
# add-on UI option) for the rare box whose go2rtc listens on a different port. Its
# streams ARE the cameras this box serves.
GO2RTC_PORT = os.getenv("AHA_GO2RTC_PORT", "1984")
GO2RTC_URL = f"http://127.0.0.1:{GO2RTC_PORT}"
# NOTE: there is intentionally NO user_id config anymore. The agent never tells
# the cloud who owns the box — ownership is set by a human via the claim code.
# (See _resolve_device_id() below for how the box's id is assigned automatically.)


# Where we persist an auto-generated device id so it stays stable across reboots
# and add-on updates (HA keeps /data; standalone falls back to a temp path).
_DEVICE_ID_FILE = "/data/aha_device_id" if os.path.isdir("/data") else "/tmp/aha_device_id"


def _host_mac() -> str | None:
    """Return the HOST's primary network-interface MAC, lowercased, colon-form.

    WHY MAC over machine-id: inside an HA add-on the agent runs in a container,
    and /etc/machine-id is the CONTAINER's id — it changes every time the add-on
    is reinstalled (container recreated). The host's physical NIC MAC does NOT
    change across reinstalls, reboots, or HAOS updates, so it's a far more
    durable identity for the box hardware.

    Resolution order:
      1. Supervisor /network/info — the reliable HA-add-on path. Returns the
         host's interfaces; we take the one flagged `primary`, else the first
         ethernet, else the first with a non-zero MAC.
      2. /sys/class/net/<iface>/address — works when the container can see host
         interfaces (host_network) or standalone on bare metal. We skip virtual
         interfaces (lo, docker*, veth*, hassio*, br-*).
      3. Python uuid.getnode() — last resort; may be randomized (detectable via
         the locally-administered bit) in which case we reject it.
    Returns a MAC string like "aa:bb:cc:dd:ee:ff" or None."""
    # (1) Supervisor network info — best source inside an HA add-on.
    # Read the token via os.getenv directly (not the module global) because
    # _host_mac runs at import time, before the SUPERVISOR_TOKEN global is bound.
    _sup_token = os.getenv("SUPERVISOR_TOKEN")
    if _sup_token:
        try:
            req = urllib.request.Request(
                "http://supervisor/network/info",
                headers={"Authorization": f"Bearer {_sup_token}"},
            )
            with urllib.request.urlopen(req, timeout=5) as r:
                data = json.loads(r.read().decode()).get("data", {})
            ifaces = data.get("interfaces", []) or []
            # primary first, then ethernet, then anything with a MAC
            ordered = (
                [i for i in ifaces if i.get("primary")]
                + [i for i in ifaces if i.get("type") == "ethernet"]
                + ifaces
            )
            for i in ordered:
                mac = (i.get("mac") or "").strip().lower()
                if mac and mac != "00:00:00:00:00:00":
                    return mac
        except Exception:
            pass
    # (2) /sys/class/net scan.
    try:
        import glob
        skip = ("lo", "docker", "veth", "hassio", "br-", "wg", "tun", "tap")
        candidates = []
        for path in sorted(glob.glob("/sys/class/net/*/address")):
            iface = path.split("/")[-2]
            if any(iface.startswith(s) for s in skip):
                continue
            try:
                with open(path) as f:
                    mac = f.read().strip().lower()
            except OSError:
                continue
            if mac and mac != "00:00:00:00:00:00":
                candidates.append((iface, mac))
        # Prefer ethernet-looking names (eth*, end*, enp*, eno*) over wlan.
        candidates.sort(key=lambda c: (not c[0].startswith(("eth", "end", "enp", "eno")), c[0]))
        if candidates:
            return candidates[0][1]
    except Exception:
        pass
    # (3) uuid.getnode() — reject if the locally-administered bit is set (random).
    try:
        node = uuid.getnode()
        if not (node >> 40) & 0x02:  # bit 41 = locally administered (random)
            return ":".join(f"{(node >> (8 * i)) & 0xff:02x}" for i in reversed(range(6)))
    except Exception:
        pass
    return None


def _stable_hardware_id() -> str | None:
    """A stable per-box id derived from the host MAC (preferred), falling back to
    the host machine-id. Hashed so the raw MAC/machine-id never leaves the box and
    the id is a tidy fixed length. Returns None if neither is readable (then
    _resolve_device_id falls back to a random, persisted id)."""
    mac = _host_mac()
    if mac:
        return "aha-" + hashlib.sha256(mac.encode()).hexdigest()[:12]
    # Fallback: machine-id (legacy behaviour; container-scoped, less durable).
    for path in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            with open(path) as f:
                mid = f.read().strip()
            if mid:
                return "aha-" + hashlib.sha256(mid.encode()).hexdigest()[:12]
        except OSError:
            continue
    return None


def _resolve_device_id() -> str:
    """Figure out this box's UNIQUE device id WITHOUT anyone typing one in.

    Priority (first hit wins):
      1. An EXPLICIT override (add-on `device_id` option or AHA_DEVICE_ID env) —
         lets a manufacturer bake a fixed serial into the image if they want one.
         The old placeholder "BOX001" is treated as "unset" so it never collides.
      2. A previously persisted auto id (so the id is stable across reboots/updates).
      3. A freshly derived id — from the host machine-id, else a random uuid — which
         we then persist so step 2 finds it next time.

    Net effect for consumer hardware: install the add-on, power on, and the box
    self-assigns a stable unique id. No device_id field for the customer to fill in."""
    explicit = _opts.get("device_id") or os.getenv("AHA_DEVICE_ID")
    if explicit and explicit not in ("", "BOX001"):
        return str(explicit)
    try:
        with open(_DEVICE_ID_FILE) as f:
            saved = f.read().strip()
        if saved:
            return saved
    except OSError:
        pass
    device_id = _stable_hardware_id() or ("aha-" + uuid.uuid4().hex[:12])
    try:
        with open(_DEVICE_ID_FILE, "w") as f:
            f.write(device_id)
    except OSError:
        pass  # non-fatal: we still run this boot with the id, just can't persist it
    return device_id


# UNIQUE identity of this box, assigned automatically (see _resolve_device_id).
DEVICE_ID = _resolve_device_id()
# Legacy shared "bootstrap key". RETIRED from the appliance build: the default is
# now EMPTY so the public repo carries no shared secret. It is sent ONLY if a
# deployment explicitly sets it (add-on option `register_key` or AHA_BOX_REGISTER_KEY
# env) to match a cloud that still enforces it during transition. The real gates are
# app-driven provisioning (POST /provision_box) + the per-box TOFU secret below.
REGISTER_KEY = _cfg("register_key", "AHA_BOX_REGISTER_KEY", "")
# Present ONLY when running as a Home Assistant add-on with `homeassistant_api:
# true` in config.yaml. It lets the agent call HA's own API (via the Supervisor
# proxy) to raise the "claim your box" notification. Absent when run standalone,
# in which case the notification helpers below quietly no-op.
SUPERVISOR_TOKEN = os.getenv("SUPERVISOR_TOKEN")


# ---------------------------------------------------------------------------
# (b) camera/NVR auto-config  +  (c) telemetry  +  (a) per-box identity — config
# All three are OPT-IN and default to off / behavior-preserving, so an existing box
# behaves exactly as before.
# ---------------------------------------------------------------------------
def _opt_list(key: str, env: str) -> list:
    """Resolve a list-valued setting: an add-on list option `key`, else a
    comma-separated env var `env`. Returns trimmed, non-empty strings."""
    v = _opts.get(key)
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    raw = v if isinstance(v, str) and v else os.getenv(env, "")
    return [x.strip() for x in str(raw).split(",") if x.strip()]


def _load_cameras_cfg() -> list:
    """(b) The camera/NVR sources to WRITE into the local go2rtc. Source of truth:
    add-on option `cameras` (a list of objects) or env AHA_CAMERAS (a JSON list).
    Empty = nothing to configure (the box serves whatever go2rtc already has)."""
    opt = _opts.get("cameras")
    if isinstance(opt, list):
        return opt
    raw = os.getenv("AHA_CAMERAS", "").strip()
    if raw:
        try:
            v = json.loads(raw)
            if isinstance(v, list):
                return v
            print("[box] AHA_CAMERAS must be a JSON list; ignoring")
        except json.JSONDecodeError:
            print("[box] AHA_CAMERAS is not valid JSON; ignoring")
    return []


# (b) Cameras to auto-configure into go2rtc, plus an optional include/exclude filter
# on the DISCOVERED streams — lets two boxes share ONE go2rtc and each serve a
# DISJOINT subset (handy for testing / multi-tenant demos). INCLUDE wins if both set.
CAMERAS_CFG = _load_cameras_cfg()
CAMERA_INCLUDE = _opt_list("camera_include", "AHA_CAMERA_INCLUDE")
CAMERA_EXCLUDE = _opt_list("camera_exclude", "AHA_CAMERA_EXCLUDE")

# (b+) ONVIF AUTO-DISCOVERY. If SHARED camera credentials are provided, the agent
# WS-Discovers ONVIF cameras on the LAN, resolves each one's MAIN-profile RTSP URL,
# auto-numbers them cam1..camX, and writes them into go2rtc — so onboarding is just
# "give the agent a username + password", no IPs/paths. Manual `cameras` entries
# (above) are still honored and added alongside, so a camera that isn't ONVIF (or uses
# different creds) can be added by hand. Needs the optional wsdiscovery + onvif-zeep
# libs (shipped in the add-on image); without them this silently no-ops.
# Camera creds can also be PUSHED by the app at claim time (config-push). When pushed
# they are persisted here and OVERRIDE the add-on options, so the app becomes the
# source of truth once it configures a box. The file is 0600 and never logged.
# Path resolution: AHA_CAMERA_CREDS_FILE env wins; else /data/... if HA add-on; else /tmp/...
_CAMERA_CREDS_FILE = os.getenv("AHA_CAMERA_CREDS_FILE") or (
    "/data/aha_camera_creds.json" if os.path.isdir("/data") else "/tmp/aha_camera_creds.json"
)
print(f"[box] camera-creds file: {_CAMERA_CREDS_FILE}")

# Persisted home_code — the cloud pushes this down the WS the moment a
# /claim succeeds (kind="claimed"), and clears it on /unpair (kind="unclaimed").
# Local copy so the add-on log can show "paired with home X" without round-trips.
_HOME_CODE_FILE = os.getenv("AHA_HOME_CODE_FILE") or (
    "/data/aha_home_code.txt" if os.path.isdir("/data") else "/tmp/aha_home_code.txt"
)
print(f"[box] home-code file:    {_HOME_CODE_FILE}")

# Persisted BOOTSTRAP token — the app pushes it to this box over the LAN (see the
# provisioning endpoint below) at pairing. The agent presents it ONCE on its first
# /register_box (X-AHA-Provision-Token); the cloud verifies it, enrolls this box's
# own secret, and burns the token — after which the agent deletes its local copy and
# authenticates with its secret alone. See SECURITY.md §5b.
_PROVISION_TOKEN_FILE = os.getenv("AHA_PROVISION_TOKEN_FILE") or (
    "/data/aha_provision_token" if os.path.isdir("/data") else "/tmp/aha_provision_token"
)
# Port for the agent's LAN provisioning HTTP endpoint. host_network is on, so this is
# reachable from the app on the same Wi-Fi. The app POSTs {home_code, token, creds}.
PROVISION_PORT = int(os.getenv("AHA_PROVISION_PORT", "8099"))
_MAX_PROVISION_BODY = 64 * 1024   # cap the unauthenticated LAN /provision POST body
print(f"[box] provision-token file: {_PROVISION_TOKEN_FILE}")


def _read_provision_token() -> "str | None":
    try:
        with open(_PROVISION_TOKEN_FILE) as f:
            return f.read().strip() or None
    except OSError:
        return None


def _write_provision_token(token: str) -> None:
    with open(_PROVISION_TOKEN_FILE, "w") as f:
        f.write(token)
    try:
        os.chmod(_PROVISION_TOKEN_FILE, 0o600)
    except OSError:
        pass


def _clear_provision_token() -> None:
    try:
        os.remove(_PROVISION_TOKEN_FILE)
    except OSError:
        pass


def _persist_home_code(hc: str) -> None:
    """Write the bound home_code locally (best-effort) so the add-on log/diagnostics
    can show which home this box belongs to without a cloud round-trip."""
    if not hc:
        return
    try:
        with open(_HOME_CODE_FILE, "w") as f:
            f.write(hc)
        os.chmod(_HOME_CODE_FILE, 0o600)
        print(f"[box] persisted home_code -> {_HOME_CODE_FILE}")
    except OSError as e:
        print(f"[box] could not persist home_code ({e})")


def _is_claimed() -> bool:
    """True if this box is bound to a home (a home_code is cached locally)."""
    try:
        with open(_HOME_CODE_FILE) as f:
            return bool(f.read().strip())
    except OSError:
        return False


def _load_persisted_camera_creds() -> dict:
    try:
        with open(_CAMERA_CREDS_FILE) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _save_persisted_camera_creds(creds: dict) -> None:
    try:
        with open(_CAMERA_CREDS_FILE, "w") as f:
            json.dump(creds, f)
        os.chmod(_CAMERA_CREDS_FILE, 0o600)
        print(f"[box] wrote camera creds -> {_CAMERA_CREDS_FILE}  (user={creds.get('onvif_username','')!r}, pw len={len(creds.get('onvif_password') or '')})")
    except OSError as e:
        print(f"[box] could not persist camera creds ({e})")


_persisted_creds = _load_persisted_camera_creds()
# Persisted (app-pushed) creds win over add-on options / env.
ONVIF_USER = _persisted_creds.get("onvif_username") or _cfg("onvif_username", "AHA_ONVIF_USER", "")
ONVIF_PASS = _persisted_creds.get("onvif_password") or _cfg("onvif_password", "AHA_ONVIF_PASS", "")
ONVIF_DISCOVERY_TIMEOUT = int(os.getenv("AHA_ONVIF_TIMEOUT", "5"))

# (c) How often (seconds) to push a full HA telemetry snapshot up the signaling
# socket. 0 = never push periodically (the cloud can still PULL on demand). Telemetry
# only works as an HA add-on (needs SUPERVISOR_TOKEN); standalone it is a no-op.
TELEMETRY_INTERVAL = int(os.getenv("AHA_TELEMETRY_INTERVAL", "300"))

# Agent build version — surfaced in telemetry so the fleet's versions are visible.
AGENT_VERSION = "1.3.0"

# (a) Per-box identity secret. Generated ON THE BOX on first boot and persisted, so
# the distributed add-on image carries NO fleet-wide secret to extract. Presented on
# every /register_box; the cloud binds it to this device_id on first sight (TOFU) and
# thereafter requires it, so no one can hijack this box's device_id without it. (The
# stronger per-box keypair + signed registration is the next step; this is the
# stdlib-only version that needs no new dependency.)
_BOX_SECRET_FILE = "/data/aha_box_secret" if os.path.isdir("/data") else "/tmp/aha_box_secret"


def _resolve_box_secret() -> str:
    try:
        with open(_BOX_SECRET_FILE) as f:
            s = f.read().strip()
        if s:
            return s
    except OSError:
        pass
    s = secrets.token_urlsafe(32)
    try:
        with open(_BOX_SECRET_FILE, "w") as f:
            f.write(s)
        os.chmod(_BOX_SECRET_FILE, 0o600)
    except OSError:
        pass  # non-fatal: we still use the secret this boot, just can't persist it
    return s


BOX_SECRET = _resolve_box_secret()

# Discovered map of camera_id -> go2rtc src, populated at startup by run().
# This is the SSRF allow-list (see handle_request): only srcs that go2rtc itself
# reported land here, so a viewer can never make us forward to an arbitrary src.
CAMERA_SRC: dict = {}

# camera_id -> {host, port, token}: the ONVIF endpoint + profile token for PTZ, built
# alongside CAMERA_SRC during ONVIF discovery. Empty for manually-added / non-ONVIF cams.
CAMERA_PTZ: dict = {}


def ws_url() -> str:
    """Build the cloud WebSocket URL for this box from BACKEND_URL.

    The backend is configured as an HTTP(S) URL (used for the REST register
    call); the signaling channel is the same host over the WebSocket scheme.
    We swap http->ws / https->wss and append the per-box path the cloud listens
    on. https->wss is replaced first so the http->ws pass can't corrupt it.
    """
    base = BACKEND_URL.replace("https://", "wss://").replace("http://", "ws://")
    return f"{base}/ws/box/{DEVICE_ID}"


# ---------------------------------------------------------------------------
# go2rtc self-bootstrap — install + start the go2rtc add-on automatically so the
# customer never has to. Best-effort: if anything here fails, the agent keeps
# running (camera discovery just finds nothing until go2rtc exists). Needs
# hassio_api: true in config.yaml.
# ---------------------------------------------------------------------------
def _supervisor(method: str, path: str, body: dict | None = None, timeout: int = 60):
    """Call the Supervisor API (http://supervisor/...) with the add-on token.
    Returns parsed JSON, or None on any failure. Never raises."""
    token = os.getenv("SUPERVISOR_TOKEN")
    if not token:
        return None
    try:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://supervisor{path}", data=data, method=method,
            headers={"Authorization": f"Bearer {token}",
                     "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode() or "{}")
    except Exception as e:
        print(f"[box] supervisor {method} {path} failed ({e})")
        return None


def _go2rtc_reachable() -> bool:
    """True if the local go2rtc answers its API."""
    try:
        urllib.request.urlopen(f"{GO2RTC_URL}/api/config", timeout=3)
        return True
    except Exception:
        return False


# go2rtc binary BUNDLED into this image by the Dockerfile (preferred source). A
# fresh HA store has NO go2rtc add-on to install, so relying on a separate add-on
# leaves the box with zero cameras — we ship our own and run it ourselves.
_BUNDLED_GO2RTC = "/usr/local/bin/go2rtc"
_GO2RTC_CONFIG = "/data/go2rtc.yaml"   # persisted add-on data dir; survives restarts
_go2rtc_proc = None


def _spawn_bundled_go2rtc() -> bool:
    """Launch the go2rtc binary bundled in THIS image on :1984. Returns True once it
    answers its API. No-op (False) if the binary isn't present (e.g. standalone dev)."""
    global _go2rtc_proc
    if not os.path.exists(_BUNDLED_GO2RTC):
        return False
    if _go2rtc_proc and _go2rtc_proc.poll() is None:
        return _go2rtc_reachable()   # already running
    # Minimal config so go2rtc has stable API + WebRTC listeners. The agent writes
    # camera streams into THIS SAME file via POST /api/config, so they persist here.
    if not os.path.exists(_GO2RTC_CONFIG):
        try:
            os.makedirs(os.path.dirname(_GO2RTC_CONFIG), exist_ok=True)
            with open(_GO2RTC_CONFIG, "w") as f:
                f.write('api:\n  listen: ":1984"\nwebrtc:\n  listen: ":8555"\n')
            os.chmod(_GO2RTC_CONFIG, 0o600)  # holds RTSP URLs with embedded NVR creds
        except Exception as e:
            print(f"[box] could not write {_GO2RTC_CONFIG}: {e} (go2rtc will use defaults)")
    try:
        # Inherit stdout/stderr so go2rtc's logs land in the HA add-on Log tab too.
        _go2rtc_proc = subprocess.Popen([_BUNDLED_GO2RTC, "-config", _GO2RTC_CONFIG])
    except Exception as e:
        print(f"[box] failed to launch bundled go2rtc: {e}")
        return False
    print(f"[box] launched bundled go2rtc (pid {_go2rtc_proc.pid}) on :1984")
    for _ in range(15):
        if _go2rtc_reachable():
            print("[box] bundled go2rtc is up")
            return True
        time.sleep(2)
    print("[box] bundled go2rtc did not answer on :1984 in time — continuing")
    return False


def _find_go2rtc_slug(listing_key: str) -> str | None:
    """Find a go2rtc add-on slug in a Supervisor listing ('addons' = installed,
    'store' = available). Matches by slug or name containing 'go2rtc'."""
    data = _supervisor("GET", f"/{listing_key}")
    if not data:
        return None
    items = (data.get("data", {}) or {}).get("addons", []) or []
    for a in items:
        slug = (a.get("slug") or "").lower()
        name = (a.get("name") or "").lower()
        if "go2rtc" in slug or "go2rtc" in name:
            return a.get("slug")
    return None


def ensure_go2rtc() -> None:
    """Make sure a local go2rtc is running and reachable.

    Flow (all best-effort, never fatal):
      0. Already reachable?            -> done.
      1. go2rtc bundled in this image? -> run it ourselves on :1984 (the normal case;
                                          a fresh HA store has NO go2rtc add-on).
      2. Installed add-on present?     -> start it, wait for reachable.
      3. In the store?                 -> install, start, wait.
      4. None of the above?            -> log clearly; the box serves zero cameras
                                          until a go2rtc exists.

    Bundling (step 1) is why the customer never has to install go2rtc separately;
    steps 2-3 stay as a fallback for boxes that already run a go2rtc add-on."""
    if _go2rtc_reachable():
        return
    # Preferred: the go2rtc we ship inside this image. No store/add-on dependency.
    if _spawn_bundled_go2rtc():
        return
    if not os.getenv("SUPERVISOR_TOKEN"):
        print("[box] go2rtc not reachable, no bundled binary, and no Supervisor token "
              "(standalone?); start go2rtc manually")
        return

    print("[box] go2rtc not reachable — attempting auto-install/start")
    slug = _find_go2rtc_slug("addons")     # already installed?
    if not slug:
        slug = _find_go2rtc_slug("store")  # available to install?
        if slug:
            print(f"[box] installing go2rtc add-on '{slug}' (this can take a minute)")
            _supervisor("POST", f"/addons/{slug}/install", {}, timeout=300)
    if not slug:
        print("[box] could not find a go2rtc add-on in the store — please install "
              "go2rtc manually; the box will report 0 cameras until then")
        return

    print(f"[box] starting go2rtc add-on '{slug}'")
    _supervisor("POST", f"/addons/{slug}/start", {})
    # Wait up to ~45s for the API to answer.
    for _ in range(15):
        if _go2rtc_reachable():
            print("[box] go2rtc is up")
            return
        time.sleep(3)
    print("[box] go2rtc did not come up in time — continuing; will retry on next boot")


# ---------------------------------------------------------------------------
# Discovery + registration
# ---------------------------------------------------------------------------
def discover_cameras() -> list:
    """Ask the LOCAL go2rtc which streams it has — each stream is one camera.

    This is the heart of "auto-discovery": instead of a hand-maintained camera
    list per box, we read go2rtc's /api/streams and treat every stream name as a
    camera. By convention here the go2rtc stream name doubles as the camera_id,
    the human name, and the src (the token we hand back to go2rtc to start a
    stream). Because the list comes from go2rtc, dropping this agent onto any box
    "just works" — whatever that go2rtc has configured becomes the box's cameras.

    Raises on network/parse errors so the caller (run) can retry — go2rtc may
    still be starting up at boot.
    """
    with urllib.request.urlopen(f"{GO2RTC_URL}/api/streams", timeout=10) as r:
        streams = json.load(r)
    cams = [{"camera_id": name, "name": name, "src": name} for name in streams.keys()]
    # Optional split: serve only an explicit subset (INCLUDE) or all-but-some
    # (EXCLUDE), so several boxes can share one go2rtc with disjoint camera sets.
    if CAMERA_INCLUDE:
        cams = [c for c in cams if c["camera_id"] in CAMERA_INCLUDE]
    elif CAMERA_EXCLUDE:
        cams = [c for c in cams if c["camera_id"] not in CAMERA_EXCLUDE]
    return cams


def _build_src(cam: dict):
    """(b) Build a go2rtc source string from one camera-config entry.

    If the entry has an explicit `src` we use it verbatim (advanced / full control).
    Otherwise we assemble one from parts: type (onvif|rtsp), host, optional port,
    optional username/password (URL-encoded), optional path. Returns None if there's
    not enough to build a source. Credentials get embedded in the src that go2rtc
    uses locally — we never LOG the src (see apply_camera_config)."""
    if cam.get("src"):
        return str(cam["src"])
    host = cam.get("host")
    if not host:
        return None
    ctype = (cam.get("type") or "rtsp").lower()
    user = cam.get("username") or cam.get("user") or ""
    pw = cam.get("password") or cam.get("pass") or ""
    cred = ""
    if user:
        cred = f"{urllib.parse.quote(str(user), safe='')}:{urllib.parse.quote(str(pw), safe='')}@"
    port = cam.get("port")
    if ctype == "onvif":
        hostport = f"{host}:{port}" if port else str(host)
        return f"onvif://{cred}{hostport}"
    # default: rtsp
    hostport = f"{host}:{port}" if port else f"{host}:554"
    path = cam.get("path") or ""
    if path and not str(path).startswith("/"):
        path = "/" + str(path)
    return f"rtsp://{cred}{hostport}{path}"


def _go2rtc_apply_streams(streams: dict) -> int:
    """Replace the streams: section of go2rtc's YAML config in one shot.

    `streams` is {name: src_url}. Returns the number applied.

    WHY config API and not /api/streams PUT: go2rtc 1.9.x changed the PUT
    semantics to require a YAML body (and the new shape isn't stable across
    minor versions — we saw `yaml: line 1: did not find expected key` on
    1.9.14). The /api/config GET/POST surface is stable across versions, so
    we GET the current YAML, splice in the new streams block, POST it back,
    then POST /api/restart so go2rtc reloads the file into memory.

    NEVER logs the credential-bearing src URLs."""
    try:
        with urllib.request.urlopen(f"{GO2RTC_URL}/api/config", timeout=5) as r:
            cfg_yaml = r.read().decode()
    except Exception as e:
        print(f"[box] could not GET go2rtc config ({e})")
        return 0
    # Guarantee a trailing newline so the streams-block regex (which requires
    # `streams:...\n`) matches even when `streams:` is the last line with no EOL.
    if cfg_yaml and not cfg_yaml.endswith("\n"):
        cfg_yaml += "\n"

    # Build the new streams: block.
    if not streams:
        new_block = "streams: {}\n"
    else:
        lines = ["streams:"]
        for name, src in streams.items():
            # Quote the URL so YAML parses it as a scalar (URLs contain ':' which
            # YAML interprets as a key/value separator otherwise). Backslash and
            # double-quote escaped for completeness — URLs rarely have either.
            safe = src.replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f"  {name}:")
            lines.append(f'    - "{safe}"')
        new_block = "\n".join(lines) + "\n"

    # Same anchored-streams regex as the wipe path — handles `streams:` followed
    # by either multi-line children OR the inline empty-mapping form `{}`.
    # Without the [^\n]* the second pass would never match the wipe's leftover
    # `streams: {}` and we'd APPEND a second `streams:` block — HTTP 500.
    pat = re.compile(r"^streams:[^\n]*\n(?:[ \t]+[^\n]*\n)*", flags=re.MULTILINE)
    if pat.search(cfg_yaml):
        new_yaml = pat.sub(new_block, cfg_yaml, count=1)
    else:
        # No streams: section yet — append.
        new_yaml = cfg_yaml.rstrip() + "\n" + new_block

    try:
        req = urllib.request.Request(
            f"{GO2RTC_URL}/api/config",
            data=new_yaml.encode(),
            method="POST",
        )
        req.add_header("Content-Type", "application/yaml")
        urllib.request.urlopen(req, timeout=10)
        print(f"[box] wrote {len(streams)} stream(s) to go2rtc config")
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")[:300].replace("\n", " | ")
        except Exception:
            body = "<could not read body>"
        print(f"[box] could not POST go2rtc config (HTTP {e.code}: {body})")
        return 0
    except Exception as e:
        print(f"[box] could not POST go2rtc config ({e})")
        return 0
    # Trigger reload — same dance as the wipe path. Restart closes the
    # response mid-flight; we expect that and don't treat it as failure.
    try:
        req = urllib.request.Request(f"{GO2RTC_URL}/api/restart", method="POST")
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass
    return len(streams)


def _go2rtc_put_stream(name: str, src: str) -> bool:
    """LEGACY single-stream PUT — kept for `apply_camera_config()` which still
    iterates the manual CAMERAS_CFG list one at a time. The ONVIF path uses
    `_go2rtc_apply_streams` (one-shot config-rewrite) instead. On go2rtc 1.9+
    this PUT path will likely 400; the manual-cameras feature isn't used by
    sir's deployment, so we leave it as a best-effort no-op until someone needs
    it."""
    try:
        q = urllib.parse.urlencode({"name": name, "src": src})
        req = urllib.request.Request(f"{GO2RTC_URL}/api/streams?{q}", method="PUT")
        urllib.request.urlopen(req, timeout=10)
        return True
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")[:200].replace("\n", " | ")
        except Exception:
            body = "<could not read body>"
        print(f"[box] could not configure stream '{name}' (HTTP {e.code}: {body})")
        return False
    except Exception as e:
        print(f"[box] could not configure stream '{name}' ({e})")
        return False


def _go2rtc_list_streams() -> list:
    """GET /api/streams — returns the list of stream names this go2rtc currently has."""
    try:
        with urllib.request.urlopen(f"{GO2RTC_URL}/api/streams", timeout=5) as r:
            data = json.loads(r.read().decode())
        # go2rtc returns either a list or a dict keyed by stream name depending on version.
        if isinstance(data, dict):
            return list(data.keys())
        if isinstance(data, list):
            return [s.get("name") for s in data if isinstance(s, dict) and s.get("name")]
        return []
    except Exception as e:
        print(f"[box] could not list go2rtc streams ({e})")
        return []


def _go2rtc_delete_stream(name: str) -> bool:
    """DELETE /api/streams?name=<name> — remove one stream from the local go2rtc."""
    try:
        q = urllib.parse.urlencode({"name": name})
        req = urllib.request.Request(f"{GO2RTC_URL}/api/streams?{q}", method="DELETE")
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"[box] could not delete stream '{name}' ({e})")
        return False


def _go2rtc_persist_empty_streams() -> bool:
    """Make the stream deletion stick to go2rtc's YAML AND its running state.

    Three-step dance, because go2rtc's REST API leaves the in-memory state
    inconsistent if you only do one or two of these:

      (1) GET /api/config           — read current YAML
      (2) regex-strip `streams:`    — emit a new YAML with `streams: {}`
      (3) POST /api/config          — persists the new YAML to disk
                                       but does NOT reload in-memory state
      (4) POST /api/restart         — actually reloads from the new YAML

    Without (4), `/api/streams` keeps returning the old stream names from
    cache, AND subsequent `PUT /api/streams?...` calls get HTTP 400 because
    go2rtc's internal state is half-baked. The restart is the only way to
    force a clean reload on the versions we've tested.

    The restart kills any in-flight viewer streams — fine for unpair (no one
    should be watching anyway). Returns True on success.
    """
    try:
        with urllib.request.urlopen(f"{GO2RTC_URL}/api/config", timeout=5) as r:
            cfg_yaml = r.read().decode()
    except Exception as e:
        print(f"[box] could not GET go2rtc config ({e})")
        return False
    # Guarantee a trailing newline so the regex below matches a streams block
    # that sits at EOF with no trailing EOL.
    if cfg_yaml and not cfg_yaml.endswith("\n"):
        cfg_yaml += "\n"
    # Replace any 'streams:' block (column-0 key + indented children) with an
    # empty mapping. Regex-based instead of pulling in PyYAML — the streams
    # block is structurally trivial.
    # Match the streams: line plus any indented children. The `[^\n]*` after
    # `streams:` is critical — it catches the inline `streams: {}` form (which
    # is what we leave behind after a previous wipe). Earlier regex required
    # `\s*\n` which only matched the multi-line form, so a second pass would
    # APPEND a new streams: section instead of replacing — go2rtc then sees
    # two `streams:` keys and rejects the whole config with HTTP 500.
    pattern = re.compile(r"^streams:[^\n]*\n(?:[ \t]+[^\n]*\n)*", flags=re.MULTILINE)
    new_yaml = pattern.sub("streams: {}\n", cfg_yaml, count=1)
    if new_yaml == cfg_yaml and "streams:" not in cfg_yaml:
        return True  # nothing to do
    try:
        req = urllib.request.Request(
            f"{GO2RTC_URL}/api/config",
            data=new_yaml.encode(),
            method="POST",
        )
        req.add_header("Content-Type", "application/yaml")
        urllib.request.urlopen(req, timeout=10)
        print("[box] persisted empty streams section to go2rtc YAML")
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")[:300].replace("\n", " | ")
        except Exception:
            body = "<could not read body>"
        print(f"[box] could not POST go2rtc config (HTTP {e.code}: {body})")
        return False
    except Exception as e:
        print(f"[box] could not POST go2rtc config ({e})")
        return False
    # Force a reload — otherwise the old streams stay cached in memory and
    # subsequent PUTs fail with HTTP 400 until go2rtc is restarted some other
    # way. POST /api/restart is fire-and-forget; go2rtc kills itself and the
    # add-on's process supervisor brings it back within 3-5 seconds.
    try:
        req = urllib.request.Request(f"{GO2RTC_URL}/api/restart", method="POST")
        urllib.request.urlopen(req, timeout=5)
        print("[box] requested go2rtc restart to apply empty streams")
    except Exception as e:
        # Restart endpoint may close the connection mid-response (because
        # go2rtc is killing itself) which raises here — that's actually a
        # success indicator. Only log + ignore.
        print(f"[box] go2rtc restart issued (response close expected: {e})")
    return True


def _wipe_camera_state() -> None:
    """Tear down EVERY trace of camera config on this box:
       1. Drop the persisted ONVIF creds file (so next boot starts fresh).
       2. Remove every stream from go2rtc.
       3. Clear the agent's in-memory id->src allowlist.
    Used by the "unclaimed" WS handler so re-pairing into a different home
    starts from zero (no stale creds, no stale streams, no SSRF leak)."""
    # (1) creds file + cached home_code
    for path, label in ((_CAMERA_CREDS_FILE, "camera creds"), (_HOME_CODE_FILE, "home_code")):
        try:
            if os.path.exists(path):
                os.remove(path)
                print(f"[box] removed persisted {label} at {path}")
        except Exception as e:
            print(f"[box] could not remove {label} file ({e})")
    # (2) go2rtc streams — DELETE each by name (clears in-memory state)
    names = _go2rtc_list_streams()
    for name in names:
        if _go2rtc_delete_stream(name):
            print(f"[box] removed go2rtc stream '{name}'")
    # (2b) Persist the empty streams section to go2rtc's YAML on disk.
    # Without this, in-memory deletes don't survive a go2rtc restart and
    # the old streams come back. POST /api/config writes through to the file.
    _go2rtc_persist_empty_streams()
    # (3) in-memory allowlist — register() below repopulates it with whatever
    # the (now empty) go2rtc reports.
    CAMERA_SRC.clear()


def apply_camera_config() -> int:
    """(b) Write any MANUALLY configured camera/NVR sources into the LOCAL go2rtc, so
    onboarding is "enter your NVR login" instead of hand-editing go2rtc YAML.

    Idempotent — safe every boot (go2rtc treats a repeated name as an update), and
    because go2rtc may not persist API-added streams across its own restart, we simply
    re-apply on every agent boot (same spirit as re-registering every boot). Returns
    the count applied. NEVER logs credentials — only the stream name and type."""
    if not CAMERAS_CFG:
        return 0
    applied = 0
    for cam in CAMERAS_CFG:
        name = cam.get("name")
        src = _build_src(cam)
        if not name or not src:
            print(f"[box] skipping camera entry (need 'name' + 'host' or 'src'): "
                  f"name={cam.get('name')!r}")
            continue
        if _go2rtc_put_stream(name, src):
            applied += 1
            print(f"[box] configured go2rtc stream '{name}' (type={cam.get('type') or 'rtsp'})")
    return applied


def _rtsp_with_creds(uri: str, user: str, pw: str) -> str:
    """Embed credentials into an rtsp:// URL. ONVIF GetStreamUri usually returns the URL
    WITHOUT them, but go2rtc needs them inline. Leaves an already-credentialed URL
    alone; URL-encodes user/pass. Returns the URL unchanged on any parse error."""
    try:
        p = urllib.parse.urlsplit(uri)
        if "@" in p.netloc or not user:
            return uri  # already credentialed, or nothing to add
        cred = f"{urllib.parse.quote(user, safe='')}:{urllib.parse.quote(pw, safe='')}@"
        return urllib.parse.urlunsplit((p.scheme, cred + p.netloc, p.path, p.query, p.fragment))
    except Exception:
        return uri


def onvif_discover(user: str, pw: str, timeout: int = 5) -> tuple:
    """(b+) WS-Discover ONVIF cameras on the LAN and resolve each one's MAIN-profile
    RTSP URL with the shared credentials.

    Returns (cameras, auth_failed_hosts):
      * cameras:           [{'host':.., 'port':.., 'token':.., 'src':rtsp_url}] sorted by host
      * auth_failed_hosts: [host_ip, ...] for any host that REJECTED our credentials
                           (vs hosts that didn't respond, were offline, etc.)

    The auth_failed list lets _handle_configure distinguish "wrong creds" from
    "no cameras here". Wrong creds → strict reject, don't save anything. Any
    other failure (offline, slow, bad SOAP) is treated as a non-credential
    issue and discovery continues.

    OPTIONAL + best-effort: needs the wsdiscovery + onvif-zeep libs; returns
    ([], []) if they're missing. Never raises; never logs the password. Only
    the FIRST media profile is used (one main stream per camera)."""
    try:
        import requests
        import urllib3
        from wsdiscovery import QName
        from wsdiscovery.discovery import ThreadedWSDiscovery
        from zeep.transports import Transport
        from onvif import ONVIFCamera
        urllib3.disable_warnings()  # cameras use self-signed certs
    except Exception as e:
        print(f"[box] ONVIF libraries not installed ({e}); skipping auto-discovery")
        return [], []

    # 1) WS-Discovery: probe the LAN for ONVIF "NetworkVideoTransmitter" services.
    hosts: dict = {}
    try:
        wsd = ThreadedWSDiscovery()
        wsd.start()
        nvt = QName("http://www.onvif.org/ver10/network/wsdl", "NetworkVideoTransmitter")
        for svc in wsd.searchServices(types=[nvt], timeout=timeout):
            for xaddr in svc.getXAddrs():
                parts = urllib.parse.urlsplit(xaddr)
                if parts.hostname:
                    hosts.setdefault(parts.hostname, parts.port or 80)  # de-dupe by host
        wsd.stop()
    except Exception as e:
        print(f"[box] ONVIF discovery probe failed ({e})")
        return [], []

    if not hosts:
        print("[box] ONVIF: no cameras answered the discovery probe")
        return [], []
    print(f"[box] ONVIF: discovered {len(hosts)} device(s); resolving main streams...")

    # Transport that (a) skips self-signed cert verification and (b) forces ONVIF
    # service calls to HTTP — many cameras/NVRs misadvertise an HTTPS service address
    # that doesn't actually serve (validated against a real Dahua/CP Plus NVR).
    class _HttpForce(Transport):
        def post_xml(self, address, envelope, headers):
            if address.startswith("https://"):
                address = "http://" + address[len("https://"):]
                address = address.replace(":443/", "/").replace(":443", "")
            return super().post_xml(address, envelope, headers)

    # 2) For each device, enumerate profiles and pick ONE MAIN stream per physical
    #    channel (an NVR exposes N channels x several sub-streams). We group profiles
    #    by VideoSource and take the highest-resolution one per channel = the main
    #    stream. adjust_time handles camera/box clock skew (else ONVIF auth fails).
    results = []
    auth_failed: list = []
    for host in sorted(hosts):
        try:
            sess = requests.Session()
            sess.verify = False
            cam = ONVIFCamera(host, hosts[host], user, pw, adjust_time=True,
                              transport=_HttpForce(session=sess))
            media = cam.create_media_service()
            best: dict = {}   # source_token -> (resolution_area, profile)
            for i, p in enumerate(media.GetProfiles()):
                try:
                    src_tok = p.VideoSourceConfiguration.SourceToken
                except Exception:
                    src_tok = f"_{i}"            # fall back to per-profile if no source
                area = 0
                try:
                    r = p.VideoEncoderConfiguration.Resolution
                    area = int(r.Width) * int(r.Height)
                except Exception:
                    pass
                if src_tok not in best or area > best[src_tok][0]:
                    best[src_tok] = (area, p)
            for src_tok in sorted(best):
                p = best[src_tok][1]
                try:
                    setup = {"StreamSetup": {"Stream": "RTP-Unicast",
                                             "Transport": {"Protocol": "RTSP"}},
                             "ProfileToken": p.token}
                    uri = media.GetStreamUri(setup).Uri
                    results.append({"host": host, "port": hosts[host], "token": p.token,
                                    "src": _rtsp_with_creds(uri, user, pw)})
                except Exception as e:
                    # Print only the exception TYPE, not str(e): a GetStreamUri
                    # error can echo back the request URL, which carries the
                    # camera credentials. Keep creds out of the log.
                    print(f"[box] ONVIF {host} ch {src_tok}: GetStreamUri failed ({type(e).__name__})")
            print(f"[box] ONVIF {host}: {len(best)} camera(s) (main stream each)")
        except Exception as e:
            err = str(e)
            # ONVIF "Sender not Authorized" / "Authentication failed" are how
            # the camera says "wrong username/password". Anything else (timeout,
            # bad XML, network reset) we treat as a transient/unrelated issue.
            if ("Not Authorized" in err
                    or "Authentication failed" in err
                    or "Invalid username or password" in err):
                auth_failed.append(host)
                print(f"[box] ONVIF {host}: auth failed ({err}); skipping")
            else:
                print(f"[box] ONVIF {host}: profile failed ({err}); skipping")
    return results, auth_failed


def _ha_notify(message: str) -> None:
    """Raise a Home Assistant persistent notification (the 🔔 in HA's sidebar).

    This is how the box owner sees the CLAIM CODE without digging through the
    add-on log. It calls Home Assistant's REST API through the Supervisor proxy
    (http://supervisor/core/api/...) authenticated with the add-on's
    SUPERVISOR_TOKEN. Works only when running as an HA add-on with
    `homeassistant_api: true`; otherwise it quietly no-ops so the agent still runs
    standalone. We use a fixed notification_id so we can dismiss it once claimed."""
    if not SUPERVISOR_TOKEN:
        return
    try:
        body = json.dumps({
            "notification_id": "aha_claim",
            "title": "AHA: add your camera box",
            "message": message,
        }).encode()
        req = urllib.request.Request(
            "http://supervisor/core/api/services/persistent_notification/create",
            data=body,
            headers={"Authorization": f"Bearer {SUPERVISOR_TOKEN}",
                     "Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
        print("[box] raised a 'claim your box' notification in Home Assistant")
    except Exception as e:
        print(f"[box] could not raise HA notification ({e}); the claim code is in the log above")


def _ha_dismiss() -> None:
    """Clear the claim notification once the box is claimed (tidy up the 🔔)."""
    if not SUPERVISOR_TOKEN:
        return
    try:
        body = json.dumps({"notification_id": "aha_claim"}).encode()
        req = urllib.request.Request(
            "http://supervisor/core/api/services/persistent_notification/dismiss",
            data=body,
            headers={"Authorization": f"Bearer {SUPERVISOR_TOKEN}",
                     "Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass


def _ha_get(path: str, base: str = "http://supervisor/core/api"):
    """(c) GET a Home Assistant Core or Supervisor API path using the add-on's
    SUPERVISOR_TOKEN. Best-effort: returns parsed JSON, or {"_error": ...} on failure,
    or None when there's no token (standalone). Never raises."""
    if not SUPERVISOR_TOKEN:
        return None
    try:
        req = urllib.request.Request(
            f"{base}{path}",
            headers={"Authorization": f"Bearer {SUPERVISOR_TOKEN}"},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"_error": str(e)}


def gather_telemetry() -> dict:
    """(c) Collect as much box/HA data as we can for the cloud to relay to the
    operator: ALL Home Assistant entity states + HA config, plus Supervisor host/OS/
    add-on info, plus agent-local facts. Best-effort — any single source failing shows
    up as an _error and the rest still returns. Blocking I/O; call via to_thread.

    PRIVACY NOTE: /states is the user's whole smart home. This is gated on the box
    being an HA add-on (SUPERVISOR_TOKEN) and, on the cloud, by box ownership."""
    return {
        "device_id": DEVICE_ID,
        "agent_version": AGENT_VERSION,
        "box_name": BOX_NAME,
        "cameras": sorted(CAMERA_SRC.keys()),
        "go2rtc_url": GO2RTC_URL,
        "ha_config": _ha_get("/config"),
        "states": _ha_get("/states"),
        "supervisor": _ha_get("/supervisor/info", base="http://supervisor"),
        "host": _ha_get("/host/info", base="http://supervisor"),
        "os": _ha_get("/os/info", base="http://supervisor"),
        "addons": _ha_get("/addons", base="http://supervisor"),
    }


def _notify_device_id() -> None:
    """Surface this box's device_id so the owner can add it in the app.

    Pairing is app-driven: the app declares (device_id, home_code) to the cloud
    (POST /provision_box), so the ONE thing the owner needs from the box is its
    device_id. We print it BIG in the add-on log AND raise an HA notification. (In
    production this is also a QR sticker on the box.) Replaces the old claim code."""
    print("=" * 60)
    print("  THIS BOX IS NOT YET PAIRED.")
    print(f"  Open the AHA app and add this box by its ID:  {DEVICE_ID}")
    print("=" * 60)
    _ha_notify(f"Your camera box is ready to add. Open the AHA app and add this "
               f"box by its ID:\n\n**{DEVICE_ID}**")


def register(cameras: list) -> None:
    """Tell the cloud who this box is and which cameras it has.

    POSTs to /register_box. The cloud upserts the box row and REPLACES its
    camera list with exactly what we send, so this call is the single source of
    truth for "what cameras does box X have." Sending the full list every boot
    keeps the cloud in sync if cameras were added/removed at go2rtc.

    AUTH: the box proves identity with its per-box TOFU secret (X-AHA-Box-Secret).
    The legacy shared bootstrap key is sent ONLY if explicitly configured (it's empty
    by default — retired). Pairing is established by the APP via POST /provision_box,
    not here; this call just reports the box and, if already pre-authorized, comes
    back CLAIMED. Raises on HTTP error (caller decides whether that's fatal)."""
    # No user_id is sent: the agent never asserts ownership. The box stays UNCLAIMED
    # until the app pre-authorizes its device_id (POST /provision_box).
    payload = {
        "device_id": DEVICE_ID,
        "name": BOX_NAME,
        "cameras": cameras,
    }
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    # Legacy shared key: sent ONLY if a deployment still configures one (empty/retired
    # by default). The cloud also defaults to not requiring it.
    if REGISTER_KEY:
        headers["X-AHA-Register-Key"] = REGISTER_KEY
    # Per-box identity: the cloud binds this secret to our device_id on first sight
    # (TOFU) and thereafter requires it, so no one else can register as us.
    headers["X-AHA-Box-Secret"] = BOX_SECRET
    # BOOTSTRAP token: if the app pushed one to us (we're not yet enrolled), present it
    # so the cloud lets this first registration through and enrolls our secret. It's
    # single-use; we delete our copy once the cloud accepts it.
    token = _read_provision_token()
    if token:
        headers["X-AHA-Provision-Token"] = token
    req = urllib.request.Request(
        f"{BACKEND_URL}/register_box", data=body, headers=headers, method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        result = json.loads(resp.read().decode())
    print(f"[box] registered {len(cameras)} camera(s)")
    if token:
        # The cloud accepted (no exception) — the token is now consumed/burned, drop
        # our local copy so we never replay it; our secret is the credential now.
        _clear_provision_token()
    if result.get("claimed"):
        # We're paired: cache the home_code and clear any stale "add your box" notice.
        _persist_home_code(result.get("home_code") or "")
        print("[box] box is claimed and ready.")
        _ha_dismiss()
    else:
        # Not paired yet — show the device_id so the owner can add it in the app.
        _notify_device_id()
    # Keep the mDNS beacon's claimed flag in sync with our pairing state.
    _mdns_refresh()


def _make_onvif(host, port):
    """Build an ONVIFCamera tolerant of self-signed certs and NVRs that misadvertise an
    HTTPS service address (forces ONVIF calls to HTTP). Lazy-imports the optional libs."""
    import requests
    import urllib3
    from zeep.transports import Transport
    from onvif import ONVIFCamera
    urllib3.disable_warnings()

    class _HttpForce(Transport):
        def post_xml(self, address, envelope, headers):
            if address.startswith("https://"):
                address = "http://" + address[len("https://"):]
                address = address.replace(":443/", "/").replace(":443", "")
            return super().post_xml(address, envelope, headers)

    sess = requests.Session()
    sess.verify = False
    return ONVIFCamera(host, port, ONVIF_USER, ONVIF_PASS, adjust_time=True,
                       transport=_HttpForce(session=sess))


def onvif_ptz(cam_id: str, pan: float, tilt: float, zoom: float) -> dict:
    """Pan/tilt/zoom one camera over ONVIF. All-zero velocities = STOP (sent on button
    release). Best-effort: a fixed (non-PTZ) camera just returns an error; never raises.
    Velocities are clamped to ONVIF's -1.0..1.0 range. Blocking — call via to_thread."""
    info = CAMERA_PTZ.get(cam_id)
    if not info:
        return {"error": f"no PTZ for {cam_id}"}

    def clamp(v):
        try:
            return max(-1.0, min(1.0, float(v)))
        except Exception:
            return 0.0

    pan, tilt, zoom = clamp(pan), clamp(tilt), clamp(zoom)
    try:
        ptz = _make_onvif(info["host"], info.get("port", 80)).create_ptz_service()
        token = info["token"]
        if pan == 0 and tilt == 0 and zoom == 0:
            ptz.Stop({"ProfileToken": token, "PanTilt": True, "Zoom": True})
            return {"ok": True, "action": "stop"}
        ptz.ContinuousMove({"ProfileToken": token,
                            "Velocity": {"PanTilt": {"x": pan, "y": tilt}, "Zoom": {"x": zoom}}})
        return {"ok": True, "action": "move"}
    except Exception as e:
        return {"error": f"ptz failed for {cam_id}: {e}"}


def provision_cameras_and_register(pre_found=None, pre_auth_failed=None):
    """Configure go2rtc from the CURRENT camera creds (ONVIF auto-discovery + any manual
    `cameras` list), discover the resulting streams, refresh the SSRF allow-list, and
    (re-)register with the cloud.

    Returns (cameras_list, auth_failed_hosts) — the caller (typically
    _handle_configure) uses auth_failed_hosts to decide whether to surface
    "wrong ONVIF credentials" to the app.

    When pre_found is passed, the ONVIF discovery is SKIPPED and we use the
    caller's already-discovered list. Used by _handle_configure which has to
    probe creds before persisting them — without this, every configure would
    do ONVIF discovery TWICE (once for the credential gate, once here),
    doubling the wall-clock time of a pair.

    Blocking (urllib + retries) — call via asyncio.to_thread. Used at BOOT and again
    whenever the app pushes new creds (configure_cameras), so both paths share one
    code path. Retries discovery because go2rtc may still be starting at boot."""
    auth_failed: list = []
    if pre_found is not None:
        found = pre_found
        auth_failed = pre_auth_failed or []
    elif ONVIF_USER:
        found, auth_failed = onvif_discover(ONVIF_USER, ONVIF_PASS, ONVIF_DISCOVERY_TIMEOUT)
    else:
        found = []
    if found or pre_found is not None:
        CAMERA_PTZ.clear()
        # Collect every ONVIF-discovered stream into a name->src mapping and
        # push them ALL at once via _go2rtc_apply_streams (one POST + restart).
        # PTZ map is populated for everything we configured.
        streams: dict = {}
        for i, c in enumerate(found, start=1):
            name = f"cam{i}"
            streams[name] = c["src"]
            if c.get("token"):
                CAMERA_PTZ[name] = {"host": c["host"], "port": c.get("port", 80),
                                    "token": c["token"]}
        applied = _go2rtc_apply_streams(streams) if streams else 0
        if found:
            print(f"[box] ONVIF: found {len(found)} camera(s); "
                  f"go2rtc applied {applied}")
        if auth_failed:
            print(f"[box] ONVIF: {len(auth_failed)} host(s) rejected creds: {auth_failed}")
    applied = apply_camera_config()
    if applied:
        print(f"[box] applied {applied} manual camera source(s) to go2rtc")
    cameras = []
    for _attempt in range(10):
        try:
            cameras = discover_cameras()
            if cameras:
                break
            print("[box] go2rtc has no streams yet; retrying...")
        except Exception as e:
            print(f"[box] waiting for go2rtc ({e})...")
        time.sleep(3)
    # Rebuild the SSRF allow-list to exactly the current set (drop stale entries).
    CAMERA_SRC.clear()
    CAMERA_SRC.update({c["camera_id"]: c["src"] for c in cameras})
    register(cameras)
    return cameras, auth_failed


async def _handle_configure(payload: dict) -> dict:
    """(config-push) Apply camera credentials PUSHED by the app at claim time.

    Sir's strict + partial rules:
      * STRICT on wrong creds — if at least one host rejected our creds AND we
        found zero cameras, the creds are clearly wrong. Don't persist them,
        don't touch go2rtc, return {ok:false, error_kind:"onvif_auth_failed",
        auth_failed_hosts:[...]} so the cloud can surface a clean error.
      * PARTIAL success — if at least one camera was successfully resolved,
        proceed normally: save creds, register the cameras we got. If some
        OTHER hosts rejected creds, attach auth_failed_hosts to the reply
        so the app can warn the user but the box is still usable.

    Reply shapes:
      ok, all good:        {ok:true, cameras:N}
      ok, partial:         {ok:true, cameras:N, partial:true, auth_failed_hosts:[...]}
      strict reject:       {ok:false, error_kind:"onvif_auth_failed", auth_failed_hosts:[...]}
      missing username:    {ok:false, error_kind:"missing_username"}
      anything else broke: {ok:false, error_kind:"setup_failed", detail:"..."}

    NEVER logs the credentials.
    """
    global ONVIF_USER, ONVIF_PASS
    user = (payload.get("onvif_username") or "").strip()
    pw = payload.get("onvif_password") or ""
    if not user:
        return {"ok": False, "error_kind": "missing_username"}

    # Probe with the NEW creds first — but do NOT save them or touch go2rtc
    # until we know they worked. If they're wrong, the previous creds (and
    # the previous go2rtc state) survive untouched.
    try:
        cameras, auth_failed = await asyncio.to_thread(
            onvif_discover, user, pw, ONVIF_DISCOVERY_TIMEOUT
        )
    except Exception as e:
        return {"ok": False, "error_kind": "discovery_failed", "detail": str(e)}

    # STRICT rule: no cameras AND somebody rejected our creds → wrong creds.
    # Leave everything as it was. The cloud's relay will pass this back to the
    # app so the user can correct the password without the box being affected.
    if not cameras and auth_failed:
        print(f"[box] configure rejected: ONVIF auth failed on {auth_failed}")
        return {
            "ok": False,
            "error_kind": "onvif_auth_failed",
            "auth_failed_hosts": auth_failed,
            "cameras": 0,
        }

    # Got at least one camera (or no creds were rejected). Persist creds and
    # proceed with the normal provision path. Note this re-runs ONVIF discovery
    # inside provision_cameras_and_register; that's wasteful but keeps the boot
    # path and config-push path sharing one routine. Acceptable for now.
    ONVIF_USER, ONVIF_PASS = user, pw
    _save_persisted_camera_creds({"onvif_username": user, "onvif_password": pw})
    try:
        # Reuse the discovery we just did (pre_found/pre_auth_failed) so we
        # don't run ONVIF twice. Saves ~3-5s per pair.
        cams_final, auth_failed_final = await asyncio.to_thread(
            provision_cameras_and_register, cameras, auth_failed,
        )
    except Exception as e:
        return {"ok": False, "error_kind": "setup_failed", "detail": str(e)}

    reply = {"ok": True, "cameras": len(cams_final)}
    if auth_failed_final:
        reply["partial"] = True
        reply["auth_failed_hosts"] = auth_failed_final
    return reply


# ---------------------------------------------------------------------------
# Signaling (the WebRTC offer/answer relay)
# ---------------------------------------------------------------------------
def offer_to_go2rtc(src: str, offer_sdp: str) -> str:
    """Forward a WebRTC offer to a specific go2rtc stream, return its answer.

    `src` MUST already be a value the box discovered (see the SSRF note in
    handle_request) — this function does not validate it, it just URL-encodes it
    and POSTs the raw SDP to go2rtc's WebRTC endpoint. go2rtc replies with the
    answer SDP, which we hand straight back to the cloud -> viewer.

    This is the LAST point at which we touch the handshake. After the viewer
    applies this answer, video flows DIRECTLY viewer<->go2rtc (P2P) and never
    comes back through this process.

    Blocking I/O (urllib) on purpose — the caller runs it via asyncio.to_thread
    so the event loop keeps servicing the WebSocket.
    """
    url = f"{GO2RTC_URL}/api/webrtc?src={urllib.parse.quote(src)}"
    req = urllib.request.Request(
        url, data=offer_sdp.encode(),
        headers={"Content-Type": "application/sdp"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return resp.read().decode()


async def handle_request(payload: dict) -> dict:
    """Turn one signaling request from the cloud into a go2rtc answer.

    `payload` is whatever the viewer's app sent, relayed verbatim by the cloud:
    {type:"offer", sdp:"...", camera_id:"..."}.

    SECURITY — SSRF-safe camera_id -> src mapping (do not "simplify" this away):
    --------------------------------------------------------------------------
    We look the requested camera_id up in CAMERA_SRC, which contains ONLY the
    srcs this box itself discovered from its own go2rtc at startup. We deliberately
    forward to that looked-up src — never to client-supplied text. This means a
    malicious or buggy viewer cannot coerce the box into hitting an arbitrary
    go2rtc src / internal URL (a Server-Side Request Forgery). The box is the
    authority on what it will stream; the client only gets to PICK from the
    allow-list, never to define a target.

    Unknown camera_ids are REJECTED outright (we do NOT fall back to forwarding
    the client's raw text as a go2rtc src). This is the strictly-safer form: the
    client may only PICK a camera_id the box itself discovered, never define one.
    A legitimate viewer always sends a discovered camera_id, so this is
    behavior-preserving for real traffic while making the code match the SSRF
    guarantee stated above (defense-in-depth — even though offer_to_go2rtc only
    ever targets THIS box's go2rtc, we keep client text out of the src entirely).

    Runs the blocking go2rtc call off the event loop via asyncio.to_thread so a
    slow camera handshake can't stall the WebSocket reader.
    """
    # (claim push) The cloud tells us we've just been claimed by an app. Dismiss
    # the "claim your box" HA notification, persist the home_code locally so the
    # add-on log + future diagnostics can show which home this box belongs to,
    # and log it for visibility. No agent restart required.
    if payload.get("kind") == "claimed":
        hc = payload.get("home_code") or ""
        print(f"[box] cloud-claimed; bound to home_code={hc!r}")
        _ha_dismiss()
        _persist_home_code(hc)
        _mdns_refresh()  # flip the LAN beacon's claimed flag to 1
        return {"ok": True}
    # (unclaim push) The cloud tells us we've been unpaired and has ERASED our
    # binding + secret server-side. Wipe every trace locally: camera config (creds +
    # go2rtc streams + allowlist, via _wipe_camera_state which also removes the
    # home_code file) AND the bootstrap token. We do NOT re-register — we're
    # de-provisioned, so the cloud would reject it; the box goes mute until the app
    # PAIRS it again (a fresh token pushed to the LAN endpoint). We refresh the mDNS
    # beacon to claimed=0 and surface our device_id so the owner can re-add us.
    if payload.get("kind") == "unclaimed":
        print(f"[box] cloud-unclaimed; erasing local pairing state")
        await asyncio.to_thread(_wipe_camera_state)
        _clear_provision_token()
        _mdns_refresh()
        _notify_device_id()
        return {"ok": True}
    # (config-push) The app pushes camera creds at claim time over this same channel.
    if payload.get("kind") == "configure_cameras":
        return await _handle_configure(payload)
    # (PTZ) pan/tilt/zoom a camera over ONVIF (hold-to-move from the app; all-zero=stop).
    if payload.get("kind") == "ptz":
        return await asyncio.to_thread(
            onvif_ptz, payload.get("camera_id"),
            payload.get("pan") or 0, payload.get("tilt") or 0, payload.get("zoom") or 0)
    # (c) Telemetry PULL: the cloud can ask this box for a full HA snapshot over the
    # SAME request/reply channel used for signaling, distinguished by payload.kind.
    if payload.get("kind") == "telemetry":
        return await asyncio.to_thread(gather_telemetry)
    sdp = payload.get("sdp")
    # Accept either key; viewers send camera_id, older callers may send src.
    cam_id = payload.get("camera_id") or payload.get("src")
    # Reject any camera_id we did not discover ourselves — never forward raw
    # client text as a go2rtc src (SSRF defense-in-depth, matches the docstring).
    if cam_id not in CAMERA_SRC:
        return {"error": f"unknown camera: {cam_id}"}
    # Map the requested camera_id to the go2rtc src we discovered for it.
    src = CAMERA_SRC[cam_id]
    if payload.get("type") == "offer" and sdp and src:
        # Wrap the go2rtc call: if go2rtc errors (e.g. 500 "codecs not matched",
        # or the camera's RTSP is down), RETURN a structured error instead of
        # letting the exception bubble up and drop the WebSocket. That keeps the
        # box serving every other camera and lets the viewer see a clean failure
        # instead of the cloud timing out.
        try:
            answer = await asyncio.to_thread(offer_to_go2rtc, src, sdp)
            return {"type": "answer", "sdp": answer}
        except Exception as e:
            return {"error": f"go2rtc failed for {cam_id}: {e}"}
    # Malformed request — return a structured error the cloud/app can surface.
    return {"error": "expected an offer with {type, sdp, camera_id}"}


# ---------------------------------------------------------------------------
# Telemetry push loop (c)
# ---------------------------------------------------------------------------
async def _telemetry_loop(ws) -> None:
    """Push a full HA snapshot up the signaling socket: once on connect, then every
    AHA_TELEMETRY_INTERVAL seconds. No-op without SUPERVISOR_TOKEN (standalone). On
    any send/gather error we return; the outer reconnect loop restarts us."""
    if not SUPERVISOR_TOKEN:
        return
    while True:
        try:
            data = await asyncio.to_thread(gather_telemetry)
            await ws.send(json.dumps({"type": "telemetry", "device_id": DEVICE_ID, "data": data}))
            print("[box] pushed telemetry snapshot")
        except Exception as e:
            print(f"[box] telemetry push failed ({e})")
            return
        if TELEMETRY_INTERVAL <= 0:
            return  # one-shot: push on connect only
        await asyncio.sleep(TELEMETRY_INTERVAL)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# mDNS beacon — advertise this box on the LAN as _aha-box._tcp so the app can
# DISCOVER it directly (Android NsdManager), with NO cloud/public-IP matching.
#
# This is a pure DISCOVERY BEACON: everything the app needs (the device_id) rides
# in the TXT record, so the app never opens a TCP connection to the box — the box
# keeps zero inbound listening sockets (mDNS is link-local multicast, not a port we
# serve). The app reads `id` from TXT and then declares it to the cloud via
# POST /provision_box {device_id, home_code}. Best-effort: any failure here must
# never stop the agent (e.g. zeroconf missing in a standalone run, or 5353 busy).
#
# The advertised port is NOMINAL (nothing listens there); browsers require a port
# in the SRV record but the app ignores it and uses the TXT `id`.
# ---------------------------------------------------------------------------
MDNS_TYPE = "_aha-box._tcp.local."
MDNS_PORT = int(os.getenv("AHA_MDNS_PORT", "8099"))  # nominal; nothing serves here
_zeroconf = None
_mdns_info = None


def _primary_ip() -> str:
    """The host's primary LAN IP (the egress interface). The UDP socket is never
    sent on — connect() just makes the OS pick the outbound interface. host_network
    is on, so this is the box's real LAN address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def _mdns_build_info(ip: str):
    """Construct the ServiceInfo (instance name keyed by device_id so it's unique).
    TXT carries everything the app needs: id, name, version, claimed flag."""
    from zeroconf import ServiceInfo
    return ServiceInfo(
        MDNS_TYPE,
        f"{DEVICE_ID}.{MDNS_TYPE}",
        addresses=[socket.inet_aton(ip)],
        port=MDNS_PORT,
        properties={
            "id": DEVICE_ID,
            "name": BOX_NAME,
            "ver": AGENT_VERSION,
            "claimed": "1" if _is_claimed() else "0",
        },
        server=f"aha-{DEVICE_ID}.local.",
    )


def _start_mdns() -> None:
    """Register the mDNS beacon (best-effort). Runs zeroconf's own background
    thread; coexists with the host avahi-daemon via SO_REUSEPORT."""
    global _zeroconf, _mdns_info
    try:
        from zeroconf import Zeroconf
    except Exception as e:
        print(f"[box] mDNS disabled (zeroconf not available): {e}")
        return
    try:
        ip = _primary_ip()
        _mdns_info = _mdns_build_info(ip)
        _zeroconf = Zeroconf()
        _zeroconf.register_service(_mdns_info)
        print(f"[box] mDNS advertising {MDNS_TYPE} on {ip} "
              f"(id={DEVICE_ID}, claimed={_is_claimed()})")
    except Exception as e:
        print(f"[box] mDNS registration failed ({e}); continuing without LAN discovery")


def _mdns_refresh() -> None:
    """Re-publish the TXT record (e.g. after claimed/unclaimed changes) so the app's
    discovery reflects the box's current pairing state. Best-effort."""
    global _mdns_info
    if _zeroconf is None:
        return
    try:
        _mdns_info = _mdns_build_info(_primary_ip())
        _zeroconf.update_service(_mdns_info)
    except Exception as e:
        print(f"[box] mDNS refresh failed ({e}); continuing")


# ---------------------------------------------------------------------------
# LAN provisioning endpoint.
#
# The app, on the same Wi-Fi, POSTs the pairing material here:
#   {home_code, token, onvif_username?, onvif_password?}
# We persist the bootstrap token (presented on the next /register_box), cache the
# home_code, apply the camera creds, and (re-)register — synchronously, so the HTTP
# reply means "provisioned + registered." host_network is on, so binding
# 0.0.0.0:<PROVISION_PORT> is reachable from the app. The token reaches us here, out
# of band from the cloud, which is what stops a fake box from bootstrapping.
# Best-effort: a failure starting/serving this must never stop the agent.
# ---------------------------------------------------------------------------
def _provision_status() -> dict:
    """The public identity the app needs to pair this box."""
    return {
        "device_id": DEVICE_ID,
        "name": BOX_NAME,
        "agent_version": AGENT_VERSION,
        "claimed": _is_claimed(),
    }


def _apply_provision(home_code: str, token: str, user: str, pw: str) -> dict:
    """Persist the pushed pairing material and (re-)register. Blocking — the HTTP
    handler runs it inline so its reply means the box is registered. The token we
    just wrote is presented by register() and burned on success."""
    global ONVIF_USER, ONVIF_PASS
    _write_provision_token(token)
    _persist_home_code(home_code)
    if user:
        ONVIF_USER, ONVIF_PASS = user, pw
        _save_persisted_camera_creds({"onvif_username": user, "onvif_password": pw})
    try:
        cams, _auth = provision_cameras_and_register()
    except Exception as e:
        return {"ok": False, "error": str(e)}
    _mdns_refresh()
    return {"ok": True, "device_id": DEVICE_ID, "cameras": len(cams), "claimed": _is_claimed()}


class _ProvisionHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print("[box][provision-http] " + (fmt % args))

    def _send(self, code: int, obj: dict) -> None:
        payload = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path.rstrip("/") in ("", "/provision", "/device_id"):
            self._send(200, _provision_status())
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        if self.path.rstrip("/") != "/provision":
            self._send(404, {"ok": False, "error": "not found"})
            return
        # SECURITY: only an UNPAIRED box accepts a LAN provision push. This endpoint is
        # unauthenticated (same-LAN reach is treated as intent), so once the box is
        # paired we refuse to let any other device on the Wi-Fi re-point it — overwrite
        # its home_code/token/ONVIF creds or wipe/replace its go2rtc streams. Re-pairing
        # must first go through the app's unpair, which clears local state (home_code
        # file removed) and flips _is_claimed() back to false.
        if _is_claimed():
            self._send(403, {"ok": False, "error": "already paired; unpair first"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            self._send(400, {"ok": False, "error": "bad length"})
            return
        if length <= 0 or length > _MAX_PROVISION_BODY:
            self._send(400, {"ok": False, "error": "missing or oversize body"})
            return
        try:
            data = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError):
            self._send(400, {"ok": False, "error": "bad json"})
            return
        token = (data.get("token") or "").strip()
        home_code = (data.get("home_code") or "").strip()
        if not token or not home_code:
            self._send(400, {"ok": False, "error": "home_code and token required"})
            return
        # Length caps mirror the cloud's (ProvisionBoxRequest: token 16..256,
        # home_code <=128) so a LAN caller can't persist absurd values on the box.
        if not (16 <= len(token) <= 256) or len(home_code) > 128:
            self._send(400, {"ok": False, "error": "field length out of range"})
            return
        user = (data.get("onvif_username") or "").strip()
        pw = data.get("onvif_password") or ""
        result = _apply_provision(home_code, token, user, pw)
        self._send(200 if result.get("ok") else 500, result)


def _start_provision_server() -> None:
    """Start the LAN provisioning HTTP server in a daemon thread (best-effort)."""
    if PROVISION_PORT <= 0:
        print("[box] LAN provisioning endpoint disabled (AHA_PROVISION_PORT=0)")
        return
    try:
        srv = http.server.ThreadingHTTPServer(("0.0.0.0", PROVISION_PORT), _ProvisionHandler)
    except OSError as e:
        print(f"[box] could NOT start LAN provisioning endpoint on :{PROVISION_PORT} ({e})")
        return
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"[box] LAN provisioning endpoint on http://0.0.0.0:{PROVISION_PORT}/provision")


def _ws_connect(url: str):
    """Open the signaling WebSocket, presenting our per-box secret on the handshake.
    The cloud's WS auth checks it (under enforcement) and rejects an unpaired/fake box.
    websockets renamed the header kwarg extra_headers -> additional_headers in v14, so
    we try both for compatibility."""
    hdrs = {"X-AHA-Box-Secret": BOX_SECRET}
    try:
        return websockets.connect(url, additional_headers=hdrs)
    except TypeError:
        return websockets.connect(url, extra_headers=hdrs)


async def run() -> None:
    """Boot sequence + the forever signaling loop.

    Phase 1 — DISCOVER (robust, retrying):
        At boot go2rtc may not be up yet, so we retry discovery up to 10 times
        (3s apart). We retry on BOTH an exception (go2rtc unreachable) AND an
        empty result (go2rtc up but no streams configured yet). After the loop
        we record whatever we found into CAMERA_SRC (the SSRF allow-list).

    Phase 2 — REGISTER:
        Push the discovered camera list to the cloud registry.

    Phase 3 — SIGNAL (forever):
        Open the outbound WebSocket and hold it. For every relayed "request"
        from the cloud, produce an answer and send a matching "reply" tagged
        with the same `id` (the cloud matches request/reply by id via a Future).
        ANY failure (drop, network blip, cloud restart) is caught and we
        reconnect after 3s — the box must self-heal without supervision.
    """
    # --- Phase 0: LAN beacon + provisioning endpoint + go2rtc bootstrap ---
    # Advertise this box over mDNS so the app can find it on the LAN (no cloud/
    # public-IP matching). Best-effort; runs in zeroconf's own thread.
    _start_mdns()
    # Bring up the LAN provisioning endpoint so the app can push {home_code, token}
    # and bootstrap this box even before it has registered with the cloud.
    _start_provision_server()
    # Make sure go2rtc exists (auto-install/start if needed) so the customer never
    # has to install it separately — the AHA add-on bootstraps it. Best-effort; off
    # the event loop because it does blocking Supervisor calls + sleeps.
    await asyncio.to_thread(ensure_go2rtc)

    # --- Phases 1-2: configure go2rtc from current creds, discover, register ---
    # Extracted into provision_cameras_and_register() so the app's configure_cameras
    # push can re-run the exact same path at runtime. Off the event loop (blocking).
    # We ignore auth_failed on boot — boot path is "use whatever creds are persisted
    # locally", not "validate fresh creds".
    #
    # Boot registration is BEST-EFFORT: it can legitimately fail when the cloud is
    # ENFORCING provisioning (AHA_PROVISION_ENFORCED) and the app hasn't pre-authorized
    # this box yet, or when the cloud is briefly unreachable. That must NOT crash the
    # agent — we surface our device_id (so the owner can add it in the app) and keep
    # the WS loop reconnecting; the box becomes usable the moment it's provisioned and
    # re-registers (next reconnect / reboot, or a configure push).
    try:
        cameras, _auth_failed = await asyncio.to_thread(provision_cameras_and_register)
    except Exception as e:
        print(f"[box] initial registration failed ({e}); "
              f"the box may not be provisioned yet — add it in the app by its ID")
        await asyncio.to_thread(_notify_device_id)
        cameras = []

    # --- Phase 3: hold the signaling WebSocket open, reconnect forever ---
    url = ws_url()
    while True:  # reconnect forever — the box should never stay offline silently
        try:
            print(f"[box] connecting to {url}")
            async with _ws_connect(url) as ws:
                print(f"[box] connected — serving {len(cameras)} camera(s) — waiting for requests")
                # (c) Push telemetry on the same socket (initial + periodic).
                tele_task = asyncio.create_task(_telemetry_loop(ws))
                try:
                    # Read frames until the socket closes; each frame is one JSON message.
                    async for raw in ws:
                        # Per-message guard: one bad frame or handler error must NOT
                        # drop the whole socket (that would knock out every camera).
                        try:
                            msg = json.loads(raw)
                            if msg.get("type") == "request":
                                # Produce the answer (go2rtc offer OR telemetry pull)...
                                result = await handle_request(msg.get("payload") or {})
                                # ...and reply with the SAME id so the cloud matches it.
                                # Use .get("id") (not msg["id"]): a malformed or
                                # one-way message without an id must not raise a
                                # KeyError here (that produced the noisy
                                # "error handling message ('id')" lines). If there's
                                # no id, there's no awaiting caller to reply to.
                                msg_id = msg.get("id")
                                if msg_id is not None:
                                    await ws.send(json.dumps({
                                        "id": msg_id, "type": "reply", "result": result,
                                    }))
                        except Exception as e:
                            print(f"[box] error handling message ({e}); continuing")
                finally:
                    tele_task.cancel()
        except Exception as e:
            # Any error (connect failure, dropped socket, bad frame) -> back off
            # and reconnect. This is the agent's self-healing guarantee.
            print(f"[box] connection lost ({e}); retrying in 3s")
            await asyncio.sleep(3)


if __name__ == "__main__":
    # Single entry point: run the whole agent under one asyncio event loop.
    asyncio.run(run())
