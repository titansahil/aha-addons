# AHA Add-ons

Home Assistant add-on repository for the **AHA Agent** — the on-box bridge that
registers a Home Assistant box with the AHA cloud and relays WebRTC signaling so
live camera video flows **peer-to-peer** (phone ↔ box), never through the cloud.

## Add this repository to Home Assistant

Settings → Add-ons → Add-on Store → ⋮ (top right) → **Repositories**, paste:

```
https://github.com/titansahil/aha-addons
```

Then install **AHA Agent** from the store. After install, set the add-on's
**`backend_url`** option to your AHA cloud URL and start it. (The
`tools/provision-box.sh` script in the backend repo automates onboarding +
adding this repo + install + setting `backend_url` + pairing.)

## What's here

| Path | What it is |
|---|---|
| `repository.yaml` | HA add-on repository manifest |
| `aha-agent/` | the add-on: `config.yaml`, `Dockerfile`, `box_client.py` |
| `build-and-push.sh` | (maintainers) build a prebuilt multi-arch image |

## Configuration (add-on options)

| Option | Required | Notes |
|---|---|---|
| `backend_url` | **yes** | AHA cloud URL, e.g. `https://cloud.example.com`. No default is baked in. |
| `register_key` | no | Legacy shared key — leave empty (retired). |
| `box_name` | no | Friendly label shown in the app. |
| camera options | no | Optional ONVIF/RTSP auto-config; the app can also push creds at pairing. |

The box's `device_id` is auto-derived from its NIC MAC. Pairing is done by the
**app**, which declares `(device_id, home_code)` to the cloud — the box self-claims.
The agent surfaces its `device_id` in a Home Assistant notification / its log.
