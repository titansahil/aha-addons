# =============================================================================
# AHA Agent — container image for the Home Assistant add-on
# =============================================================================
# The agent is mostly stdlib (asyncio, json, urllib). Third-party deps:
#   * websockets   — the outbound signaling socket.
#   * zeroconf     — advertise the box on the LAN over mDNS (_aha-box._tcp) so the
#     app can discover it directly, no cloud/public-IP matching needed.
#   * onvif-zeep + WSDiscovery — OPTIONAL ONVIF camera auto-discovery (feature b+);
#     only used when ONVIF creds are configured, but installed so the image "just
#     works". These pull in zeep + lxml.
#
# IMPORTANT SCALE LESSON (do not regress):
#   Building images ON a weak box (e.g. HA Green, 4GB RAM) can OOM Home Assistant —
#   especially COMPILING lxml. We use python:3.12-SLIM (Debian) instead of Alpine so
#   lxml/zeep install from prebuilt manylinux wheels (NO on-device compilation). For
#   FLEET deployment, build ONCE here, push a multi-arch image to a registry (see
#   ../build-and-push.sh), and have customer boxes PULL a prebuilt `image:`.
# =============================================================================

# Debian slim: lxml/onvif-zeep have manylinux wheels here, so pip never compiles
# (avoids the OOM/slow-build risk Alpine+musl would hit). Slightly bigger image,
# much safer build.
FROM python:3.12-slim

# Runtime deps. --no-cache-dir keeps pip's download cache out of the image layer.
RUN pip install --no-cache-dir websockets zeroconf onvif-zeep WSDiscovery

# -----------------------------------------------------------------------------
# BUNDLE go2rtc into the image. A fresh Home Assistant store has NO go2rtc add-on
# to auto-install, so we ship the static go2rtc binary and the agent runs it
# itself (box_client.py: _spawn_bundled_go2rtc). go2rtc's Linux releases are
# statically linked, so they run on this slim Debian with no extra libraries.
# We pull `releases/latest` (stable URL — nothing to pin/bump) and pick the
# binary by the image's own Debian arch, so amd64 / arm64 / armhf all build.
# -----------------------------------------------------------------------------
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    case "$(dpkg --print-architecture)" in \
      amd64)       A=amd64 ;; \
      arm64)       A=arm64 ;; \
      armhf|armel) A=arm ;; \
      i386)        A=i386 ;; \
      *)           A=amd64 ;; \
    esac; \
    curl -fsSL -o /usr/local/bin/go2rtc \
      "https://github.com/AlexxIT/go2rtc/releases/latest/download/go2rtc_linux_${A}"; \
    chmod +x /usr/local/bin/go2rtc; \
    /usr/local/bin/go2rtc --version

# Copy the agent in. box_client.py is the single source of truth and lives next
# to this Dockerfile (ha-addon/aha-agent/box_client.py); the build context is
# this directory, so the COPY picks it up directly.
COPY box_client.py /box_client.py

# Run the agent directly.
#   -u = unbuffered stdout/stderr, so the agent's print() logs appear LIVE in
#        the Home Assistant add-on "Log" tab instead of being buffered.
CMD ["python", "-u", "/box_client.py"]
