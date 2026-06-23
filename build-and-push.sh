#!/usr/bin/env bash
# =============================================================================
# build-and-push.sh — build a MULTI-ARCH AHA Agent image and push it to a
#                      container registry, so customer boxes PULL it.
# =============================================================================
# WHY THIS EXISTS (the OOM lesson):
#   By default the AHA Agent add-on builds its image ON the box from the
#   Dockerfile in aha-agent/. The agent is tiny, so that works for one box.
#   But building images on a WEAK box (e.g. HA Green, 4 GB RAM) can OOM Home
#   Assistant. At fleet scale you build ONCE here, push a multi-arch image, and
#   have every box simply PULL the matching-arch image instead of building.
#
# WHAT IT DOES:
#   Uses `docker buildx` to build the aha-agent image for BOTH linux/arm64 and
#   linux/amd64 (the two arches the add-on targets) in a single command and
#   push the result to $REGISTRY.
#
# PREREQUISITES (one-time on your build machine, NOT on a box):
#   - Docker with Buildx + QEMU emulation, so one machine can build all arches:
#       docker run --privileged --rm tonistiigi/binfmt --install all
#   - You are logged in to your registry:  docker login <registry>
#
# USAGE:
#   1. Edit the REGISTRY placeholder below (or export REGISTRY=... before run).
#   2. ./build-and-push.sh            # builds + pushes :latest and :<VERSION>
# =============================================================================

# Fail fast: stop on any error, on unset variables, and on pipe failures.
set -euo pipefail

# -----------------------------------------------------------------------------
# Config — change REGISTRY to your own. Examples:
#   ghcr.io/your-org          (GitHub Container Registry)
#   docker.io/your-dockerhub  (Docker Hub)
#   123456789.dkr.ecr.ap-south-1.amazonaws.com  (AWS ECR)
# REGISTRY/IMAGE may be overridden from the environment, e.g.:
#   REGISTRY=ghcr.io/acme ./build-and-push.sh
# -----------------------------------------------------------------------------
REGISTRY="${REGISTRY:-REGISTRY_PLACEHOLDER}"   # <-- EDIT ME (or export REGISTRY=...)
IMAGE_NAME="${IMAGE_NAME:-aha-agent}"

# Keep this in lockstep with config.yaml's `version:` so the pushed tag matches
# what the add-on expects to pull.
VERSION="${VERSION:-1.0.0}"

# The build context is the add-on folder (it holds the Dockerfile + box_client.py).
# This script lives in ha-addon/, so the context is ha-addon/aha-agent/.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTEXT_DIR="${SCRIPT_DIR}/aha-agent"

# Platforms to build for. MUST cover the arches listed in config.yaml's `arch:`.
#   linux/arm64  -> HA Green/Yellow, Raspberry Pi 4/5 (64-bit), arm servers
#   linux/amd64  -> generic x86 mini-PCs / NUCs
# (armv7 is in config.yaml's arch list for old 32-bit Pis; add linux/arm/v7
#  here too if you intend to support them — python:3.12-alpine publishes it.)
PLATFORMS="linux/arm64,linux/amd64"

# Full image references (we push both a version tag and :latest).
IMAGE_VERSIONED="${REGISTRY}/${IMAGE_NAME}:${VERSION}"
IMAGE_LATEST="${REGISTRY}/${IMAGE_NAME}:latest"

# -----------------------------------------------------------------------------
# Guardrail: refuse to run until REGISTRY has actually been set.
# -----------------------------------------------------------------------------
if [ "${REGISTRY}" = "REGISTRY_PLACEHOLDER" ]; then
  echo "ERROR: set REGISTRY first." >&2
  echo "  Edit the REGISTRY line in this script, or run:" >&2
  echo "    REGISTRY=ghcr.io/your-org ./build-and-push.sh" >&2
  exit 1
fi

echo "==> Building ${IMAGE_NAME} for ${PLATFORMS}"
echo "    context : ${CONTEXT_DIR}"
echo "    tags    : ${IMAGE_VERSIONED}"
echo "              ${IMAGE_LATEST}"

# -----------------------------------------------------------------------------
# Ensure a buildx builder that supports multi-arch exists and is selected.
# `docker buildx create` makes a builder backed by the docker-container driver
# (required for multi-platform). We name it so re-runs are idempotent.
# -----------------------------------------------------------------------------
BUILDER="aha-multiarch"
if ! docker buildx inspect "${BUILDER}" >/dev/null 2>&1; then
  echo "==> Creating buildx builder '${BUILDER}'"
  docker buildx create --name "${BUILDER}" --driver docker-container --use
else
  docker buildx use "${BUILDER}"
fi
# Boot the builder (no-op if already running) so the build starts cleanly.
docker buildx inspect --bootstrap >/dev/null

# -----------------------------------------------------------------------------
# Build for ALL platforms in one shot and push directly to the registry.
# NOTE: --push is required for multi-arch. A multi-platform image cannot be
# loaded into the local docker daemon (it has no single-arch image to load);
# it must be pushed as a manifest list. Boxes then pull the manifest and Docker
# automatically selects the layer matching the box's CPU arch.
# -----------------------------------------------------------------------------
docker buildx build \
  --platform "${PLATFORMS}" \
  --tag "${IMAGE_VERSIONED}" \
  --tag "${IMAGE_LATEST}" \
  --push \
  "${CONTEXT_DIR}"

echo "==> Done. Pushed:"
echo "      ${IMAGE_VERSIONED}"
echo "      ${IMAGE_LATEST}"
echo
echo "Next: switch the add-on to PULL this image instead of building on the box."
echo "In aha-agent/config.yaml add an 'image:' key and drop the local build, e.g.:"
echo
echo "    image: \"${REGISTRY}/${IMAGE_NAME}-{arch}\""
echo "    version: \"${VERSION}\""
echo
echo "Home Assistant substitutes {arch} (aarch64/amd64/armv7) and PULLS the"
echo "matching image — no on-device build, no OOM risk on weak boxes."
echo
echo "NOTE on the {arch} naming: Home Assistant's 'image:' template expects a"
echo "PER-ARCH repository suffix (…-aarch64, …-amd64). If you publish a single"
echo "multi-arch manifest under one tag (as this script does), either:"
echo "  (a) also push per-arch tags named with HA's arch suffixes, or"
echo "  (b) reference the manifest directly without {arch} and rely on Docker"
echo "      to pick the right layer per box."
