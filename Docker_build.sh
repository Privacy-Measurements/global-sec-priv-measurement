#!/usr/bin/env bash
set -euo pipefail

# Builds the Docker image from the repo root Dockerfile
# and saves it as a portable .tar archive.
#
# Usage:
#   ./Docker_build.sh [image_name] [tag] [output_tar]
#
# Examples:
#   ./Docker_build.sh
#   ./Docker_build.sh global-privacy-ssh latest
#   ./Docker_build.sh global-privacy-ssh v1 /tmp/global-privacy-ssh-v1.tar

IMAGE_NAME="${1:-global-privacy-ssh}"
IMAGE_TAG="${2:-latest}"
OUTPUT_TAR="${3:-${IMAGE_NAME//\//_}-${IMAGE_TAG}.tar}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [[ ! -f "Dockerfile" ]]; then
  echo "Error: Dockerfile not found in ${SCRIPT_DIR}" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker CLI not found. Install Docker first." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Error: Cannot connect to Docker daemon (unix:///var/run/docker.sock)." >&2
  echo "Start Docker and retry, for example:" >&2
  echo "  sudo systemctl start docker" >&2
  echo "  sudo systemctl enable docker" >&2
  exit 1
fi

IMAGE_REF="${IMAGE_NAME}:${IMAGE_TAG}"

echo "[1/2] Building Docker image: ${IMAGE_REF}"
docker build -t "${IMAGE_REF}" .

echo "[2/2] Saving image to tar: ${OUTPUT_TAR}"
docker save -o "${OUTPUT_TAR}" "${IMAGE_REF}"

echo "Done."
echo "Image: ${IMAGE_REF}"
echo "Tarball: ${SCRIPT_DIR}/${OUTPUT_TAR}"
echo ""
echo "Load on another machine with:"
echo "  docker load -i ${OUTPUT_TAR}"
