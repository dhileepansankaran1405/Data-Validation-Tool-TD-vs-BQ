#!/bin/bash
# ==============================================================================
# DEPLOYMENT ENGINE: DVT ENTERPRISE ARTIFACT REGISTRY PUSH
# ==============================================================================
set -euo pipefail

# Infrastructure Configurations
PROJECT_ID="your-gcp-project-id"   # MODIFY WITH YOUR ACTUAL GCP PROJECT ID
LOCATION="us-central1"             # MODIFY WITH TARGET CLOUD COMPOSER REGION
REPO_NAME="dvt-docker-repo"
IMAGE_NAME="data-validation"
TAG="v1"

FULL_IMAGE_PATH="${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${TAG}"

echo "🔎 Prerequisite Check: Validating system tool readiness..."
for cmd in gcloud docker; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "❌ Error: Required tool '$cmd' is missing from host environment." >&2
        exit 1
    fi
done

echo "🔄 Step 1: Configuring local Docker authentication for Artifact Registry..."
gcloud auth configure-docker "${LOCATION}-docker.pkg.dev" --quiet

echo "📁Step 2: Validating remote Artifact Registry container target existence..."
if ! gcloud artifacts repositories describe "${REPO_NAME}" --location="${LOCATION}" --project="${PROJECT_ID}" &> /dev/null; then
    echo "📦 Repository missing. Instantiating secure regional Docker Registry repository..."
    gcloud artifacts repositories create "${REPO_NAME}" \
        --repository-format=docker \
        --location="${LOCATION}" \
        --project="${PROJECT_ID}" \
        --description="Production Docker artifact vault for Cloud Composer Data Validation Tool"
else
    echo "✅ Artifact Repository configuration discovered safely."
fi

echo "🏗️ Step 3: Compiling container architecture using Docker BuildKit..."
# Enforcing platform limits guarantees an M1/M2/M3 Mac correctly outputs Linux x86/64 binaries for GKE nodes
export DOCKER_BUILDKIT=1
docker build \
    --platform linux/amd64 \
    --no-cache \
    -t "${IMAGE_NAME}:${TAG}" .

echo "🏷️ Step 4: Normalizing local image tagging parameters..."
docker tag "${IMAGE_NAME}:${TAG}" "${FULL_IMAGE_PATH}"

echo "🚀 Step 5: Streaming structured container images to Google Cloud Artifact Vault..."
docker push "${FULL_IMAGE_PATH}"

echo "=============================================================================="
echo "✅ DEPLOYMENT SUCCESSFUL!"
echo "📍 Airflow Container Target Image Path Reference:"
echo "👉 ${FULL_IMAGE_PATH}"
echo "=============================================================================="
