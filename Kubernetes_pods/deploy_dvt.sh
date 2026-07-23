# 1. Project Configuration Variables
$PROJECT_ID="groovy-scarab-502011-g2"
$LOCATION="us-central1"
$REPO_NAME="dvt-docker-repo"
$IMAGE_NAME="data-validation"
$TAG="v1"
$FULL_IMAGE_PATH="${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${TAG}"

Write-Host "🔎 Step 1: Configuring Docker authentication..."
gcloud auth configure-docker "${LOCATION}-docker.pkg.dev" --quiet

Write-Host "📁 Step 2: Checking Artifact Registry repository..."
try {
    gcloud artifacts repositories describe "$REPO_NAME" --location="$LOCATION" --project="$PROJECT_ID" --quiet
    Write-Host "✅ Artifact Repository exists."
} catch {
    Write-Host "📦 Repository missing. Creating repository..."
    gcloud artifacts repositories create "$REPO_NAME" `
        --repository-format=docker `
        --location="$LOCATION" `
        --project="$PROJECT_ID" `
        --description="Docker repository for Data Validation Tool"
}

Write-Host "🏗️ Step 3: Compiling container using Docker BuildKit..."
$env:DOCKER_BUILDKIT="1"
docker build --platform linux/amd64 --no-cache -t "${IMAGE_NAME}:${TAG}" .

Write-Host "🏷️ Step 4: Tagging image..."
docker tag "${IMAGE_NAME}:${TAG}" "$FULL_IMAGE_PATH"

Write-Host "🚀 Step 5: Pushing image to Google Artifact Registry..."
docker push "$FULL_IMAGE_PATH"

Write-Host "=============================================================================="
Write-Host "✅ DEPLOYMENT SUCCESSFUL!"
Write-Host "👉 Image Path: $FULL_IMAGE_PATH"
Write-Host "=============================================================================="