Build & Push the DVT Container Image
To process large validation tasks safely, DVT runs inside isolated containers. This setup handles cross-architecture, pins specific library version targets to prevent breaking changes.
1. Create the Dockerfile (Dockerfile)
Create a new file named Dockerfile on your local build machine or VM environment using a text editor:
dockerfile
# Utilizing stable, specific debian slim variants to prevent breaking runtime regressions
FROM python:3.11.9-slim-bookworm

# System configuration variables to ensure clean streaming container output
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install essential native binaries needed for database communication bindings
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libc6-dev \
    libsqlite3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade installation package managers natively
RUN pip install --upgrade pip

# Pin explicit tool versions to ensure reproducible environments over multi-year migration projects
RUN pip install "google-pyspark-data-validation[bigquery,teradata]==3.10.0"
# pip install google-pso-data-validator[teradata]
# Enforce secure least-privilege runtime footprints by disabling root execution inside GKE
RUN useradd -m -u 1001 dvtuser
USER dvtuser
WORKDIR /home/dvtuser

# Set entrypoint to native DVT command binary structure
ENTRYPOINT ["data-validation"]
