FROM python:3.14.6-slim
# 1. Install compiler tools, Java JDK, and dependencies required to build C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    default-jdk \
    curl \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# 2. Set JAVA_HOME (Required by JPype / JayDeBeApi)
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 3. Upgrade pip and install DVT & database connectors
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        google-pso-data-validator \
        JayDeBeApi \
        google-cloud-secret-manager