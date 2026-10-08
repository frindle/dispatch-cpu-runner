FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 git build-essential ca-certificates openssl util-linux iproute2 procps curl \
    && rm -rf /var/lib/apt/lists/*
# Unprivileged identity every job (and every dependency install) runs as.
RUN (userdel -r node 2>/dev/null || true) && groupadd -g 10001 runner && useradd -u 10001 -g 10001 -m -s /bin/bash runner
# Fallback copy only: docker-compose.yml bind-mounts ./agent over /opt/runner (read-only) so code
# updates do not need an image rebuild. Rebuilds are for Dockerfile (dependency) changes only.
COPY agent/agent.py /opt/runner/agent.py
# The supervising agent stays root ONLY to create the empty network namespace and then
# drop every capability (setpriv) before executing job code; see README "Isolation".
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 CPU_RUNNER_CACHE=/cache CPU_RUNNER_WORK=/work
RUN mkdir -p /cache /work && chown runner:runner /cache /work
WORKDIR /work
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD ["python3", "/opt/runner/agent.py", "healthcheck"]
ENTRYPOINT ["python3", "/opt/runner/agent.py"]
CMD ["run"]
