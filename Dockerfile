FROM node:26-bookworm-slim
LABEL org.opencontainers.image.title="dispatch-cpu-runner" \
      org.opencontainers.image.description="Pull-based CPU job runner with per-job network-namespace isolation" \
      org.opencontainers.image.source="https://github.com/frindle/dispatch-cpu-runner" \
      org.opencontainers.image.licenses="MIT"
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 git build-essential ca-certificates openssl util-linux iproute2 procps curl \
    && rm -rf /var/lib/apt/lists/*
# Unprivileged identity every job (and every dependency install) runs as.
RUN (userdel -r node 2>/dev/null || true) && groupadd -g 10001 runner && useradd -u 10001 -g 10001 -m -s /bin/bash runner
# The agent is baked into the image so "update container" (docker pull) ships new code.
# Dev-only override: docker-compose.dev.yml bind-mounts ./agent over /opt/runner for hot reload.
COPY agent/agent.py /opt/runner/agent.py
# The supervising agent stays root ONLY to create the empty network namespace and then
# drop every capability (setpriv) before executing job code; see README "Isolation".
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 CPU_RUNNER_CACHE=/cache CPU_RUNNER_WORK=/work \
    CPU_RUNNER_STATE_DIR=/state
RUN mkdir -p /cache /work /state && chown runner:runner /cache /work && chmod 700 /state
WORKDIR /work
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD ["python3", "/opt/runner/agent.py", "healthcheck"]
ENTRYPOINT ["python3", "/opt/runner/agent.py"]
CMD ["run"]
