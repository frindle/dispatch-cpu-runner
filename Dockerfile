FROM node:26-bookworm-slim
LABEL org.opencontainers.image.title="dispatch-cpu-runner" \
      org.opencontainers.image.description="Pull-based CPU job runner with per-job network-namespace isolation" \
      org.opencontainers.image.source="https://github.com/frindle/dispatch-cpu-runner" \
      org.opencontainers.image.licenses="MIT"
# /usr/bin/python3 (Debian 3.11) runs the AGENT only. Jobs get python 3.14 (below) first on PATH.
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 git build-essential ca-certificates openssl util-linux iproute2 procps curl sqlite3 tzdata \
    && rm -rf /var/lib/apt/lists/*
# Job python: a pinned, checksummed python-build-standalone CPython whose MINOR matches the queue host's
# (3.14), so a python bundle's verify runs on the same interpreter generation it was authored against.
# Debian bookworm only ships 3.11, hence the standalone build (self-contained: libc only).
# Bump: change PY_VERSION/PBS_TAG and both sha256 values (the release page lists them) in the same commit.
ARG PY_VERSION=3.14.8
ARG PBS_TAG=20261009
ARG PY_SHA256_X86_64=ddc902fd26460f728758c67d80417c1031edf97589f1f162c4ba3aa158a35998
ARG PY_SHA256_AARCH64=54f9cd13b97add6bbd077175b6d89f974cc582ece1c40ace1893067582edaf0e
COPY requirements-runner.txt /tmp/requirements-runner.txt
RUN set -eux; \
    case "$(uname -m)" in \
      x86_64) a=x86_64; sha="$PY_SHA256_X86_64" ;; \
      aarch64|arm64) a=aarch64; sha="$PY_SHA256_AARCH64" ;; \
      *) echo "unsupported arch $(uname -m)" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/py.tgz "https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_TAG}/cpython-${PY_VERSION}%2B${PBS_TAG}-${a}-unknown-linux-gnu-install_only_stripped.tar.gz"; \
    echo "${sha}  /tmp/py.tgz" | sha256sum -c -; \
    mkdir -p /opt/python; tar -xzf /tmp/py.tgz -C /opt/python --strip-components=1 --no-same-owner; rm /tmp/py.tgz; \
    /opt/python/bin/python3 -m venv /opt/venv; \
    /opt/venv/bin/pip install --no-cache-dir --disable-pip-version-check --only-binary=:all: -r /tmp/requirements-runner.txt; \
    /opt/venv/bin/python3 -c "import sqlite3, ssl, flask, requests, paramiko; print('job python ok')"; \
    rm -f /tmp/requirements-runner.txt
ENV PATH=/opt/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
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
  CMD ["/usr/bin/python3", "/opt/runner/agent.py", "healthcheck"]
ENTRYPOINT ["/usr/bin/python3", "/opt/runner/agent.py"]
CMD ["run"]
