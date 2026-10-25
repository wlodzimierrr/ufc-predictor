# syntax=docker/dockerfile:1
FROM python:3.11-slim-bookworm

ARG IMAGE_REVISION=unknown
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    IMAGE_REVISION=${IMAGE_REVISION} \
    MPLCONFIGDIR=/tmp/matplotlib
LABEL org.opencontainers.image.source="https://github.com/wlodzimierrr/ufc-predictor" \
      org.opencontainers.image.revision="${IMAGE_REVISION}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates libgomp1 make tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 automation \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /tmp automation \
    && mkdir -p /app /runtime-inputs/data /runtime-inputs/models \
    && chown 10001:10001 /app

WORKDIR /app
COPY requirements-automation.txt ./
RUN python3 -m pip install --no-cache-dir -r requirements-automation.txt

# The build context is allowlisted separately. Never bake in a local input
# bundle, model, .env file or evidence directory, even for a local build.
COPY --chown=10001:10001 Makefile predict.py ./
COPY --chown=10001:10001 betting/ ./betting/
COPY --chown=10001:10001 features/ ./features/
COPY --chown=10001:10001 modeling/ ./modeling/
COPY --chown=10001:10001 warehouse/ ./warehouse/
COPY --chown=10001:10001 configs/betting_conservative_candidate.toml ./configs/
COPY --chown=10001:10001 scripts/automation/ ./scripts/automation/
RUN chmod 0555 scripts/automation/*.sh

# Import the actual refresh commands during builds. This checks runtime
# dependencies without opening a database, loading a model, or running a refresh.
RUN python3 -c "import predict; import features.build_upcoming; import modeling.build_pre_event_prediction_log; import betting.backtest; import warehouse.load_betting_reports"

USER 10001:10001

# refresh.sh validates and copies the read-only /runtime-inputs mounts into
# private writable /app/data and /app/models before invoking serial make.
# tini forwards termination signals to the refresh process group and reaps
# children; the wrapper retains a failed command's exit status.
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/app/scripts/automation/refresh.sh"]
CMD []
