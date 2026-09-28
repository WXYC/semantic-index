FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# `build` adds boto3 for the out-of-process nightly rebuild's S3 round-trip
# (scripts/run_build_job.py); the same image serves the API and runs the VPC
# build task (command overridden in infra/build-job.yaml). No Essentia —
# nightly_sync needs none of the audio modules. See plans/si-out-of-process-rebuild.
COPY pyproject.toml .
RUN pip install --no-cache-dir ".[api,build]"

COPY semantic_index/ semantic_index/
COPY generated/ generated/
COPY scripts/ scripts/
COPY explorer/ explorer/
COPY start.sh .

# No `COPY data/`. The serving database is not in the build context: it reaches
# the host as an S3 build artifact that the conductor swaps into the volume
# mounted at /data (see infra/README.md). DB_PATH below is absolute, so the
# relative default in semantic_index/api/config.py never applies in the image
# and an /app/data copy would be read by nothing.
ENV DB_PATH=/data/wxyc_artist_graph.db

EXPOSE 8083

CMD ["./start.sh"]
