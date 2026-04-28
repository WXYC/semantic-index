FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY pyproject.toml .
RUN pip install --no-cache-dir ".[api]"

COPY semantic_index/ semantic_index/
COPY generated/ generated/
COPY scripts/ scripts/
COPY explorer/ explorer/
COPY data/ data/
COPY start.sh .

ENV DB_PATH=/data/wxyc_artist_graph.db

EXPOSE 8083

CMD ["./start.sh"]
