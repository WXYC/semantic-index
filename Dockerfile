FROM python:3.12-slim AS rust-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential git && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install --no-cache-dir maturin

WORKDIR /build
RUN git clone --depth 1 https://github.com/WXYC/wxyc-etl.git && \
    cd wxyc-etl/wxyc-etl-python && \
    maturin build --release && \
    cp /build/wxyc-etl/target/wheels/*.whl /build/

FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY --from=rust-builder /build/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

COPY pyproject.toml .
RUN pip install --no-cache-dir ".[api]"

COPY semantic_index/ semantic_index/
COPY scripts/ scripts/
COPY explorer/ explorer/
COPY data/ data/
COPY start.sh .

ENV DB_PATH=/data/wxyc_artist_graph.db

EXPOSE 8083

CMD ["./start.sh"]
