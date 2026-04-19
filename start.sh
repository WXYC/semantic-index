#!/bin/sh
exec uvicorn semantic_index.api.app:app --host "${HOST:-0.0.0.0}" --port "${PORT:-8083}" --workers "${WORKERS:-4}"
