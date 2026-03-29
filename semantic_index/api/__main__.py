"""Entry point for running the Graph API with uvicorn.

Usage:
    python -m semantic_index.api
"""

import logging

import uvicorn

from semantic_index.api.app import create_app
from semantic_index.api.config import Settings

logging.basicConfig(level=logging.INFO)

settings = Settings()
app = create_app(settings.db_path)

if __name__ == "__main__":
    uvicorn.run(app, host=settings.host, port=settings.port)
