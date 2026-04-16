"""Entry point for running the Graph API with uvicorn.

Usage:
    python -m semantic_index.api
"""

import logging
import socket

import uvicorn

from semantic_index.api.app import create_app
from semantic_index.api.config import Settings

logging.basicConfig(level=logging.INFO)

settings = Settings()
app = create_app(settings.db_path, anthropic_api_key=settings.anthropic_api_key)


def find_available_port(host: str, start: int) -> int:
    """Return start if available, otherwise find the next free port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex((host if host != "0.0.0.0" else "127.0.0.1", start)) != 0:
            return start
    # Port in use — let the OS pick one
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


if __name__ == "__main__":
    port = find_available_port(settings.host, settings.port)
    if port != settings.port:
        logging.getLogger(__name__).info("Port %d in use, using %d instead", settings.port, port)
    uvicorn.run(app, host=settings.host, port=port)
