"""API configuration via pydantic-settings."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Graph API configuration.

    Reads from environment variables (case-insensitive) or a .env file.

    Attributes:
        db_path: Path to the SQLite graph database produced by the pipeline.
        host: Host to bind the uvicorn server to.
        port: Port for the uvicorn server.
    """

    db_path: str = "data/wxyc_artist_graph.db"
    host: str = "0.0.0.0"
    port: int = 8000
    anthropic_api_key: str | None = None
