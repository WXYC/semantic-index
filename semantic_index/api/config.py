"""API configuration via pydantic-settings."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Graph API configuration.

    Reads from environment variables (case-insensitive) or a .env file.

    Attributes:
        db_path: Path to the SQLite graph database produced by the pipeline.
        port: Port for the uvicorn server.
    """

    db_path: str = "output/wxyc_artist_graph.db"
    port: int = 8000
