"""ASGI entrypoint: `uvicorn obs_platform.api.main:app`."""

from .app import create_app

app = create_app()

__all__ = ["app"]
