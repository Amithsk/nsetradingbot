#/nsetradingbot/Code/zerodha/config.py
"""Zerodha Kite Connect configuration and client creation."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from kiteconnect import KiteConnect


# /home/amith/nsetradingbot
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# /home/amith/nsetradingbot/.env
ENV_FILE = PROJECT_ROOT / ".env"

# Daily access token generated after Zerodha login.
ACCESS_TOKEN_FILE = (
    PROJECT_ROOT / "Output" / "zerodha_access_token.json"
)

load_dotenv(ENV_FILE)


@dataclass(frozen=True)
class KiteConfig:
    """Zerodha Kite Connect credentials loaded from .env."""

    api_key: str
    api_secret: str

    @classmethod
    def from_env(
        cls,
        api_key_env: str = "KITE_API_KEY",
        api_secret_env: str = "KITE_API_SECRET",
    ) -> "KiteConfig":

        api_key = os.getenv(api_key_env)
        api_secret = os.getenv(api_secret_env)

        missing = []

        if not api_key:
            missing.append(api_key_env)

        if not api_secret:
            missing.append(api_secret_env)

        if missing:
            raise ValueError(
                "Missing Kite Connect configuration: "
                + ", ".join(missing)
            )

        return cls(
            api_key=api_key,
            api_secret=api_secret,
        )


def load_kite_config() -> KiteConfig:
    """Load Zerodha credentials from the project .env file."""

    return KiteConfig.from_env()


def load_access_token() -> str:
    """Load the daily Zerodha access token from the local token file."""

    if not ACCESS_TOKEN_FILE.exists():
        raise FileNotFoundError(
            f"Zerodha access token file not found: {ACCESS_TOKEN_FILE}"
        )

    try:
        data = json.loads(
            ACCESS_TOKEN_FILE.read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in access token file: {ACCESS_TOKEN_FILE}"
        ) from exc

    access_token = data.get("access_token")

    if not access_token:
        raise ValueError(
            f"Access token missing from: {ACCESS_TOKEN_FILE}"
        )

    return str(access_token)


def get_kite_client() -> KiteConnect:
    """Create an authenticated KiteConnect client."""

    config = load_kite_config()
    access_token = load_access_token()

    kite = KiteConnect(
        api_key=config.api_key
    )

    kite.set_access_token(access_token)

    return kite