#/nsetradingbot/Code/zerodha/config.py
"""Configuration interface for future Zerodha Kite Connect usage.

This module intentionally does not create credentials or perform
unauthenticated API calls. It only defines the configuration contract and
how runtime credentials will be supplied when the client is wired in.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class KiteConfig:
    """Container for the values required by a future KiteConnect client."""

    api_key: str
    api_secret: str
    access_token: str

    @classmethod
    def from_env(
        cls,
        api_key_env: str = "KITE_API_KEY",
        api_secret_env: str = "KITE_API_SECRET",
        access_token_env: str = "ACCESS_TOKEN",
    ) -> "KiteConfig":
        api_key = os.getenv(api_key_env)
        api_secret = os.getenv(api_secret_env)
        access_token = os.getenv(access_token_env)

        missing = []
        if not api_key:
            missing.append(api_key_env)
        if not api_secret:
            missing.append(api_secret_env)
        if not access_token:
            missing.append(access_token_env)

        if missing:
            raise ValueError(
                "Missing Kite Connect configuration: " + ", ".join(missing)
            )

        return cls(
            api_key=api_key,
            api_secret=api_secret,
            access_token=access_token,
        )


def load_kite_config(
    api_key_env: str = "KITE_API_KEY",
    api_secret_env: str = "KITE_API_SECRET",
    access_token_env: str = "ACCESS_TOKEN",
) -> KiteConfig:
    """Load credentials from environment variables without hard-coding secrets."""

    return KiteConfig.from_env(
        api_key_env=api_key_env,
        api_secret_env=api_secret_env,
        access_token_env=access_token_env,
    )


def get_kite_client(
    client_factory: Optional[Callable[[], Any]] = None,
    config: Optional[KiteConfig] = None,
    *,
    api_key_env: str = "KITE_API_KEY",
    api_secret_env: str = "KITE_API_SECRET",
    access_token_env: str = "ACCESS_TOKEN",
):
    """Return a KiteConnect client factory result when one is injected.

    Authentication is intentionally not implemented in this phase. This helper
    only gives the structure needed for a later injection point.
    """

    if client_factory is not None:
        return client_factory()

    if config is not None:
        raise NotImplementedError(
            "KiteConnect client construction is intentionally deferred. "
            "The client should be injected in a later phase."
        )

    try:
        load_kite_config(
            api_key_env=api_key_env,
            api_secret_env=api_secret_env,
            access_token_env=access_token_env,
        )
    except ValueError as exc:
        raise NotImplementedError(
            "Authentication is not implemented yet. Configure the Kite client "
            "later by injecting a client factory or credentials."
        ) from exc

    raise NotImplementedError(
        "KiteConnect client creation is intentionally deferred for Phase 2. "
        "Authentication will be added in a later step."
    )
