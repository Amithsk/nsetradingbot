"""Download and publish validated single-session NSE stock candles.

This module deliberately consumes the authenticated Zerodha service rather
than credentials or token files.  It is safe to invoke for one or more
symbols, but each invocation covers exactly one trading date.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests

try:  # Python 3.11 on the Pi; keep a compatible fallback for older venvs.
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


IST = ZoneInfo("Asia/Kolkata")
CSV_COLUMNS = ("Datetime", "Close", "High", "Low", "Open", "Volume")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "stock-download.toml"
SESSION_START = time(9, 15)
SESSION_END = time(15, 15)


class DownloadError(RuntimeError):
    """A service request or response failure for one symbol."""


class ValidationError(RuntimeError):
    """A candle payload that cannot represent a complete regular session."""


class ExistingFileConflict(RuntimeError):
    """An existing output differs from newly validated data."""


@dataclass(frozen=True)
class Settings:
    symbols: tuple[str, ...]
    endpoint_base_url: str
    output_dir: Path
    timeout_seconds: float
    publish: bool


@dataclass(frozen=True)
class StockResult:
    symbol: str
    trade_date: date
    state: str
    detail: str
    output_file: Path | None = None
    count: int | None = None
    first_timestamp: str | None = None
    last_timestamp: str | None = None


def parse_trade_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("trade date must be YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("trade date must be zero-padded YYYY-MM-DD")
    return parsed


def normalize_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    result: list[str] = []
    for symbol in symbols:
        normalized = str(symbol).strip().upper()
        if normalized.startswith("NSE:"):
            normalized = normalized[4:]
        if not normalized or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789&-" for char in normalized):
            raise ValueError(f"Invalid NSE symbol: {symbol!r}")
        if normalized not in result:
            result.append(normalized)
    if not result:
        raise ValueError("At least one symbol is required")
    return tuple(result)


def load_settings(config_path: Path | None, args: argparse.Namespace) -> Settings:
    config: dict[str, Any] = {}
    if config_path is not None:
        try:
            with config_path.open("rb") as config_file:
                config = tomllib.load(config_file)
        except FileNotFoundError as exc:
            raise ValueError(f"Configuration file not found: {config_path}") from exc

    configured_symbols = config.get("symbols", [])
    symbols = normalize_symbols(args.symbols if args.symbols else configured_symbols)
    endpoint = args.endpoint_base_url or config.get("endpoint_base_url", "http://127.0.0.1:8001")
    output_value = args.output_dir or config.get("output_dir", "Output")
    output_dir = Path(output_value)
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir
    timeout = float(config.get("timeout_seconds", 30))
    if timeout <= 0:
        raise ValueError("timeout_seconds must be positive")
    return Settings(
        symbols=symbols,
        endpoint_base_url=str(endpoint).rstrip("/"),
        output_dir=output_dir,
        timeout_seconds=timeout,
        publish=bool(config.get("publish", False) or args.publish),
    )


def is_known_non_trading_day(target_date: date) -> bool:
    if target_date.weekday() >= 5:
        return True
    # Reuse the repository's maintained NSE holiday set without changing it.
    from Code.utils.nseholiday import nseholiday

    return bool(nseholiday(target_date))


def current_ist_date() -> date:
    return datetime.now(IST).date()


def session_is_complete(now: datetime | None = None) -> bool:
    now = now or datetime.now(IST)
    return now.astimezone(IST).time() >= time(15, 20)


def fetch_symbol_candles(settings: Settings, symbol: str, trade_date: date) -> list[dict[str, Any]]:
    endpoint = urljoin(settings.endpoint_base_url + "/", "zerodha/symbol/historical-candles")
    try:
        response = requests.get(
            endpoint,
            params={"symbol": f"NSE:{symbol}", "trade_date": trade_date.isoformat()},
            timeout=settings.timeout_seconds,
        )
    except requests.RequestException as exc:
        raise DownloadError(f"request failed for {symbol} {trade_date}: {exc}") from exc
    if response.status_code != 200:
        body = response.text[:500]
        raise DownloadError(f"service returned HTTP {response.status_code} for {symbol} {trade_date}: {body}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise DownloadError(f"service returned invalid JSON for {symbol} {trade_date}") from exc
    if not isinstance(payload, dict) or payload.get("status") != "success":
        raise DownloadError(f"service returned unsuccessful payload for {symbol} {trade_date}")
    candles = payload.get("candles")
    if not isinstance(candles, list):
        raise DownloadError(f"service returned invalid candles payload for {symbol} {trade_date}")
    if not candles:
        raise DownloadError(f"service returned empty candles for {symbol} {trade_date}")
    return candles


def _parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValidationError("Datetime must be an ISO timestamp string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError(f"invalid Datetime: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValidationError(f"Datetime is missing timezone: {value!r}")
    return parsed.astimezone(IST).replace(microsecond=0)


def _number(record: dict[str, Any], field: str) -> float:
    try:
        value = float(record[field])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationError(f"invalid or missing {field}") from exc
    if not math.isfinite(value) or value < 0:
        raise ValidationError(f"invalid {field}: {record.get(field)!r}")
    return value


def validate_candles(candles: list[dict[str, Any]], target_date: date) -> list[dict[str, Any]]:
    """Return the complete, canonical 09:15--15:15 IST regular-session rows."""
    expected = [
        datetime.combine(target_date, SESSION_START, tzinfo=IST) + timedelta(minutes=5 * index)
        for index in range(73)
    ]
    expected_set = set(expected)
    rows: dict[datetime, dict[str, Any]] = {}
    for record in candles:
        if not isinstance(record, dict):
            raise ValidationError("candle must be an object")
        timestamp = _parse_timestamp(record.get("Datetime"))
        if timestamp.date() != target_date or timestamp not in expected_set:
            continue
        if timestamp in rows:
            raise ValidationError(f"duplicate candle timestamp: {timestamp.isoformat()}")
        open_value = _number(record, "Open")
        high_value = _number(record, "High")
        low_value = _number(record, "Low")
        close_value = _number(record, "Close")
        volume_value = _number(record, "Volume")
        if low_value > min(open_value, close_value) or high_value < max(open_value, close_value) or low_value > high_value:
            raise ValidationError(f"inconsistent OHLC values at {timestamp.isoformat()}")
        rows[timestamp] = {
            "Datetime": timestamp.isoformat(timespec="seconds"),
            "Close": record["Close"],
            "High": record["High"],
            "Low": record["Low"],
            "Open": record["Open"],
            "Volume": int(volume_value) if volume_value.is_integer() else record["Volume"],
        }
    missing = [timestamp.isoformat() for timestamp in expected if timestamp not in rows]
    if missing:
        raise ValidationError(f"missing {len(missing)} session candles; first missing: {missing[0]}")
    return [rows[timestamp] for timestamp in expected]


def csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    from io import StringIO

    stream = StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=CSV_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def write_or_reuse(target: Path, rows: list[dict[str, Any]]) -> str:
    content = csv_bytes(rows)
    if target.exists():
        if target.read_bytes() == content:
            return "reused"
        raise ExistingFileConflict(f"existing file differs: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(descriptor, "wb") as temporary_file:
            temporary_file.write(content)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.replace(temporary_name, target)
    except Exception:
        Path(temporary_name).unlink(missing_ok=True)
        raise
    return "written"


def run_symbol(settings: Settings, symbol: str, trade_date: date) -> StockResult:
    try:
        rows = validate_candles(fetch_symbol_candles(settings, symbol, trade_date), trade_date)
        output_file = settings.output_dir / trade_date.strftime("%Y%m%d") / f"{symbol}.csv"
        state = write_or_reuse(output_file, rows)
        return StockResult(symbol, trade_date, state, state, output_file, len(rows), rows[0]["Datetime"], rows[-1]["Datetime"])
    except DownloadError as exc:
        return StockResult(symbol, trade_date, "request_failed", str(exc))
    except ValidationError as exc:
        return StockResult(symbol, trade_date, "invalid_data", str(exc))
    except ExistingFileConflict as exc:
        return StockResult(symbol, trade_date, "conflict", str(exc))


def _git(args: list[str]) -> str:
    completed = subprocess.run(["git", *args], cwd=PROJECT_ROOT, text=True, capture_output=True, check=True)
    return completed.stdout.strip()


def publish(results: list[StockResult], trade_date: date) -> str:
    paths = [str(result.output_file.relative_to(PROJECT_ROOT)) for result in results if result.output_file]
    _git(["add", "--", *paths])
    message = f"Stock candles {trade_date:%Y%m%d}"
    _git(["commit", "--only", "-m", message, "--", *paths])
    commit = _git(["rev-parse", "HEAD"])
    _git(["push", "origin", "HEAD"])
    branch = _git(["branch", "--show-current"])
    remote_commit = _git(["ls-remote", "origin", f"refs/heads/{branch}"]).split()
    if not remote_commit or remote_commit[0] != commit:
        raise RuntimeError(f"push completed but remote did not confirm commit {commit}")
    return commit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", help="NSE symbols; overrides config watchlist")
    parser.add_argument("--trade-date", type=parse_trade_date, help="YYYY-MM-DD; explicit historical session")
    parser.add_argument("--config", type=Path, help="TOML configuration file")
    parser.add_argument("--endpoint-base-url", help="Pi market-data service URL; overrides config")
    parser.add_argument("--output-dir", help="Output root; overrides config")
    parser.add_argument("--publish", action="store_true", help="Commit and push only successful stock CSVs")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config_path = args.config or (None if args.symbols else DEFAULT_CONFIG)
    try:
        settings = load_settings(config_path, args)
    except ValueError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    explicit_date = args.trade_date is not None
    trade_date = args.trade_date or current_ist_date()
    if not explicit_date:
        if is_known_non_trading_day(trade_date):
            print(f"skipped: {trade_date} is a known non-trading day")
            return 0
        if not session_is_complete():
            print(f"deferred: {trade_date} session has not completed in IST")
            return 0

    results = [run_symbol(settings, symbol, trade_date) for symbol in settings.symbols]
    for result in results:
        print(json.dumps({
            "symbol": result.symbol, "trade_date": result.trade_date.isoformat(), "state": result.state,
            "detail": result.detail, "file": str(result.output_file) if result.output_file else None,
            "count": result.count, "first": result.first_timestamp, "last": result.last_timestamp,
        }, sort_keys=True))

    if any(result.state not in {"written", "reused"} for result in results):
        print("run incomplete; publication skipped", file=sys.stderr)
        return 1
    if settings.publish:
        try:
            print(f"published commit: {publish(results, trade_date)}")
        except (subprocess.CalledProcessError, RuntimeError) as exc:
            print(f"local verification passed; publication pending: {exc}", file=sys.stderr)
            return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
