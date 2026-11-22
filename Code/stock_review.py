"""Generate one immutable, completed-session Stock Review from Git and MySQL.

The command never contacts the Pi.  Stock candles are read with ``git show``
at the supplied revision; the NIFTY benchmark is read from local MySQL.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import statistics
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from Code.utils.nseintraday_db_utils import connect_db


IST = ZoneInfo("Asia/Kolkata")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CSV_COLUMNS = ("Datetime", "Close", "High", "Low", "Open", "Volume")
SESSION_START = time(9, 15)
SESSION_COUNT = 72
SCHEMA_VERSION = "stock-review-v1"


class InputError(RuntimeError):
    """An input cannot produce a completed review."""


@dataclass(frozen=True)
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("trade date must be YYYY-MM-DD") from exc


def expected_timestamps(trade_date: date) -> list[datetime]:
    start = datetime.combine(trade_date, SESSION_START, tzinfo=IST)
    return [start + timedelta(minutes=5 * index) for index in range(SESSION_COUNT)]


def _number(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise InputError(f"invalid {field}: {value!r}") from exc
    if not math.isfinite(number) or number < 0:
        raise InputError(f"invalid {field}: {value!r}")
    return number


def _timestamp(value: Any, source: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
        # MySQL's existing nifty_prices timestamps are naive; that table is
        # produced on Windows from IST-labelled CSVs, so treat them as IST.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=IST)
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise InputError(f"invalid {source} timestamp: {value!r}") from exc
        if parsed.tzinfo is None:
            raise InputError(f"{source} timestamp has no timezone: {value!r}")
    return parsed.astimezone(IST).replace(microsecond=0)


def validate_candles(records: Iterable[dict[str, Any]], trade_date: date, source: str) -> list[Candle]:
    expected = expected_timestamps(trade_date)
    expected_set = set(expected)
    rows: dict[datetime, Candle] = {}
    for record in records:
        timestamp = _timestamp(record.get("Datetime", record.get("Date")), source)
        if timestamp.date() != trade_date or timestamp not in expected_set:
            continue
        if timestamp in rows:
            raise InputError(f"{source} has duplicate timestamp {timestamp.isoformat()}")
        candle = Candle(
            timestamp=timestamp,
            open=_number(record.get("Open"), f"{source}.Open"),
            high=_number(record.get("High"), f"{source}.High"),
            low=_number(record.get("Low"), f"{source}.Low"),
            close=_number(record.get("Close"), f"{source}.Close"),
            volume=_number(record.get("Volume"), f"{source}.Volume"),
        )
        if candle.low > min(candle.open, candle.close) or candle.high < max(candle.open, candle.close):
            raise InputError(f"{source} has inconsistent OHLC at {timestamp.isoformat()}")
        rows[timestamp] = candle
    missing = [stamp.isoformat() for stamp in expected if stamp not in rows]
    if missing:
        raise InputError(f"{source} missing {len(missing)} aligned candles; first gap: {missing[0]}")
    return [rows[stamp] for stamp in expected]


def resolve_revision(revision: str) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", f"{revision}^{{commit}}"], cwd=PROJECT_ROOT,
        text=True, capture_output=True, check=False,
    )
    if completed.returncode:
        raise InputError(f"cannot resolve Git revision {revision!r}: {completed.stderr.strip()}")
    return completed.stdout.strip()


def read_stock_from_git(symbol: str, trade_date: date, revision: str) -> tuple[list[Candle], dict[str, Any]]:
    symbol = symbol.strip().upper()
    if not symbol or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789&-" for char in symbol):
        raise InputError(f"invalid NSE symbol: {symbol!r}")
    path = f"Output/{trade_date:%Y%m%d}/{symbol}.csv"
    completed = subprocess.run(
        ["git", "show", f"{revision}:{path}"], cwd=PROJECT_ROOT,
        text=True, capture_output=True, check=False,
    )
    if completed.returncode:
        raise InputError(f"stock CSV not present at {revision}:{path}")
    raw = completed.stdout
    reader = csv.DictReader(io.StringIO(raw))
    if tuple(reader.fieldnames or ()) != CSV_COLUMNS:
        raise InputError(f"stock CSV header must be {','.join(CSV_COLUMNS)}")
    rows = validate_candles(list(reader), trade_date, "stock CSV")
    return rows, {
        "schema_version": SCHEMA_VERSION,
        "source": "git",
        "git_revision": revision,
        "csv_path": path,
        "sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "candle_count": len(rows),
        "interval_minutes": 5,
        "timezone": "Asia/Kolkata",
        "first_timestamp": rows[0].timestamp.isoformat(),
        "last_timestamp": rows[-1].timestamp.isoformat(),
    }


def read_nifty(trade_date: date) -> list[Candle]:
    engine = connect_db({"host": "localhost", "user": "root", "db": "nifty", "port": 3306})
    try:
        with engine.connect() as conn:
            records = conn.execute(text("""
                SELECT Date, Open, High, Low, Close, Volume
                FROM nifty_prices
                WHERE DATE(Date) = :trade_date
                ORDER BY Date
            """), {"trade_date": trade_date}).mappings().all()
    finally:
        engine.dispose()
    # NIFTY timestamps are stored without an offset, but are semantically IST.
    normalized = []
    for record in records:
        item = dict(record)
        item["Datetime"] = item.pop("Date")
        timestamp = item["Datetime"]
        if isinstance(timestamp, datetime) and timestamp.tzinfo is None:
            item["Datetime"] = timestamp.replace(tzinfo=IST)
        normalized.append(item)
    return validate_candles(normalized, trade_date, "NIFTY benchmark")


def _distinct(indices: Iterable[int], minimum_gap: int = 3) -> list[int]:
    result: list[int] = []
    for index in sorted(indices):
        if not result or index - result[-1] >= minimum_gap:
            result.append(index)
    return result


def _zone(candles: list[Candle], direction: str) -> dict[str, Any]:
    """Deterministic V1 zone: 2+ distinct touches of a confirmed 2/2 swing."""
    ranges = [candle.high - candle.low for candle in candles]
    tolerance = max(statistics.median(ranges) * 0.35, candles[-1].close * 0.0015)
    pivots: list[tuple[int, float]] = []
    for index in range(2, len(candles) - 2):
        window = candles[index - 2:index + 3]
        value = candles[index].low if direction == "support" else candles[index].high
        values = [c.low if direction == "support" else c.high for c in window]
        if (direction == "support" and value == min(values)) or (direction == "resistance" and value == max(values)):
            pivots.append((index, value))
    candidates: list[dict[str, Any]] = []
    for _, level in pivots:
        lower, upper = level - tolerance, level + tolerance
        if direction == "support":
            touches = _distinct(index for index, candle in enumerate(candles) if candle.low <= upper and candle.close >= lower)
            invalid = any(candle.close < lower for candle in candles[(touches[-1] + 1 if touches else 0):])
            eligible = level <= candles[-1].close
        else:
            touches = _distinct(index for index, candle in enumerate(candles) if candle.high >= lower and candle.close <= upper)
            invalid = any(candle.close > upper for candle in candles[(touches[-1] + 1 if touches else 0):])
            eligible = level >= candles[-1].close
        if len(touches) >= 2 and not invalid and eligible:
            candidates.append({
                "state": "AVAILABLE", "confirmation": "CONFIRMED", "rule": "v1_2_swing_2_distinct_reactions",
                "lower": round(lower, 4), "upper": round(upper, 4), "level": round(level, 4),
                "reaction_timestamps": [candles[index].timestamp.isoformat() for index in touches],
                "reaction_indices": touches,
            })
    if not candidates:
        return {"state": "UNAVAILABLE", "confirmation": "UNCONFIRMED", "reason": "No active zone with two distinct confirmed reactions", "rule": "v1_2_swing_2_distinct_reactions"}
    selected = (max(candidates, key=lambda item: item["level"]) if direction == "support"
                else min(candidates, key=lambda item: item["level"]))
    selected.pop("reaction_indices")
    return selected


def _movement(candles: list[Candle]) -> dict[str, Any]:
    first, last = candles[0], candles[-1]
    points = last.close - first.open
    percent = points / first.open * 100 if first.open else None
    if abs(percent or 0) < 0.2:
        label = "RANGE"
    elif points > 0:
        label = "ADVANCE"
    else:
        label = "DECLINE"
    extreme = max(candles, key=lambda candle: abs(candle.close - first.open))
    return {
        "label": label,
        "points": round(points, 4), "percent": round(percent, 6) if percent is not None else None,
        "explanation": f"{label.title()} from {first.open:.4f} at {first.timestamp.isoformat()} to {last.close:.4f} at {last.timestamp.isoformat()}; largest close displacement was {extreme.close:.4f} at {extreme.timestamp.isoformat()}.",
        "key_observations": [{"timestamp": extreme.timestamp.isoformat(), "close": extreme.close}],
    }


def _volume_context(candles: list[Candle], support: dict[str, Any], movement: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"schema_version": SCHEMA_VERSION, "movement": movement}
    reactions = support.get("reaction_timestamps", [])
    if not reactions:
        result.update({"state": "UNAVAILABLE", "reason": "No qualifying confirmed support bounce"})
        return result
    timestamp = datetime.fromisoformat(reactions[-1])
    index = next(index for index, candle in enumerate(candles) if candle.timestamp == timestamp)
    prior = candles[max(0, index - 5):index]
    if len(prior) < 5:
        result.update({"state": "UNAVAILABLE", "reason": "Fewer than five completed candles precede the qualifying bounce"})
        return result
    median = statistics.median(candle.volume for candle in prior)
    result.update({
        "state": "AVAILABLE", "bounce_timestamp": candles[index].timestamp.isoformat(),
        "bounce_volume": candles[index].volume, "prior_five_median_volume": median,
        "comparison": "ABOVE_MEDIAN" if candles[index].volume > median else "AT_OR_BELOW_MEDIAN",
    })
    return result


def _comparison(stock: list[Candle], nifty: list[Candle], support: dict[str, Any]) -> dict[str, Any]:
    stock_points = stock[-1].close - stock[0].open
    nifty_points = nifty[-1].close - nifty[0].open
    stock_pct = stock_points / stock[0].open * 100
    nifty_pct = nifty_points / nifty[0].open * 100
    relation = "SAME_DIRECTION" if stock_points * nifty_points > 0 else "DIVERGENT" if stock_points * nifty_points < 0 else "FLAT_BENCHMARK_OR_STOCK"
    moment = (support.get("reaction_timestamps") or [stock[-1].timestamp.isoformat()])[-1]
    return {
        "schema_version": SCHEMA_VERSION, "state": "AVAILABLE", "benchmark_source": "nifty.nifty_prices",
        "benchmark_status": "UNVERIFIED", "reason": "NIFTY series has aligned candles but no retained NFO contract identity or expiry.",
        "interval": {"start": stock[0].timestamp.isoformat(), "end": stock[-1].timestamp.isoformat()},
        "stock": {"open": stock[0].open, "close": stock[-1].close, "points": round(stock_points, 4), "percent": round(stock_pct, 6)},
        "benchmark": {"open": nifty[0].open, "close": nifty[-1].close, "points": round(nifty_points, 4), "percent": round(nifty_pct, 6)},
        "relationship": relation, "meaningful_moment_timestamp": moment,
        "explanation": f"Stock moved {stock_pct:.3f}% and the aligned benchmark moved {nifty_pct:.3f}% ({relation}).",
    }


def _watch(support: dict[str, Any], resistance: dict[str, Any], last_close: float) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "support_scenario": ({"state": "AVAILABLE", "zone": {"lower": support["lower"], "upper": support["upper"]}, "condition": "Hold above the support-zone lower bound for one completed five-minute candle."} if support.get("state") == "AVAILABLE" else {"state": "UNAVAILABLE", "reason": support.get("reason")}),
        "resistance_scenario": ({"state": "AVAILABLE", "zone": {"lower": resistance["lower"], "upper": resistance["upper"]}, "condition": "Close above the resistance-zone upper bound on a completed five-minute candle."} if resistance.get("state") == "AVAILABLE" else {"state": "AVAILABLE", "price": last_close, "condition": "Wait for a completed five-minute breakout above the session close."}),
    }


def build_report(symbol: str, trade_date: date, revision: str) -> tuple[dict[str, Any], str]:
    stock, coverage = read_stock_from_git(symbol, trade_date, revision)
    nifty = read_nifty(trade_date)
    support = _zone(stock, "support")
    resistance = _zone(stock, "resistance")
    movement = _movement(stock)
    volume = _volume_context(stock, support, movement)
    comparison = _comparison(stock, nifty, support)
    watch = _watch(support, resistance, stock[-1].close)
    coverage["header"] = {"exchange": "NSE", "symbol": symbol.upper(), "trade_date": trade_date.isoformat(), "candle_interval_minutes": 5, "timezone": "Asia/Kolkata"}
    benchmark_coverage = {"schema_version": SCHEMA_VERSION, "source": "nifty.nifty_prices", "candle_count": len(nifty), "interval_minutes": 5, "timezone": "Asia/Kolkata", "first_timestamp": nifty[0].timestamp.isoformat(), "last_timestamp": nifty[-1].timestamp.isoformat(), "contract_identity": None, "contract_expiry": None}
    markdown = "\n".join([
        f"# NSE {symbol.upper()} — {trade_date.isoformat()}", "",
        "## Session prices", f"Open {stock[0].open:.4f} | High {max(c.high for c in stock):.4f} | Low {min(c.low for c in stock):.4f} | Close {stock[-1].close:.4f}", "",
        "## How the stock moved", movement["explanation"], "",
        "## Nearest support and resistance", f"Support: {json.dumps(support, sort_keys=True)}", f"Resistance: {json.dumps(resistance, sort_keys=True)}", "",
        "## Stock vs NIFTY futures", comparison["explanation"], f"Benchmark status: {comparison['benchmark_status']} — {comparison['reason']}", "",
        "## Volume at support", json.dumps(volume, sort_keys=True), "",
        "## What to watch next session", json.dumps(watch, sort_keys=True),
    ])
    payload = {"exchange": "NSE", "symbol": symbol.upper(), "trade_date": trade_date, "analysis_version": SCHEMA_VERSION, "status": "COMPLETE", "report_markdown": markdown, "session_open": stock[0].open, "session_high": max(c.high for c in stock), "session_low": min(c.low for c in stock), "session_close": stock[-1].close, "stock_move_points": movement["points"], "stock_move_percent": movement["percent"], "support_json": support, "resistance_json": resistance, "volume_context_json": volume, "benchmark_status": "UNVERIFIED", "benchmark_identity": None, "benchmark_contract": None, "benchmark_expiry": None, "comparison_json": comparison, "next_session_watch_json": watch, "stock_coverage_json": coverage, "benchmark_coverage_json": benchmark_coverage, "generated_at": datetime.now(IST).replace(tzinfo=None)}
    return payload, markdown


def _json_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: json.dumps(value, sort_keys=True) if isinstance(value, dict) else value for key, value in payload.items()}


def save_or_read(payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    engine = connect_db()
    key = {"exchange": payload["exchange"], "symbol": payload["symbol"], "trade_date": payload["trade_date"]}
    try:
        with engine.begin() as conn:
            existing = conn.execute(text("SELECT * FROM stock_review_daily WHERE exchange=:exchange AND symbol=:symbol AND trade_date=:trade_date FOR UPDATE"), key).mappings().first()
            if existing:
                return dict(existing), False
            fields = _json_fields(payload)
            columns = ", ".join(fields)
            values = ", ".join(f":{field}" for field in fields)
            conn.execute(text(f"INSERT INTO stock_review_daily ({columns}) VALUES ({values})"), fields)
    except IntegrityError:
        with engine.connect() as conn:
            existing = conn.execute(text("SELECT * FROM stock_review_daily WHERE exchange=:exchange AND symbol=:symbol AND trade_date=:trade_date"), key).mappings().one()
            return dict(existing), False
    finally:
        engine.dispose()
    with connect_db().connect() as conn:
        saved = conn.execute(text("SELECT * FROM stock_review_daily WHERE exchange=:exchange AND symbol=:symbol AND trade_date=:trade_date"), key).mappings().one()
    return dict(saved), True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--trade-date", required=True, type=parse_date)
    parser.add_argument("--git-revision", required=True)
    parser.add_argument("--save", action="store_true", help="Persist if no report already exists for the key")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        revision = resolve_revision(args.git_revision)
        payload, markdown = build_report(args.symbol, args.trade_date, revision)
        print(markdown)
        if not args.save:
            print(json.dumps({"preview": True, "stock_coverage": payload["stock_coverage_json"], "benchmark_coverage": payload["benchmark_coverage_json"]}, default=str, sort_keys=True))
            return 0
        saved, inserted = save_or_read(payload)
        print(json.dumps({"saved": inserted, "id": saved["id"], "exchange": saved["exchange"], "symbol": saved["symbol"], "trade_date": str(saved["trade_date"])}, default=str, sort_keys=True))
        return 0
    except InputError as exc:
        print(f"input validation failed: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"stock review failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
