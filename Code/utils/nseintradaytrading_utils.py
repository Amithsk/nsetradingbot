#Title: NSE Intraday Trading Utilities - Signal Price Enrichment and Liquidity Metrics
#signal_price_enrichment 
#liquidity_metrics->Add liquidity metrics computation and filtering functions to the signal generation
#dymanic liquiduty->Added function for dynamic liquidity ranking and filtering

"""
signal_price_enrichment.py

Functions to compute ATR and populate entry_price, stop_price, target_price
for momentum_topn and gap_follow signals using only intraday_bhavcopy data.

Assumptions / conventions:
- For signals with entry_model == 'next_open', entry_date = trade_date + 1 trading day.
- ATR is computed using past days up to (and including) trade_date - 1 (i.e., not using the entry day).
- If insufficient history for configured lookback, uses min_periods = max(3, lookback//2).
- If ATR still not computable, fields are left NaN and an 'imputed' flag is not set.
- For gap_follow we prefer stop relative to prev_close (below for LONG, above for SHORT).
"""

import logging
from datetime import timedelta,datetime
import pandas as pd
import numpy as np
from sqlalchemy import text
import sqlalchemy as sa
from Code.utils.nseintraday_db_utils import detect_intraday_columns
import json
from typing import Optional, Set, Iterable
from sqlalchemy.engine import Engine
from sqlalchemy import text
from functools import lru_cache



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def compute_true_range(df):
    """
    df must contain columns: high, low, prev_close (prev_close may be NaN)
    returns a Series of TR aligned with df index
    TR = max(high - low, abs(high - prev_close), abs(low - prev_close))
    """
    high = df['high']
    low = df['low']
    prev = df['prev_close']
    tr1 = high - low
    tr2 = (high - prev).abs()
    tr3 = (low - prev).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr


def fetch_bhavcopy_history(engine, symbols, end_date, lookback_days):
    """
    Fetch bhavcopy rows for `symbols` up to and including end_date (YYYY-MM-DD or pd.Timestamp)
    Returns DataFrame with columns: symbol, trade_date, open, high, low, close, prev_close
    - prev_close is computed per symbol from prior close (shifted).
    """
    if isinstance(end_date, pd.Timestamp):
        end_date = end_date.date()
    # compute window start date to get lookback history
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=lookback_days * 2)).strftime("%Y-%m-%d")
    end_date_s = pd.to_datetime(end_date).strftime("%Y-%m-%d")
    syms = "', '".join([str(s) for s in set(symbols)])
    q = f"""
    SELECT symbol, trade_date, `open`, `high`, `low`, `close`
    FROM intraday_bhavcopy
    WHERE symbol IN ('{syms}')
      AND DATE(trade_date) BETWEEN '{start_date}' AND '{end_date_s}'
    ORDER BY symbol, trade_date;
    """
    df = pd.read_sql(text(q), engine)
    if df.empty:
        return df
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()
    df = df.sort_values(['symbol', 'trade_date'])
    # compute prev_close per symbol
    df['prev_close'] = df.groupby('symbol')['close'].shift(1)
    return df


def compute_atr_for_symbols(engine, symbols, ref_date, atr_lookback=14, min_periods=None):
    """
    Compute ATR values for the set of symbols ending at ref_date - 1 (i.e., history up to prev day).
    Returns DataFrame indexed by symbol with column 'atr'.
    """
    if min_periods is None:
        min_periods = max(3, atr_lookback // 2)

    # fetch history; get at least atr_lookback + 2 days because we need prev_close
    history = fetch_bhavcopy_history(engine, symbols, ref_date - pd.Timedelta(days=1), lookback_days=atr_lookback + 5)
    if history.empty:
        return pd.DataFrame(columns=['symbol', 'atr']).set_index('symbol')

    # compute true range and ATR per symbol
    outs = []
    for sym, g in history.groupby('symbol'):
        # restrict to rows <= ref_date - 1
        cutoff = (pd.to_datetime(ref_date) - pd.Timedelta(days=1)).normalize()
        g = g[g['trade_date'] <= cutoff].copy()
        if g.empty:
            outs.append({'symbol': sym, 'atr': np.nan})
            continue
        # compute TR and rolling ATR
        tr = compute_true_range(g)
        # rolling mean with min_periods
        atr_series = tr.rolling(window=atr_lookback, min_periods=min_periods).mean()
        atr_val = atr_series.iloc[-1] if len(atr_series) > 0 else np.nan
        # fallback: if last is nan and min_periods < atr_lookback maybe pick last valid
        if pd.isna(atr_val):
            attright = atr_series.dropna()
            atr_val = attright.iloc[-1] if not attright.empty else np.nan
        outs.append({'symbol': sym, 'atr': float(atr_val) if pd.notna(atr_val) else np.nan})
    df_atr = pd.DataFrame(outs).set_index('symbol')
    return df_atr


def enrich_signals_with_stops_targets(engine, df_signals, ref_date=None,
                                      atr_lookback=14, atr_stop_mult=1.5, atr_target_mult=3.0,
                                      gap_lookback_high_days=20,
                                      pct_stop_fallback=0.005, pct_target_fallback=0.01):
    """
    Enrich df_signals with entry_price (if possible), stop_price and target_price.

    Behavior:
      - Handles strategies 'momentum_topn' and 'gap_follow' (others left with notes).
      - Prefetches ATR from strategy_features (feature_name = f"atr_{atr_lookback}").
      - Prefetches bhav open/close/prev_close for trade_date and trade_date+1 for entry open resolution.
      - Uses ATR-based multipliers when ATR available; otherwise uses percent-based fallback if entry_price or close exists.
      - Does NOT overwrite existing non-null stop_price/target_price; sets *_imputed flags for filled fields.
      - Returns a new DataFrame (copy) with enrichment columns and 'price_enrichment_notes' containing per-row details.
    """
    
    try:
        logger  # prefer module-level logger if present
    except Exception:
        logger = None

    df = df_signals.copy()
    if df.empty:
        return df

    # Normalise and ensure datetime columns
    if 'trade_date' not in df.columns:
        raise ValueError("df_signals must contain 'trade_date' column")
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()

    # Default ref_date is max trade_date if not supplied
    if ref_date is None:
        ref_date = df['trade_date'].max()

    # compute entry_date per-row (next_open -> trade_date + 1)
    def compute_entry_date(row):
        em = str(row.get('entry_model') or '').lower()
        if em == 'next_open' or em == '':
            return (row['trade_date'] + pd.Timedelta(days=1)).normalize()
        else:
            return row['trade_date']
    df['entry_date'] = df.apply(compute_entry_date, axis=1)

    # Prepare imputed flags
    df['stop_price_imputed'] = False
    df['target_price_imputed'] = False
    df['entry_price_imputed'] = False

    # Pre-fetch ATR and BHAV rows for all relevant symbols/dates
    # Normalize symbol strings
    df['symbol'] = df['symbol'].astype(str).str.strip().str.upper()
    symbol_list = sorted(df['symbol'].unique().tolist())
    trade_dates = sorted(df['trade_date'].dt.date.astype(str).unique().tolist())

    # Build ATR lookup: key = (symbol, date) -> float(atr)
    atr_map = {}
    if symbol_list and trade_dates:
        try:
            atr_sql = text("""
                SELECT symbol, trade_date, value
                FROM strategy_features
                WHERE feature_name = :feat
                  AND symbol IN :symbols
                  AND trade_date IN :dates
            """).bindparams(
                sa.bindparam("symbols", expanding=True),
                sa.bindparam("dates", expanding=True)
            )
            df_atr = pd.read_sql(atr_sql, engine, params={"feat": f"atr_{atr_lookback}", "symbols": symbol_list, "dates": trade_dates})
            if not df_atr.empty:
                df_atr['symbol'] = df_atr['symbol'].astype(str).str.strip().str.upper()
                df_atr['trade_date'] = pd.to_datetime(df_atr['trade_date']).dt.date
                df_atr['value'] = pd.to_numeric(df_atr['value'], errors='coerce')
                atr_map = {(r['symbol'], r['trade_date']): float(r['value']) for _, r in df_atr.iterrows() if pd.notna(r['value'])}
        except Exception as e:
            if logger:
                logger.exception("Failed to prefetch ATR rows: %s", e)
            else:
                print("Failed to prefetch ATR rows:", e)
            atr_map = {}

    # Build bhav lookup for trade_date and trade_date + 1
    lookup_dates = set(trade_dates)
    # include trade_date+1 as potential entry_date
    try:
        lookup_dates.update([(pd.to_datetime(d).date() + pd.Timedelta(days=1)).isoformat() for d in trade_dates])
    except Exception:
        # if any conversion fails, keep it simple
        pass
    lookup_dates = sorted(list(lookup_dates))

    bhav_open_map = {}
    bhav_close_map = {}
    bhav_prev_close_map = {}
    if symbol_list and lookup_dates:
        try:
            bhav_sql = text("""
                SELECT symbol, trade_date, open, close, prev_close
                FROM intraday_bhavcopy
                WHERE symbol IN :symbols
                  AND trade_date IN :dates
            """).bindparams(
                sa.bindparam("symbols", expanding=True),
                sa.bindparam("dates", expanding=True)
            )
            df_bhav = pd.read_sql(bhav_sql, engine, params={"symbols": symbol_list, "dates": lookup_dates})
            if not df_bhav.empty:
                df_bhav['symbol'] = df_bhav['symbol'].astype(str).str.strip().str.upper()
                df_bhav['trade_date'] = pd.to_datetime(df_bhav['trade_date']).dt.date
                for _, r in df_bhav.iterrows():
                    key = (r['symbol'], r['trade_date'])
                    open_v = r.get('open', None)
                    close_v = r.get('close', None)
                    prev_close_v = r.get('prev_close', None) if 'prev_close' in r else None
                    bhav_open_map[key] = float(open_v) if pd.notna(open_v) else None
                    bhav_close_map[key] = float(close_v) if pd.notna(close_v) else None
                    bhav_prev_close_map[key] = float(prev_close_v) if prev_close_v is not None and pd.notna(prev_close_v) else None
        except Exception as e:
            if logger:
                logger.exception("Failed to prefetch bhav rows for enricher: %s", e)
            else:
                print("Failed to prefetch bhav rows for enricher:", e)
            # leave maps empty; function will fallback to percent bands where possible

    # Iterate and compute per-row
    enrichment_notes = []
    for idx, row in df.iterrows():
        sym = str(row['symbol']).strip().upper()
        trade_date = pd.to_datetime(row['trade_date']).date()
        entry_date = pd.to_datetime(row['entry_date']).date() if not pd.isna(row['entry_date']) else trade_date
        strategy = str(row.get('strategy') or '').lower()
        score = row.get('signal_score', None)
        entry_price = row.get('entry_price', None)
        stop_price = row.get('stop_price', None)
        target_price = row.get('target_price', None)
        note = {}

        # grab ATR from prefetch map
        atr_val = atr_map.get((sym, trade_date), np.nan)
        note['atr'] = float(atr_val) if not pd.isna(atr_val) else None

        # Resolve entry_price if missing: prefer bhav open at entry_date -> fallback to trade_date close
        if pd.isna(entry_price) or entry_price is None:
            open_v = bhav_open_map.get((sym, entry_date))
            if open_v is not None:
                entry_price = float(open_v)
                df.at[idx, 'entry_price_imputed'] = True
                note['entry_price_from'] = 'entry_open'
            else:
                close_v = bhav_close_map.get((sym, trade_date))
                if close_v is not None:
                    entry_price = float(close_v)
                    df.at[idx, 'entry_price_imputed'] = True
                    note['entry_price_from'] = 'trade_close_fallback'
                else:
                    entry_price = None
                    note['entry_price_from'] = None

        # Strategy-specific computations
        if strategy == 'momentum_topn':
            # Determine side from signal_score (default LONG)
            try:
                s_score = float(score) if score is not None else None
                side = 'LONG' if (s_score is None or s_score >= 0) else 'SHORT'
            except Exception:
                side = 'LONG'
            note['computed_side'] = side

            # ATR-based
            if (not pd.isna(atr_val)) and (entry_price is not None):
                if side == 'LONG':
                    computed_stop = entry_price - (atr_stop_mult * atr_val)
                    computed_target = entry_price + (atr_target_mult * atr_val)
                else:
                    computed_stop = entry_price + (atr_stop_mult * atr_val)
                    computed_target = entry_price - (atr_target_mult * atr_val)
                # Only set if missing
                if pd.isna(stop_price) or stop_price is None or float(stop_price) == 0.0:
                    stop_price = float(np.round(computed_stop, 4))
                    df.at[idx, 'stop_price_imputed'] = True
                    note['stop_from'] = 'atr'
                if pd.isna(target_price) or target_price is None or float(target_price) == 0.0:
                    target_price = float(np.round(computed_target, 4))
                    df.at[idx, 'target_price_imputed'] = True
                    note['target_from'] = 'atr'
            else:
                # Percent fallback if entry_price exists
                if entry_price is not None:
                    if side == 'LONG':
                        computed_stop = entry_price * (1.0 - pct_stop_fallback)
                        computed_target = entry_price * (1.0 + pct_target_fallback)
                    else:
                        computed_stop = entry_price * (1.0 + pct_stop_fallback)
                        computed_target = entry_price * (1.0 - pct_target_fallback)
                    if pd.isna(stop_price) or stop_price is None or float(stop_price) == 0.0:
                        stop_price = float(np.round(computed_stop, 4))
                        df.at[idx, 'stop_price_imputed'] = True
                        note['stop_from'] = 'percent_fallback'
                    if pd.isna(target_price) or target_price is None or float(target_price) == 0.0:
                        target_price = float(np.round(computed_target, 4))
                        df.at[idx, 'target_price_imputed'] = True
                        note['target_from'] = 'percent_fallback'
                else:
                    note['momentum'] = 'atr_and_entry_missing'

        elif strategy == 'gap_follow':
            # Determine side: try to parse gap_pct from notes JSON if present, otherwise use signal_score
            side = None
            notes_raw = row.get('notes') or ''
            try:
                parsed = json.loads(notes_raw) if isinstance(notes_raw, str) and notes_raw.strip().startswith('{') else {}
                gap_pct = parsed.get('gap_pct')
                if gap_pct is not None:
                    side = 'LONG' if float(gap_pct) > 0 else 'SHORT'
            except Exception:
                # ignore parse errors
                side = None
            if side is None:
                try:
                    sc = float(score) if score is not None else None
                    side = 'LONG' if (sc is None or sc >= 0) else 'SHORT'
                except Exception:
                    side = 'LONG'
            note['computed_side'] = side
            # prefer prev_close-based stop if prev_close exists
            prev_close = bhav_prev_close_map.get((sym, trade_date))
            # ATR-based route
            if (not pd.isna(atr_val)) and (entry_price is not None):
                if side == 'LONG':
                    if prev_close is not None:
                        computed_stop = float(prev_close) - (atr_stop_mult * atr_val)
                    else:
                        computed_stop = entry_price - (atr_stop_mult * atr_val)
                    computed_target = entry_price + (atr_target_mult * atr_val)
                else:
                    if prev_close is not None:
                        computed_stop = float(prev_close) + (atr_stop_mult * atr_val)
                    else:
                        computed_stop = entry_price + (atr_stop_mult * atr_val)
                    computed_target = entry_price - (atr_target_mult * atr_val)
                if pd.isna(stop_price) or stop_price is None or float(stop_price) == 0.0:
                    stop_price = float(np.round(computed_stop, 4))
                    df.at[idx, 'stop_price_imputed'] = True
                    note['stop_from'] = 'atr'
                if pd.isna(target_price) or target_price is None or float(target_price) == 0.0:
                    target_price = float(np.round(computed_target, 4))
                    df.at[idx, 'target_price_imputed'] = True
                    note['target_from'] = 'atr'
            else:
                # percent fallback if entry_price exists
                if entry_price is not None:
                    if side == 'LONG':
                        computed_stop = entry_price * (1.0 - pct_stop_fallback)
                        computed_target = entry_price * (1.0 + pct_target_fallback)
                    else:
                        computed_stop = entry_price * (1.0 + pct_stop_fallback)
                        computed_target = entry_price * (1.0 - pct_target_fallback)
                    if pd.isna(stop_price) or stop_price is None or float(stop_price) == 0.0:
                        stop_price = float(np.round(computed_stop, 4))
                        df.at[idx, 'stop_price_imputed'] = True
                        note['stop_from'] = 'percent_fallback'
                    if pd.isna(target_price) or target_price is None or float(target_price) == 0.0:
                        target_price = float(np.round(computed_target, 4))
                        df.at[idx, 'target_price_imputed'] = True
                        note['target_from'] = 'percent_fallback'
                else:
                    note['gap_follow'] = 'atr_and_entry_missing'
        else:
            note['info'] = 'strategy_not_handled_by_enricher'

        # Write computed (or preserved) values back into df copy (only replace when value computed / imputed)
        if stop_price is not None:
            df.at[idx, 'stop_price'] = stop_price
        if target_price is not None:
            df.at[idx, 'target_price'] = target_price
        if entry_price is not None:
            df.at[idx, 'entry_price'] = entry_price

        # collect note
        enrichment_notes.append({'index': int(idx), 'symbol': sym, 'notes': note})

    # Add JSON notes column
    try:
        df['price_enrichment_notes'] = df.index.map(lambda i: next((n['notes'] for n in enrichment_notes if n['index'] == i), {}))
    except Exception:
        df['price_enrichment_notes'] = None

    # Logging summary
    try:
        total = len(enrichment_notes)
        atr_used = sum(1 for n in enrichment_notes if n['notes'].get('atr') is not None)
        percent_used = sum(1 for n in enrichment_notes if n['notes'].get('stop_from') == 'percent_fallback' or n['notes'].get('target_from') == 'percent_fallback')
        entry_imputed = int(df['entry_price_imputed'].sum()) if 'entry_price_imputed' in df.columns else 0
        msg = f"Enrichment summary - total={total}, atr_available={atr_used}, percent_fallback_used={percent_used}, entry_imputed={entry_imputed}"
        if logger:
            logger.info(msg)
        else:
            print(msg)
    except Exception:
        pass

    return df

def _read_sql_in_batches(engine, base_query_template, date_params, symbols, batch_size=800):
    """
    Helper to execute a parametrized query in batches to avoid huge IN(...) clauses
    base_query_template must contain a "{placeholders}" token where the IN-list goes.
    date_params: list of positional params that go before the symbol list (e.g. [start_date, end_date])
    symbols: list of symbol strings
    """
    import pandas as pd

    parts = []
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        placeholders = ", ".join(["%s"] * len(batch))
        q = base_query_template.format(placeholders=placeholders)
        params = tuple(date_params + batch)
        parts.append(pd.read_sql(q, engine, params=params))
    if parts:
        return pd.concat(parts, ignore_index=True, sort=False)
    return pd.DataFrame()




# ---------------------------------------------------------------
# 1) compute_liquidity_metrics
# ---------------------------------------------------------------


def compute_liquidity_metrics(engine, symbols, trade_date, lookback_days=20, batch_size=800):
    """
    MySQL-safe, batch-capable computation of liquidity metrics.

    Returns DataFrame with columns:
      symbol, trade_date, net_trdval, net_trdqty, trades, close,
      avg_20d_vol, turnover_rank_pct, turnover_norm, vol_norm, fno_flag, notes
    """
    
    # Safety: empty symbol list
    if not symbols:
        return pd.DataFrame(
            columns=[
                "symbol", "trade_date", "net_trdval", "net_trdqty", "trades", "close",
                "avg_20d_vol", "turnover_rank_pct", "turnover_norm", "vol_norm",
                "fno_flag", "notes"
            ]
        )

    # Detect canonical column names in intraday_bhavcopy
    colmap = detect_intraday_columns(engine)
    sym_col = colmap["symbol"]
    date_col = colmap["trade_date"]
    netval_col = colmap["net_trdval"]
    netqty_col = colmap["net_trdqty"]
    trades_col = colmap["trades"]

    # Normalize trade_date to date
    if isinstance(trade_date, str):
        trade_date_dt = datetime.strptime(trade_date, "%Y-%m-%d").date()
    else:
        trade_date_dt = trade_date

    # ---------------------------
    # 1) Query today's bhavcopy (batched)
    # ---------------------------
    base_query_today = f"""
    SELECT {sym_col} AS symbol, trade_date, net_trdval, net_trdqty, trades, close
    FROM intraday_bhavcopy
    WHERE {date_col} = %s
      AND mkt_flag = 0 AND ind_sec = 'N'
      AND {sym_col} IN ({{placeholders}})
"""

   

    # Use batch helper to avoid huge IN lists
    df_today = _read_sql_in_batches(
        engine,
        base_query_today,
        date_params=[trade_date_dt],
        symbols=symbols,
        batch_size=batch_size
    )

    # If empty, return a dataframe with placeholders for all requested symbols
    if df_today.empty:
        out = pd.DataFrame({"symbol": symbols})
        out["trade_date"] = trade_date_dt
        out["net_trdval"] = np.nan
        out["net_trdqty"] = np.nan
        out["trades"] = np.nan
        out["close"] = np.nan
        out["avg_20d_vol"] = np.nan
        out["turnover_rank_pct"] = np.nan
        out["turnover_norm"] = np.nan
        out["vol_norm"] = np.nan
        out["fno_flag"] = False
        out["notes"] = None
        return out

    # ---------------------------
    # 2) Query 20-day avg volume (batched)
    # ---------------------------
    start_date = trade_date_dt - timedelta(days=lookback_days * 2)  # expanded window to capture 20 trading days
    base_query_vol = f"""
        SELECT {sym_col} AS symbol,
               AVG({netqty_col}) AS avg_20d_vol
        FROM intraday_bhavcopy
        WHERE {date_col} BETWEEN %s AND %s
          AND {sym_col} IN ({{placeholders}})
        GROUP BY {sym_col}
    """

    df_vol = _read_sql_in_batches(
        engine,
        base_query_vol,
        date_params=[start_date, trade_date_dt],
        symbols=symbols,
        batch_size=batch_size
    )

    # Merge today's bhavcopy and avg volume
    df = df_today.merge(df_vol, on="symbol", how="left")

    # Fill missing avg_20d_vol with today's qty
    df["avg_20d_vol"] = df["avg_20d_vol"].fillna(df["net_trdqty"])

    # ---------------------------
    # 3) Normalization & ranks
    # ---------------------------

    # Percentile rank for turnover
    if df["net_trdval"].notna().sum() > 0:
        df["turnover_rank_pct"] = df["net_trdval"].rank(pct=True)
    else:
        df["turnover_rank_pct"] = np.nan

    # Turnover normalized (0–1)
    max_turnover = df["net_trdval"].max()
    if pd.notna(max_turnover) and max_turnover > 0:
        df["turnover_norm"] = df["net_trdval"] / max_turnover
    else:
        df["turnover_norm"] = np.nan

    # Volume normalized (0–1)
    max_vol = df["avg_20d_vol"].max()
    if pd.notna(max_vol) and max_vol > 0:
        df["vol_norm"] = df["avg_20d_vol"] / max_vol
    else:
        df["vol_norm"] = np.nan

    # Placeholder for F&O membership and notes
    df["fno_flag"] = False
    df["notes"] = None

    # Ensure output columns order for consumers
    out_cols = [
        "symbol", "trade_date", "net_trdval", "net_trdqty", "trades", "close",
        "avg_20d_vol", "turnover_rank_pct", "turnover_norm", "vol_norm",
        "fno_flag", "notes"
    ]
    return df[out_cols].reset_index(drop=True)


# ---------------------------
# Dynamic Liquidity Engine 
# ---------------------------

_logger = logging.getLogger(__name__)

def compute_liquidity_ranks(df):
    """
    Add percentile ranks:
      - turnover_rank_pct_today : rank of net_trdval (0..1)
      - A20_rank_pct : rank of avg_20d_trdval (0..1)
    Returns modified df.
    """
    if df is None or df.empty:
        return df

    # safe creation
    if "net_trdval" not in df.columns:
        df["turnover_rank_pct_today"] = np.nan
    else:
        # higher turnover -> higher rank (pct)
        df["turnover_rank_pct_today"] = df["net_trdval"].rank(pct=True, method="average")

    if "avg_20d_trdval" not in df.columns:
        df["A20_rank_pct"] = np.nan
    else:
        df["A20_rank_pct"] = df["avg_20d_trdval"].rank(pct=True, method="average")

    return df


def apply_liquidity_filters(df, rules, mode="tag_only"):
    """
    Hybrid percentile + hard-floor liquidity filter.

    rules keys used:
      - dynamic_percentile_keep (float 0..1)
      - hard_min_A20 (int)
      - hard_min_vol20 (int)
      - hard_min_trades (int)
      - hard_min_price (float)
      - use_today_percentile (bool)

    mode:
      - "tag_only": annotate liquidity_pass but keep all rows
      - "enforce": drop rows where liquidity_pass == False

    Adds columns:
      pass_pct, pass_hard, liquidity_pass, liquidity_info (dict)
    Returns filtered/annotated df (copy if enforced).
    """
    if df is None:
        return df

    # defaults
    pct_keep = float(rules.get("dynamic_percentile_keep", 0.30))
    hard_min_A20 = int(rules.get("hard_min_A20", int(rules.get("hard_min_net_trdval", 200000))))
    hard_min_vol20 = int(rules.get("hard_min_vol20", int(rules.get("hard_min_net_trdqty", 20000))))
    hard_min_trades = int(rules.get("hard_min_trades", 50))
    hard_min_price = float(rules.get("hard_min_price", float(rules.get("min_price", 20.0))))
    use_today_pct = bool(rules.get("use_today_percentile", True))

    # ensure ranks exist
    rank_col = "turnover_rank_pct_today" if use_today_pct else "A20_rank_pct"
    if rank_col not in df.columns:
        df = compute_liquidity_ranks(df)

    # compute cutoff: keep top pct_keep fraction
    # since rank pct is 0..1 where larger is better, compute quantile of (1 - pct_keep)
    try:
        cutoff = df[rank_col].quantile(1.0 - pct_keep)
    except Exception:
        cutoff = 1.0 - pct_keep

    df["pass_pct"] = df[rank_col].fillna(0.0) >= cutoff

  
    # hard floors (safety checks)
    if "avg_20d_trdval" in df.columns:
        pass_A20 = df["avg_20d_trdval"].fillna(0) >= hard_min_A20
    else:
        pass_A20 = pd.Series([False] * len(df), index=df.index)

    if "avg_20d_vol" in df.columns:
        pass_vol = df["avg_20d_vol"].fillna(0) >= hard_min_vol20
    else:
        pass_vol = pd.Series([False] * len(df), index=df.index)

    if "trades" in df.columns:
        pass_trades = df["trades"].fillna(0) >= hard_min_trades
    else:
        pass_trades = pd.Series([False] * len(df), index=df.index)

    if "close" in df.columns:
        pass_price = df["close"].fillna(0) >= hard_min_price
    else:
        pass_price = pd.Series([False] * len(df), index=df.index)

    # combine all hard-floor checks
    df["pass_hard"] = pass_A20 & pass_vol & pass_trades & pass_price

   

    # final pass flag
    df["liquidity_pass"] = df["pass_pct"] & df["pass_hard"]

    # ensure fno_flag exists
    if "fno_flag" not in df.columns:
        df["fno_flag"] = False

    # small structured summary for auditing (lightweight)
    def _liquidity_summary(r):
        return {
            "net_trdval": float(r.get("net_trdval") if pd.notna(r.get("net_trdval")) else 0.0),
            "avg_20d_trdval": float(r.get("avg_20d_trdval") if pd.notna(r.get("avg_20d_trdval")) else 0.0),
            "turnover_rank_pct_today": float(r.get("turnover_rank_pct_today") if pd.notna(r.get("turnover_rank_pct_today")) else 0.0),
            "liquidity_momentum": float(r.get("liquidity_momentum") if pd.notna(r.get("liquidity_momentum")) else 1.0),
            "liquidity_pass": bool(r.get("liquidity_pass"))
        }

    df["liquidity_info"] = df.apply(_liquidity_summary, axis=1)

    # mode action
    if mode == "enforce":
        before = len(df)
        df_filtered = df[df["liquidity_pass"] == True].copy()
        after = len(df_filtered)
        _logger.info("apply_liquidity_filters: enforce mode filtered %d -> %d", before, after)
        return df_filtered.reset_index(drop=True)

    # tag_only: just log and return annotated frame
    _logger.info("apply_liquidity_filters: tag_only total=%d passed=%d pass_pct=%.2f%%",
                 len(df), int(df["liquidity_pass"].sum()), float(0 if len(df)==0 else df["liquidity_pass"].mean()*100))
    return df.reset_index(drop=True)


def compute_combined_score(df, weights=None, fno_boost=False):
    """
    Compute a normalized combined score from momentum, turnover and volume.
    Safe to run when liquidity columns are missing — uses sensible fallbacks.

    Inputs:
      - df: DataFrame with (optionally) columns:
            'signal_score' (momentum/strategy score), 'avg_20d_trdval', 'net_trdval',
            'avg_20d_vol', 'fno_flag' (optional boolean/int)
      - weights: dict like {"mom": 0.6, "turnover": 0.3, "vol": 0.1}
      - fno_boost: False or numeric boost factor (>1.0). If numeric and fno_flag True, multiplies score.

    Returns:
      - DataFrame copy with 'combined_score' (float 0..inf, normalized on available data)
        and 'liquidity_info' (dict) columns added.
    """
    import numpy as np
    import pandas as pd

    if weights is None:
        weights = {"mom": 0.6, "turnover": 0.3, "vol": 0.1}

    # defensive copy
    df = df.copy()
    if df.empty:
        return df

    # helper to safely get a numeric Series (or NaN Series if missing)
    def _safe_series(name):
        if name in df.columns:
            return pd.to_numeric(df[name], errors="coerce")
        else:
            return pd.Series([np.nan] * len(df), index=df.index)

    # momentum: use absolute signal_score (so both LONG/SHORT comparable)
    mom_s = _safe_series("signal_score").abs().fillna(0.0)

    # turnover base: prefer avg_20d_trdval, fallback to net_trdval (today)
    a20_s = _safe_series("avg_20d_trdval")
    net_today_s = _safe_series("net_trdval")
    turn_base = a20_s.fillna(net_today_s.fillna(0.0)).fillna(0.0)

    # volume baseline
    vol_s = _safe_series("avg_20d_vol").fillna(0.0)

    # normalization helper (avoid div0)
    def _norm(series):
        mx = series.max(skipna=True)
        if pd.isna(mx) or float(mx) == 0.0:
            return series * 0.0
        return series / float(mx)

    mom_norm = _norm(mom_s)
    turn_norm = _norm(turn_base)
    vol_norm = _norm(vol_s)

    alpha = float(weights.get("mom", 0.6))
    beta = float(weights.get("turnover", 0.3))
    gamma = float(weights.get("vol", 0.1))

    df["combined_score"] = alpha * mom_norm + beta * turn_norm + gamma * vol_norm

    # apply FnO boost if requested and fno_flag present
    if fno_boost:
        # if fno_boost is a boolean True, treat as 1.0 (no-op). If numeric >1.0, apply that factor.
        try:
            boost_factor = float(fno_boost) if not isinstance(fno_boost, bool) else None
        except Exception:
            boost_factor = None

        if boost_factor and "fno_flag" in df.columns:
            # fno_flag may be bool/int/str — coerce safely
            fno_flag = _safe_series("fno_flag").fillna(0).astype(int).clip(0, 1)
            df["combined_score"] = df["combined_score"] * (1.0 + fno_flag * (boost_factor - 1.0))

    # create a compact liquidity_info dict for audit/tracing (safe casts)
    def _build_liq_info(row):
        def _as_float(v):
            try:
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    return None
                return float(v)
            except Exception:
                return None

        return {
            "avg_20d_trdval": _as_float(row.get("avg_20d_trdval")),
            "net_trdval": _as_float(row.get("net_trdval")),
            "avg_20d_vol": _as_float(row.get("avg_20d_vol")),
            "turnover_vs_20d": _as_float(row.get("turnover_vs_20d")),
            "liquidity_pass": bool(row.get("liquidity_pass")) if "liquidity_pass" in row else None,
            "fno_flag": True if row.get("fno_flag") in (1, True, "1", "true", "True") else False
        }

    df["liquidity_info"] = df.apply(_build_liq_info, axis=1)

    return df



def suggest_dynamic_percentile(df, target_count, min_pct=0.05, max_pct=0.50):
    """
    Given df (universe) and desired N target, return percentile p (0..1)
    such that approx p * universe_size ~= target_count. Clamped between min_pct and max_pct.
    """
    if df is None or df.empty:
        return 0.30
    universe_size = len(df)
    if universe_size <= 0:
        return 0.30
    p = float(target_count) / float(universe_size)
    p = max(min_pct, min(max_pct, p))
    return float(round(p, 2))


def build_liquidity_json(row):
    """
    Build serializable liquidity info JSON/dict for storing in params.
    """
    return {
        "net_trdval": float(row.get("net_trdval") if pd.notna(row.get("net_trdval")) else 0.0),
        "avg_20d_trdval": float(row.get("avg_20d_trdval") if pd.notna(row.get("avg_20d_trdval")) else 0.0),
        "turnover_rank_pct_today": float(row.get("turnover_rank_pct_today") if pd.notna(row.get("turnover_rank_pct_today")) else 0.0),
        "liquidity_momentum": float(row.get("liquidity_momentum") if pd.notna(row.get("liquidity_momentum")) else 1.0),
        "combined_score": float(row.get("combined_score") if pd.notna(row.get("combined_score")) else 0.0),
        "liquidity_pass": bool(row.get("liquidity_pass"))
    }


#To retrieve the symbol from the instrument master table for filtering purposes
logger = logging.getLogger(__name__)


def _normalize_symbols_iterable(iterable: Iterable[str]) -> Set[str]:
    """Normalize iterable of symbols to an uppercase stripped set."""
    return {str(s).strip().upper() for s in iterable if s is not None and str(s).strip() != ""}


def get_instruments_master_symbols(engine: Engine, symbol_column: str = "symbol") -> Set[str]:
    """
    Fetch all symbols from instruments_master and return as a normalized set.

    Args:
        engine: SQLAlchemy Engine connected to your DB.
        symbol_column: Column name for the symbol (default 'symbol').

    Returns:
        Set[str] of uppercase, stripped symbols. Empty set on error.
    """
    if engine is None:
        raise ValueError("engine is required")

    sql = text(f"SELECT {symbol_column} FROM instruments_master")
    try:
        df = pd.read_sql(sql, engine)
        if df.empty or symbol_column not in df.columns:
            return set()
        return _normalize_symbols_iterable(df[symbol_column].astype(str).tolist())
    except Exception as exc:
        logger.exception("get_instruments_master_symbols DB error: %s", exc)
        return set()


@lru_cache(maxsize=1)
def _get_instruments_master_symbols_cached(engine_url: str) -> Set[str]:
    """
    Internal helper to cache a symbols set per-engine URL string.
    Caller should pass engine.url.__to_string__() (or similar unique string) to cache per DB.
    WARNING: caching by engine URL requires you to pass a stable string key.
    """
    # The actual engine object is not cached here; caller must call get_instruments_master_symbols
    # and pass results into is_symbol_in_instruments_master via symbols_cache param.
    # This helper kept for completeness; see usage below for caching pattern.
    return set()


def is_symbol_in_instruments_master(
    symbol: Optional[str],
    engine: Optional[Engine] = None,
    symbols_cache: Optional[Set[str]] = None
) -> bool:
    """
    Check whether a symbol exists in instruments_master.

    Behavior:
      - If `symbols_cache` is provided, membership is checked against it (fast, recommended).
      - Else if `engine` provided, this will fetch symbols from DB (bulk) and check membership.
      - Otherwise raises ValueError.

    Args:
        symbol: symbol to check (case-insensitive).
        engine: optional SQLAlchemy Engine used to fetch symbols if no cache supplied.
        symbols_cache: optional pre-fetched set of symbols (preferred).

    Returns:
        True if symbol present, False otherwise.
    """
    if symbol is None:
        return False

    sym = str(symbol).strip().upper()
    if sym == "":
        return False

    if symbols_cache is not None:
        # Expect the cache to already be normalized uppercase set
        return sym in symbols_cache

    if engine is None:
        raise ValueError("Either symbols_cache or engine must be provided")

    try:
        # Bulk fetch once and check membership (avoids per-row DB calls)
        symbols = get_instruments_master_symbols(engine)
        return sym in symbols
    except Exception as exc:
        logger.exception("is_symbol_in_instruments_master error for %s: %s", sym, exc)
        return False
    

#End of Code retrieval code
