#Title: NSE Intraday Trading Utilities - Signal Price Enrichment and Liquidity Metrics
#signal_price_enrichment 
#liquidity_metrics->Add liquidity metrics computation and filtering functions to the signal generation

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
        SELECT {sym_col} AS symbol,
               {date_col} AS trade_date,
               {netval_col} AS net_trdval,
               {netqty_col} AS net_trdqty,
               {trades_col} AS trades,
               close
        FROM intraday_bhavcopy
        WHERE {date_col} = %s
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



# ---------------------------------------------------------------
# 2) apply_liquidity_filters
# ---------------------------------------------------------------
def apply_liquidity_filters(df_signals, liquidity_df, rules, mode="tag_only"):
    """
    Merges liquidity_df into df_signals and applies liquidity scoring + filtering.

    Returns a DataFrame with:
        liquidity_pass (bool),
        mom_norm,
        combined_score,
        liquidity_info (dict)

    mode = "tag_only" → keep all but annotate
    mode = "enforce"  → drop failing signals
    """

    if df_signals.empty:
        return df_signals

    # Merge liquidity fields
    df = df_signals.merge(liquidity_df, on="symbol", how="left", suffixes=("", "_liq"))

    # ------------------------------------
    # Hard-exclude rules
    #------------------------------------
    df["liquidity_pass"] = False

    # Only apply rules if liquidity data is present
    df.loc[df["net_trdval"].isna(), "liquidity_pass"] = False

    if "hard_min_net_trdval" in rules:
        df.loc[df["net_trdval"] < rules["hard_min_net_trdval"], "liquidity_pass"] = False

    if "hard_min_net_trdqty" in rules:
        df.loc[df["net_trdqty"] < rules["hard_min_net_trdqty"], "liquidity_pass"] = False

    if "hard_min_trades" in rules:
        df.loc[df["trades"] < rules["hard_min_trades"], "liquidity_pass"] = False

    if "min_price" in rules:
        df.loc[df["close"] < rules["min_price"], "liquidity_pass"] = False

    # ------------------------------------
    # Momentum normalization (abs momentum)
    # ------------------------------------
    if "momentum_score" in df.columns:
        mom_abs = df["momentum_score"].abs()
        max_mom = mom_abs.max()
        if pd.notna(max_mom) and max_mom > 0:
            df["mom_norm"] = mom_abs / max_mom
        else:
            df["mom_norm"] = 0
    else:
        df["mom_norm"] = 0

    # ------------------------------------
    # Combined score
    # ------------------------------------
    w_mom = rules["ranking_weights"]["mom"]
    w_turn = rules["ranking_weights"]["turnover"]
    w_vol = rules["ranking_weights"]["vol"]

    df["combined_score"] = (
        w_mom * df["mom_norm"].fillna(0) +
        w_turn * df["turnover_norm"].fillna(0) +
        w_vol * df["vol_norm"].fillna(0)
    )

    # FnO boost
    if "fno_boost" in rules:
        df.loc[df["fno_flag"] == True, "combined_score"] *= rules["fno_boost"]

    # ------------------------------------
    # Liquidity info object for params/extras JSON
    # ------------------------------------
    df["liquidity_info"] = df.apply(
        lambda row: {
            "net_trdval": row.get("net_trdval"),
            "net_trdqty": row.get("net_trdqty"),
            "trades": row.get("trades"),
            "avg_20d_vol": row.get("avg_20d_vol"),
            "turnover_rank_pct": row.get("turnover_rank_pct"),
            "turnover_norm": row.get("turnover_norm"),
            "vol_norm": row.get("vol_norm"),
            "mom_norm": row.get("mom_norm"),
            "combined_score": row.get("combined_score"),
            "liquidity_pass": bool(row.get("liquidity_pass")),
        },
        axis=1
    )

    # ------------------------------------
    # enforce mode → drop failures
    # tag_only mode → keep all
    # ------------------------------------
    if mode == "enforce":
        df = df[df["liquidity_pass"] == True].copy()

    return df
