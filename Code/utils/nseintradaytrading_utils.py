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
from datetime import timedelta
import pandas as pd
import numpy as np
from sqlalchemy import text

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
                                      gap_lookback_high_days=20):
    """
    Given df_signals (the DataFrame that contains signals to be persisted), populate:
      - entry_price (if None, will attempt to fill using bhavcopy open on entry_date)
      - stop_price
      - target_price
    Works for strategies: 'momentum_topn' and 'gap_follow' (others left untouched).
    Returns a new DataFrame (copy) with added columns and a 'price_enrichment_notes' JSON string column.
    """
    df = df_signals.copy()
    if df.empty:
        return df

    # normalize column names and ensure trade_date is datetime
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()
    if ref_date is None:
        # ref_date is the current processing date; use max trade_date by default
        ref_date = df['trade_date'].max()

    # determine entry_date per row: next_open -> trade_date + 1, else trade_date
    def compute_entry_date(row):
        em = str(row.get('entry_model') or '').lower()
        if em == 'next_open' or em == '' or em == 'next_open':
            return (row['trade_date'] + pd.Timedelta(days=1)).normalize()
        else:
            return row['trade_date']
    df['entry_date'] = df.apply(compute_entry_date, axis=1)

    # we will compute ATR per symbol (reference date = trade_date)
    symbols = df['symbol'].unique().tolist()
    # For simplicity compute ATR once using ref_date = max(trade_date) across df,
    # but ideally compute per-signal using its trade_date; here compute per-signal below.
    enrichment_notes = []
    # prepare a column for imputed flags
    df['stop_price_imputed'] = False
    df['target_price_imputed'] = False
    df['entry_price_imputed'] = False
    # Pre-fetch bhavcopy rows for all symbols for a window of interest:
    # Need entry day open/close and history up to trade_date - 1 for ATR
    max_trade_date = df['trade_date'].max()
    lookback_days = atr_lookback + 10
    bhav = fetch_bhavcopy_history(engine, symbols, end_date=max_trade_date, lookback_days=lookback_days)
    # create convenience lookup per (symbol, date)
    if not bhav.empty:
        bhav.set_index(['symbol','trade_date'], inplace=False)
    # Helper to get entry open
    def get_entry_open(sym, entry_date):
        if bhav.empty:
            return np.nan
        sub = bhav[(bhav['symbol'] == sym) & (bhav['trade_date'] == pd.to_datetime(entry_date).normalize())]
        if not sub.empty:
            return float(sub.iloc[0]['open'])
        return np.nan

    # Compute ATR per symbol per trade_date (we will compute per-row to be precise)
    results = []
    for idx, row in df.iterrows():
        sym = row['symbol']
        trade_date = row['trade_date']
        entry_date = row['entry_date']
        strategy = str(row.get('strategy') or '').lower()
        score = row.get('signal_score', None)
        entry_price = row.get('entry_price', None)
        stop_price = row.get('stop_price', None)
        target_price = row.get('target_price', None)

        # compute ATR referenced to trade_date: use history up to trade_date - 1
        try:
            atr_df = compute_atr_for_symbols(engine, [sym], ref_date=trade_date, atr_lookback=atr_lookback)
            atr_val = None
            if not atr_df.empty and sym in atr_df.index:
                atr_val = float(atr_df.loc[sym]['atr'])
            else:
                atr_val = np.nan
        except Exception as e:
            logger.exception("Failed to compute ATR for %s on %s: %s", sym, trade_date, e)
            atr_val = np.nan

        # Resolve entry_price if missing: use bhavcopy open on entry_date (if available)
        if pd.isna(entry_price) or entry_price is None:
            entry_open = get_entry_open(sym, entry_date)
            if pd.notna(entry_open):
                entry_price = float(entry_open)
                df.at[idx, 'entry_price_imputed'] = True
            else:
                # leave None / NaN
                entry_price = None

        # Now compute stops/targets by strategy
        note = {}
        if strategy == 'momentum_topn':
            # Option: use ATR-based stop/target around entry_price
            if entry_price is None:
                note['momentum'] = 'no_entry_price'
            note['atr'] = atr_val if not pd.isna(atr_val) else None
            if pd.notna(atr_val) and entry_price is not None:
                # LONG vs SHORT depends on sign of signal_score
                s_score = float(score) if score is not None else None
                if s_score is None:
                    # fallback assume LONG (conservative: no)
                    side = 'LONG'
                else:
                    side = 'LONG' if s_score >= 0 else 'SHORT'
                if side == 'LONG':
                    computed_stop = entry_price - (atr_stop_mult * atr_val)
                    computed_target = entry_price + (atr_target_mult * atr_val)
                else:
                    computed_stop = entry_price + (atr_stop_mult * atr_val)
                    computed_target = entry_price - (atr_target_mult * atr_val)
                # Only write stop/target if not present; or overwrite if present is NaN
                if pd.isna(stop_price) or stop_price is None:
                    stop_price = float(np.round(computed_stop, 4))
                    df.at[idx, 'stop_price_imputed'] = True
                if pd.isna(target_price) or target_price is None:
                    target_price = float(np.round(computed_target, 4))
                    df.at[idx, 'target_price_imputed'] = True
                note['computed_side'] = side
                note['computed_stop'] = stop_price
                note['computed_target'] = target_price
            else:
                note['momentum'] = 'atr_missing_or_entry_missing'

        elif strategy == 'gap_follow':
            # For gap follow we expect df to have prev_close in bhavcopy; entry_price likely is open
            # We'll compute stop relative to prev_close and target using atr
            # fetch prev_close from bhav for trade_date
            prev_close_val = None
            ph = bhav[(bhav['symbol'] == sym) & (bhav['trade_date'] == pd.to_datetime(trade_date).normalize())]
            # NOTE: since fetch_bhavcopy_history uses prev_close as prior day's close,
            # ph row here corresponds to trade_date; ph['prev_close'] should be previous day's close.
            if not ph.empty:
                prev_close_val = ph.iloc[-1].get('prev_close', None)
            note['atr'] = atr_val if not pd.isna(atr_val) else None
            note['prev_close'] = float(prev_close_val) if prev_close_val is not None else None
            # We expect entry_price to be open (if not already set)
            if entry_price is None:
                entry_open = get_entry_open(sym, entry_date)
                if pd.notna(entry_open):
                    entry_price = float(entry_open)
                    df.at[idx, 'entry_price_imputed'] = True
            # Decide side from gap_pct if present in notes/params; else infer from signal_score
            side = None
            if 'gap_pct' in (row.get('notes') or ''):
                # best-effort parse JSON in notes
                try:
                    import json as _json
                    notes_j = _json.loads(row.get('notes') or "{}")
                    gap_pct = float(notes_j.get('gap_pct')) if notes_j.get('gap_pct') else None
                    if gap_pct is not None:
                        side = 'LONG' if gap_pct > 0 else 'SHORT'
                except Exception:
                    side = None
            if side is None:
                try:
                    sc = float(row.get('signal_score')) if row.get('signal_score') is not None else None
                    side = 'LONG' if (sc is None or sc >= 0) else 'SHORT'
                except Exception:
                    side = 'LONG'
            # compute stop: put stop below prev_close for LONG, above prev_close for SHORT (if prev_close available)
            if pd.notna(atr_val) and entry_price is not None:
                if side == 'LONG':
                    # prefer prev_close-based stop if available
                    if prev_close_val is not None:
                        computed_stop = float(prev_close_val) - (atr_stop_mult * atr_val)
                    else:
                        computed_stop = entry_price - (atr_stop_mult * atr_val)
                    computed_target = entry_price + (atr_target_mult * atr_val)
                else:
                    if prev_close_val is not None:
                        computed_stop = float(prev_close_val) + (atr_stop_mult * atr_val)
                    else:
                        computed_stop = entry_price + (atr_stop_mult * atr_val)
                    computed_target = entry_price - (atr_target_mult * atr_val)

                if pd.isna(stop_price) or stop_price is None:
                    stop_price = float(np.round(computed_stop, 4))
                    df.at[idx, 'stop_price_imputed'] = True
                if pd.isna(target_price) or target_price is None:
                    target_price = float(np.round(computed_target, 4))
                    df.at[idx, 'target_price_imputed'] = True
                note['computed_side'] = side
                note['computed_stop'] = stop_price
                note['computed_target'] = target_price
            else:
                note['gap_follow'] = 'atr_missing_or_entry_missing'

        else:
            note['info'] = 'strategy_not_handled_by_enricher'

        # write computed values back into df copy
        df.at[idx, 'entry_price'] = entry_price if entry_price is not None else df.at[idx, 'entry_price'] if 'entry_price' in df.columns else None
        df.at[idx, 'stop_price'] = stop_price if stop_price is not None else df.at[idx, 'stop_price'] if 'stop_price' in df.columns else None
        df.at[idx, 'target_price'] = target_price if target_price is not None else df.at[idx, 'target_price'] if 'target_price' in df.columns else None
        enrichment_notes.append({'index': idx, 'notes': note})

    # add JSON notes column
    try:
        import json
        df['price_enrichment_notes'] = df.index.map(lambda i: next((n['notes'] for n in enrichment_notes if n['index'] == i), {}))
    except Exception:
        df['price_enrichment_notes'] = None

    return df
