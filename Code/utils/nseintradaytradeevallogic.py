from typing import Dict, Any
import math

def _safe_float(v):
    try:
        if v is None:
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None

def decide_outcome(signal_row: Dict[str, Any], bhav_row: Dict[str, Any], defaults: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Simple deterministic intraday evaluation:
    - entry_price_used: prefer signal.entry_price, else bhav.open
    - realized_high/low/close: from bhav_row
    - For LONG: if realized_high >= target -> win (exit_price = target)
                elif realized_low <= stop -> loss (exit_price = stop)
                else -> neutral (exit_price = close)
      For SHORT: reversed logic.
    - If both stop and target were hit intraday (both conditions true) -> ambiguous_flag = 1, label_outcome = 'neutral' and notes explain both hit.
    - realized_return calculated when exit_price available.
    """
    # read numeric values defensively
    entry_price = signal_row.get('entry_price') if signal_row.get('entry_price') is not None else bhav_row.get('open')
    entry_price = _safe_float(entry_price)
    stop_price = _safe_float(signal_row.get('stop_price'))
    target_price = _safe_float(signal_row.get('target_price'))

    realized_high = _safe_float(bhav_row.get('high'))
    realized_low = _safe_float(bhav_row.get('low'))
    close_price = _safe_float(bhav_row.get('close'))

    # determine side: prefer explicit signal_type if present, else fallback to 'LONG'
    side = None
    try:
        side_raw = signal_row.get('signal_type') or signal_row.get('signal') or ''
        side = str(side_raw).upper().strip() if side_raw is not None else None
    except Exception:
        side = None
    if side not in ('LONG', 'SHORT'):
        # fallback: if target > entry assume LONG, else SHORT if target < entry; otherwise default LONG
        if entry_price and target_price:
            side = 'LONG' if target_price >= entry_price else 'SHORT'
        else:
            side = 'LONG'

    outcome = {
        'entry_price_used': entry_price,
        'stop_price': stop_price,
        'target_price': target_price,
        'realized_high': realized_high,
        'realized_low': realized_low,
        'close_price': close_price,
        'realized_return': None,
        'exit_price': None,
        'exit_reason': None,
        'label_outcome': 'neutral',
        'ambiguous_flag': 0,
        'notes': ''
    }

    # If we don't have essential price data, return neutral with note
    if entry_price is None or realized_high is None or realized_low is None or close_price is None:
        outcome['notes'] = 'insufficient price data to evaluate (entry/hlc missing)'
        return outcome

    # Determine whether stop/target were hit intraday
    target_hit = False
    stop_hit = False
    if target_price is not None:
        if side == 'LONG' and realized_high >= target_price:
            target_hit = True
        if side == 'SHORT' and realized_low <= target_price:
            target_hit = True
    if stop_price is not None:
        if side == 'LONG' and realized_low <= stop_price:
            stop_hit = True
        if side == 'SHORT' and realized_high >= stop_price:
            stop_hit = True

    # Ambiguity: both hit in same day (we don't have intraday sequence) => mark ambiguous and neutral
    if target_hit and stop_hit:
        outcome['ambiguous_flag'] = 1
        outcome['exit_price'] = None
        outcome['exit_reason'] = None
        outcome['label_outcome'] = 'neutral'
        outcome['notes'] = 'both stop and target appear hit intraday -> ambiguous (no intraday sequence available)'
        return outcome

    # Target wins
    if target_hit:
        outcome['exit_price'] = target_price
        outcome['exit_reason'] = 'target'
        outcome['label_outcome'] = 'win'
    # Stop losses
    elif stop_hit:
        outcome['exit_price'] = stop_price
        outcome['exit_reason'] = 'stop'
        outcome['label_outcome'] = 'loss'
    else:
        # neither hit: assume exit at close -> neutral unless close beyond target/stop (redundant)
        outcome['exit_price'] = close_price
        outcome['exit_reason'] = 'close'
        outcome['label_outcome'] = 'neutral'
        outcome['notes'] = 'neither target nor stop hit intraday; exit assumed at close'

    # Compute realized return (percentage) if exit_price and entry_price present
    try:
        if outcome['exit_price'] is not None and entry_price not in (None, 0):
            if side == 'LONG':
                realized_pct = ((outcome['exit_price'] - entry_price) / entry_price) * 100.0
            else:
                # for SHORT: profit if exit_price < entry_price
                realized_pct = ((entry_price - outcome['exit_price']) / entry_price) * 100.0
            outcome['realized_return'] = float(realized_pct)
        else:
            outcome['realized_return'] = None
    except Exception:
        outcome['realized_return'] = None

    return outcome
