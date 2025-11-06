# eval_logic.py
from decimal import Decimal
from typing import Dict, Any

# Defaults (tuneable)
DEFAULT_TARGET_PCT = Decimal("0.005")   # +0.5%
DEFAULT_STOP_PCT   = Decimal("-0.005")  # -0.5%
CLOSE_THRESHOLD    = Decimal("0.0")     # fallback threshold for close-based win

def decide_outcome(signal_row: Dict[str, Any],
                   bhav_row: Dict[str, Any],
                   defaults: Dict[str, Decimal] = None) -> Dict[str, Any]:
    """
    Pure function. Given:
      - signal_row: dict with keys like entry_price, target_price, stop_price, entry_model
      - bhav_row: dict with keys open, high, low, close
      - defaults: dict with keys 'target_pct', 'stop_pct', 'close_threshold'
    Returns a dict with:
      - entry_price_used, target_price, stop_price, realized_high, realized_low, close_price,
        exit_price, exit_reason, realized_return, label_outcome, ambiguous_flag, notes
    Important: NO DB I/O here.
    """

    if defaults is None:
        defaults = {'target_pct': DEFAULT_TARGET_PCT,
                    'stop_pct': DEFAULT_STOP_PCT,
                    'close_threshold': CLOSE_THRESHOLD}

    # Determine entry price
    entry_price_signal = signal_row.get('entry_price')
    if entry_price_signal is not None and entry_price_signal > 0:
        entry_price = Decimal(str(entry_price_signal))
    else:
        entry_price = Decimal(str(bhav_row['open']))

    # Determine target/stop
    target_price = signal_row.get('target_price')
    stop_price = signal_row.get('stop_price')

    if not target_price or float(target_price) == 0.0:
        target_price = (entry_price * (Decimal('1.0') + defaults['target_pct'])).quantize(Decimal('0.0001'))
    else:
        target_price = Decimal(str(target_price))

    if not stop_price or float(stop_price) == 0.0:
        stop_price = (entry_price * (Decimal('1.0') + defaults['stop_pct'])).quantize(Decimal('0.0001'))
    else:
        stop_price = Decimal(str(stop_price))

    high = Decimal(str(bhav_row['high']))
    low  = Decimal(str(bhav_row['low']))
    close = Decimal(str(bhav_row['close']))

    ambiguous = False
    exit_price = None
    exit_reason = 'no_hit'
    label = 'neutral'
    realized_return = Decimal('0.0')
    notes = ''

    hit_target = high >= target_price
    hit_stop   = low <= stop_price

    if hit_target and not hit_stop:
        exit_reason = 'target'
        exit_price = target_price
        label = 'win'
    elif hit_stop and not hit_target:
        exit_reason = 'stop'
        exit_price = stop_price
        label = 'loss'
    elif hit_target and hit_stop:
        ambiguous = True
        # Conservative default: assume stop hit first (safe)
        exit_reason = 'ambiguous_assume_stop'
        exit_price = stop_price
        label = 'loss'
        notes = 'ambiguous: both target & stop inside day; assumed stop first (conservative)'
    else:
        # fallback to close-based decision
        exit_price = close
        exit_reason = 'close'
        realized_return = (close - entry_price) / entry_price
        if realized_return >= defaults['close_threshold']:
            label = 'win'
        elif realized_return <= -abs(defaults['close_threshold']):
            label = 'loss'
        else:
            label = 'neutral'
        notes = 'no target/stop hit - used close fallback'

    if exit_price is not None and entry_price > 0:
        realized_return = (Decimal(str(exit_price)) - entry_price) / entry_price

    return {
        'entry_price_used': float(entry_price),
        'target_price': float(target_price),
        'stop_price': float(stop_price),
        'realized_high': float(high),
        'realized_low': float(low),
        'close_price': float(close),
        'exit_price': float(exit_price),
        'exit_reason': exit_reason,
        'realized_return': float(realized_return),
        'label_outcome': label,
        'ambiguous_flag': int(ambiguous),
        'notes': notes
    }
