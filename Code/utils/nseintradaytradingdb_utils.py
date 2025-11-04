# Code/common/db_utils.py
import json
import logging
import datetime
import numbers
import decimal
from collections.abc import Mapping, Sequence, Set

import pandas as pd

# --- Detection helper ---
def find_offending_cells(df: pd.DataFrame, sample_limit: int = 10):
    """
    Return list of offending cells as tuples: (row_idx, col, type_name, sample_value)
    """
    offenders = []
    # orient="records" makes a list of dicts; small memory but fine for typical signal sizes
    for i, row in enumerate(df.to_dict(orient="records")):
        for col, val in row.items():
            if val is None:
                continue
            if _is_sql_primitive(val):
                continue
            offenders.append((i, col, type(val).__name__, val))
            if len(offenders) >= sample_limit:
                return offenders
    return offenders

# --- Primitive checker ---
def _is_sql_primitive(v):
    """Return True if value is a primitive acceptable by DB drivers (string, number, datetime, None)."""
    if v is None:
        return True
    if isinstance(v, (str, bool, numbers.Integral, numbers.Real, decimal.Decimal)):
        return True
    if isinstance(v, (datetime.date, datetime.datetime, datetime.time)):
        return True
    return False

# --- Sanitizer ---
def sanitize_df_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a sanitized copy of df where non-primitive object cells are converted
    to JSON strings (or str fallback). Operates on object dtype columns only.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("sanitize_df_for_sql expects a pandas.DataFrame")

    # define once, in outer scope (avoids scope issues)
    def _safe(v):
        if v is None or _is_sql_primitive(v):
            return v
        # dict-like
        if isinstance(v, Mapping):
            try:
                return json.dumps(v, ensure_ascii=False, default=str)
            except Exception:
                return str(v)
        # list/tuple/set -> JSON array
        if isinstance(v, (Sequence, Set)) and not isinstance(v, (str, bytes, bytearray)):
            try:
                return json.dumps(list(v), ensure_ascii=False, default=str)
            except Exception:
                return str(v)
        # bytes -> decode
        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode("utf-8", errors="replace")
            except Exception:
                return repr(v)
        # numpy/pandas scalar types -> native
        try:
            import numpy as np, pandas as _pd
            if isinstance(v, (np.generic, _pd.Timestamp, _pd.Timedelta, _pd.Period)):
                return v.item() if hasattr(v, "item") else str(v)
        except Exception:
            pass
        # fallback to json.dumps(default=str) or str()
        try:
            return json.dumps(v, ensure_ascii=False, default=str)
        except Exception:
            return str(v)

    df_out = df.copy()
    for col in df_out.columns:
        if df_out[col].dtype == "object":
            # apply unconditionally for object columns (safe & simple)
            df_out[col] = df_out[col].apply(_safe)
    return df_out

# --- Safe write helper ---
def safe_to_sql(df: pd.DataFrame, table_name: str, engine, if_exists: str = "replace", index: bool = False, chunksize: int | None = None):
    """
    Safely write df to SQL using SQLAlchemy engine.
    - Ensures df is a DataFrame.
    - Raises informative errors if not.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"safe_to_sql expected DataFrame but got {type(df)}")

    logging.info("safe_to_sql: writing %d rows to table '%s' (if_exists=%s)", len(df), table_name, if_exists)
    try:
        df.to_sql(table_name, engine, index=index, if_exists=if_exists, chunksize=chunksize)
    except Exception as e:
        logging.exception("safe_to_sql failed writing table '%s': %s", table_name, e)
        raise

# --- Merge helper for MySQL (ON DUPLICATE KEY UPDATE) ---
def merge_temp_to_target(engine, temp_table: str, target_table: str, unique_key_cols: list, update_columns: list, extra_where: str = None):
    """
    Execute a MySQL-style INSERT ... SELECT ... ON DUPLICATE KEY UPDATE to merge temp_table into target_table.
    - unique_key_cols: list of column names that together form the unique constraint (must exist on target_table).
    - update_columns: list of columns to update when duplicate key found.
    - extra_where: optional SQL condition (string) to append on the SELECT.
    """
    if not unique_key_cols:
        raise ValueError("unique_key_cols must be provided")
    if not update_columns:
        raise ValueError("update_columns must be provided")

    # build select clause: select all columns from temp table
    # (we assume temp_table has same columns as target_table or subset)
    update_clause = ", ".join([f"{col}=VALUES({col})" for col in update_columns])
    where_clause = f"WHERE {extra_where}" if extra_where else ""
    sql = f"""
    INSERT INTO {target_table}
    SELECT * FROM {temp_table} {where_clause}
    ON DUPLICATE KEY UPDATE {update_clause};
    """
    logging.info("merge_temp_to_target executing merge SQL into %s (temp=%s)", target_table, temp_table)
    with engine.begin() as conn:
        conn.execute(sql)
    logging.info("merge_temp_to_target completed into %s", target_table)
