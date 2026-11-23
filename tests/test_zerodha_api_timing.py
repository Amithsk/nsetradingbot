"""Mocked coverage for NIFTY futures endpoint timing diagnostics."""

from __future__ import annotations

import re
import unittest
from datetime import date
from unittest.mock import patch

from fastapi import HTTPException

from Code.zerodha import api


class FakeKite:
    """Minimal Kite-compatible fake; no live Zerodha calls are made."""

    def instruments(self, exchange: str):
        self.exchange = exchange
        return [
            {
                "instrument_token": 12345,
                "tradingsymbol": "NIFTY26DEC",
                "expiry": "2026-12-31",
                "exchange": "NFO",
                "segment": "NFO-FUT",
                "instrument_type": "FUT",
                "name": "NIFTY",
            }
        ]


class NiftyFuturesTimingTests(unittest.TestCase):
    """Verify timing output without depending on credentials or the network."""

    timing_prefixes = (
        "kite_client_initialization_ms",
        "nfo_instrument_retrieval_ms",
        "contract_selection_ms",
        "historical_candle_retrieval_ms",
        "response_assembly_ms",
        "total_endpoint_execution_ms",
    )

    @patch.object(api, "get_target_date", return_value=date(2026, 11, 23))
    @patch.object(api, "fetch_historical_candles")
    @patch.object(api, "get_kite_client")
    def test_success_emits_all_timing_logs_once_and_preserves_response(
        self,
        mock_get_kite_client,
        mock_fetch_historical_candles,
        _mock_get_target_date,
    ):
        mock_get_kite_client.return_value = FakeKite()
        mock_fetch_historical_candles.return_value = [{"Datetime": "value"}]

        with self.assertLogs("uvicorn.error", level="INFO") as captured:
            response = api.nifty_futures_candles()

        self.assertEqual(
            response,
            {
                "status": "success",
                "trade_date": "2026-11-23",
                "interval": "5minute",
                "contract": {
                    "instrument_token": 12345,
                    "tradingsymbol": "NIFTY26DEC",
                    "expiry": "2026-12-31",
                    "exchange": "NFO",
                    "segment": "NFO-FUT",
                    "instrument_type": "FUT",
                },
                "candles": [{"Datetime": "value"}],
                "count": 1,
            },
        )

        timing_messages = [
            message
            for message in captured.output
            if "NIFTY futures timing |" in message
        ]
        self.assertEqual(len(timing_messages), 6)
        for prefix in self.timing_prefixes:
            matching = [message for message in timing_messages if prefix in message]
            self.assertEqual(len(matching), 1)
            elapsed_ms = float(re.search(r"=([0-9.]+)$", matching[0]).group(1))
            self.assertGreaterEqual(elapsed_ms, 0)

    @patch.object(api, "get_kite_client", side_effect=RuntimeError("upstream failed"))
    def test_client_failure_keeps_http_error_and_emits_elapsed_and_total_timings(
        self,
        _mock_get_kite_client,
    ):
        with self.assertLogs("uvicorn.error", level="INFO") as captured:
            with self.assertRaises(HTTPException) as raised:
                api.nifty_futures_candles()

        self.assertEqual(raised.exception.status_code, 500)
        self.assertEqual(
            raised.exception.detail,
            "Unable to fetch NIFTY Futures candles: upstream failed",
        )
        timing_messages = [
            message
            for message in captured.output
            if "NIFTY futures timing |" in message
        ]
        self.assertEqual(len(timing_messages), 2)
        self.assertTrue(any("kite_client_initialization_ms" in message for message in timing_messages))
        self.assertTrue(any("total_endpoint_execution_ms" in message for message in timing_messages))


if __name__ == "__main__":
    unittest.main()
