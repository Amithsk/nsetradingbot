"""CLI coverage for the opt-in saved-report replacement path."""

from __future__ import annotations

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from unittest.mock import patch

from Code import stock_review


class StockReviewReplaceTests(unittest.TestCase):
    payload = {
        "exchange": "NSE",
        "symbol": "BHARTIARTL",
        "trade_date": date(2026, 11, 23),
        "stock_coverage_json": {},
        "benchmark_coverage_json": {},
    }
    saved = {"id": 3, "exchange": "NSE", "symbol": "BHARTIARTL", "trade_date": date(2026, 11, 23)}

    def test_replace_requires_save(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            result = stock_review.main([
                "--symbol", "BHARTIARTL", "--trade-date", "2026-11-23",
                "--git-revision", "main", "--replace",
            ])
        self.assertEqual(result, 2)
        self.assertIn("--replace requires --save", stderr.getvalue())

    @patch.object(stock_review, "save_or_read")
    @patch.object(stock_review, "build_report")
    @patch.object(stock_review, "resolve_revision", return_value="revision")
    def test_replace_is_explicit_and_reports_saved(self, _revision, build_report, save_or_read):
        build_report.return_value = (self.payload, "report")
        save_or_read.return_value = (self.saved, True)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            result = stock_review.main([
                "--symbol", "BHARTIARTL", "--trade-date", "2026-11-23",
                "--git-revision", "main", "--save", "--replace",
            ])
        self.assertEqual(result, 0)
        save_or_read.assert_called_once_with(self.payload, replace=True)
        self.assertIn('"saved": true', stdout.getvalue())
        self.assertIn('"replaced": true', stdout.getvalue())

    @patch.object(stock_review, "save_or_read")
    @patch.object(stock_review, "build_report")
    @patch.object(stock_review, "resolve_revision", return_value="revision")
    def test_normal_save_keeps_reuse_semantics(self, _revision, build_report, save_or_read):
        build_report.return_value = (self.payload, "report")
        save_or_read.return_value = (self.saved, False)
        result = stock_review.main([
            "--symbol", "BHARTIARTL", "--trade-date", "2026-11-23",
            "--git-revision", "main", "--save",
        ])
        self.assertEqual(result, 0)
        save_or_read.assert_called_once_with(self.payload, replace=False)


if __name__ == "__main__":
    unittest.main()
