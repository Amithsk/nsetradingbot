This repository contains a production-grade intraday trading automation system designed and implemented end-to-end.
The project covers the full lifecycle from NSE data ingestion → feature engineering → signal generation → price enrichment → evaluation → analytics dashboards.

I built this system to support reliable, reproducible and fully automated daily signal generation for intraday trading strategies.

**Project Overview**

The platform consists of multiple independent, fault-tolerant pipelines:

**Data Ingestion:**

Automated download of NSE PR/GL/HL/TT bhavcopy ZIP files.

Browser-like session simulation (cookies, user-agent), retry logic, and fallback to previous day’s data.

Bhavcopy files committed into Git for traceability.

**Market Data Processing:**

Parsing raw NSE CSVs (multiple schema variations handled).

Symbol normalization, type-safe conversions, NaN/inf handling.

Candidate extraction (gainers/losers, circuit breakers, turnover leaderboard).

Enriched CSVs written and upserted into intraday_bhavcopy.

**Feature Engineering:**

Rolling ATR, Momentum, RSI, True Range.

Symbol-wise historical windows with defensive lookback logic.

Upsert into strategy_features with ON DUPLICATE support.

**Signal Generation:**

Momentum Top-N

Gap-Follow

Volatility Breakout

Automatic assignment of signal direction, entry model, and score computation.

Signals enriched with ATR-driven stop/target logic.

Signal Enrichment (risk model):

ATR-based stop loss and target.

Percent-fallback risk model when ATR or entry price is missing.

Symbol/date-specific price lookups for entry-price imputation.

**Signal Evaluation:**

Per-signal realized high/low/close extraction.

Evaluation of win/loss/neutral outcomes.

Incremental backfill of missing dates.

Upsert into signal_evaluation_results.

Analytics Dashboard (Streamlit):

Intraday “Trade-of-the-Day” summary.

Feature trend visualization.

Per-symbol signal timelines and evaluation overlays.

Compact views for daily decisioning.

**Architecture**


        ┌───────────────────────────────┐
        │  NSE Daily Downloader         │
        └───────────────┬──────────────┘
                        ▼
        ┌───────────────────────────────┐
        │ Raw CSV Parser & Candidate    │
        │ Extraction (GL/HL/PR/TT)      │
        └───────────────┬──────────────┘
                        ▼
        ┌───────────────────────────────┐
        │ Bhavcopy Upsert (MySQL)       │
        └───────────────┬──────────────┘
                        ▼
        ┌───────────────────────────────┐
        │ Feature Engineering (ATR, RSI │
        │ Momentum, TR)                 │
        │ → strategy_features           │
        └───────────────┬──────────────┘
                        ▼
        ┌───────────────────────────────┐
        │ Strategy Engines (3 models)   │
        │ → strategy_signals            │
        └───────────────┬──────────────┘
                        ▼
        ┌───────────────────────────────┐
        │ Stop/Target Enrichment        │
        │ ATR + fallback logic          │
        └───────────────┬──────────────┘
                        ▼
        ┌───────────────────────────────┐
        │ Evaluation Engine             │
        │ → signal_evaluation_results   │
        └───────────────────────────────┘




**Design Philosophy
**
Build pipelines that never silently fail

Separate ingestion, processing, feature & signal layers

Make each pipeline independently testable

Use explicit defensive checks (sanitizers, date extractors, schema detectors)

Ensure reproducibility with Git-committed bhavcopy snapshots

Keep the codebase modular: data → features → signals → evaluation
