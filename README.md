# Python-Actions.FindMind

FinMind-based data fetchers for the GoodInfo Analyzer-compatible pipeline.

## Active implementations

- `skills/skill-finmind-fetch/`: FinMind Type 13 daily price and margin data. This is the single active copy of the shared `skill-finmind-fetch` skill.
- `skills/skill-finmind-fetch/scripts/fetch_type14.py`: FinMind Type 14 weekly margin adapter. It reuses Type 13 daily CSV when available, otherwise aggregates FinMind daily price/margin data.
- `skills/skill-finmind-fetch/scripts/fetch_type16.py`: FinMind Type 16 quarterly financial-ratio adapter. It uses the Analyzer 164-column schema and is a separate script within the same skill because it consumes quarterly financial statements, balance sheets, and cash-flow statements.
- `skills/skill-finmind-fetch/scripts/compare_type16.py`: compares generated Type 16 CSV values with the Analyzer GoodInfo CSV.
- `archived/`: legacy FindMind scripts, data, and workflows; not active implementations.

## Credentials

Copy `.env.example` to `.env` and set one or more of `FINMIND_TOKEN`, `FINMIND_API_TOKEN`, `FINDMIND_GMAIL_TOKEN`, `FINDMIND_GMAIL_TOKEN1`, and `FINDMIND_GMAIL_TOKEN2`. Non-empty tokens rotate per request. The `.env` file is ignored and must not be committed.

## Type 16 example

    python3 skills/skill-finmind-fetch/scripts/fetch_type16.py --stock-id 2330 --company-name 台積電 --start-date 2020-01-01 --end-date 2026-12-31 --output financial/type16/raw_fin_ratio_quarter_2330.csv

## Type 14 example

    python3 skills/skill-finmind-fetch/scripts/fetch_type14.py --stock-id 2330 --company-name 台積電 --daily-csv /path/to/raw_margin_daily.csv --output financial/type14/raw_margin_weekly_2330.csv
