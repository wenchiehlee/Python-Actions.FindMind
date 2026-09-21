# Python-Actions.FindMind

FinMind-based data fetchers for the GoodInfo Analyzer-compatible pipeline.

## Active implementations

- `skills/skill-finmind-fetch/`: FinMind Type 13 daily price and margin data. This is the single active copy of the shared `skill-finmind-fetch` skill.
- `skills/skill-finmind-fetch/scripts/fetch_type16.py`: FinMind Type 16 quarterly financial-ratio adapter. It uses the Analyzer 164-column schema and is a separate script within the same skill because it consumes quarterly financial statements, balance sheets, and cash-flow statements.
- `skills/skill-finmind-fetch/scripts/compare_type16.py`: compares generated Type 16 CSV values with the Analyzer GoodInfo CSV.
- `archived/`: legacy FindMind scripts, data, and workflows; not active implementations.

## Credentials

Copy `.env.example` to `.env` and set `FINMIND_TOKEN`. The `.env` file is ignored and must not be committed.

## Type 16 example

    python3 skills/skill-finmind-fetch/scripts/fetch_type16.py --stock-id 2330 --company-name 台積電 --start-date 2020-01-01 --end-date 2026-12-31 --output financial/type16/raw_fin_ratio_quarter_2330.csv
