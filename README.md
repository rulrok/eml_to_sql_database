# EML to DuckDB (headers only MVP)

This project ingests `.eml` email files and loads selected headers into a DuckDB database using [dlt](https://github.com/dlt-hub/dlt).

Future work: parse message body and attachments. For now, we only extract headers you configure.

## Setup

1. Create and activate a virtual environment (already created as `.venv`).
2. Install dependencies:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

This project uses the `fast-mail-parser` library for high-performance EML parsing.
Note: it builds a small Rust extension. If your environment lacks a recent Rust toolchain,
install it with rustup (one-time):

```bash
curl -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
```

Then install Python requirements normally.

## Configure

Edit `config/eml_config.yaml`:
- `eml.source_dir`: directory containing `.eml` files
- `eml.headers`: list of headers to extract into top-level columns
- `duckdb.database`: path to the DuckDB file that will be created
- `duckdb.dataset`: dataset/schema name inside DuckDB managed by dlt

## Run (dlt project style)

Use the pipeline script under `pipelines/`:

```bash
source .venv/bin/activate
python pipelines/eml_to_duckdb.py --config config/eml_config.yaml
```

On completion, open the DuckDB file to inspect (dataset name may have a suffix in dev mode):

```bash
duckdb data/eml.duckdb
-- Inside DuckDB CLI:
SELECT * FROM eml_data_messages LIMIT 5;
```

## Development notes
- The `messages` table contains a JSON-like `headers` column with all headers plus dedicated columns for the configured ones (e.g., `subject`, `from`, `to`).
- The primary key is `id` (uses `Message-ID` if present, otherwise a generated UUID).
- Columns are type-hinted to ensure they are materialized even if an individual batch contains only nulls for some headers (e.g., `cc`, `bcc`).
