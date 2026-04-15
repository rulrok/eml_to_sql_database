# EML to SQL Database

Parse `.eml` email files and load into a local DuckDB database using [dlt](https://github.com/dlt-hub/dlt).

This project helps to perform data-analysis in a large quantity of dumped emails.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Note: `fast-mail-parser` may compile a small Rust extension. If you hit build errors, install a Rust toolchain (e.g., via rustup) and reinstall.

## Use

1) Drop your `.eml` files into `eml_files/`
2) Run the loader:

```bash
source .venv/bin/activate
python eml_to_duckdb.py
```

This will create/update a DuckDB database at the root of the project.

Open it with DuckDB and query the `emails.messages` table, for example:
```bash
duckdb message.duckdb
```

Example loading:
```sql
SELECT * FROM emails.messages LIMIT 5;
SELECT * FROM emails.messages__headers LIMIT 5;
SELECT * FROM emails.messages__custom_headers LIMIT 5;
```
