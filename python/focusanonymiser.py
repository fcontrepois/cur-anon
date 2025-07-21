# focusanonymiser.py
"""
Anonymise generic tabular cost and usage files (e.g., Azure/Focus format) in Parquet or CSV format.

Removes or masks sensitive details, preserves data utility, and lets you share your reports safely.

---

USAGE EXAMPLES:

Generate a config file:
    python focusanonymiser.py --input rawdata.parquet --create-config --config config_focus.json

Run anonymisation:
    python focusanonymiser.py --input rawdata.parquet --output anonymised.parquet --config config_focus.json
    python focusanonymiser.py --input rawdata.parquet --output anonymised.csv --config config_focus.json

FLAGS:
    --input           Path to the input Parquet or CSV file (required)
    --output          Path to the output file (required unless --create-config is used)
    --config          Path to the JSON config file (required unless --create-config is used)
    --create-config   Generate a config file from the input file and exit
    --help            Show this help message and exit

CONFIG FILE OPTIONS:
    The config file is a JSON file with this structure:
    {
      "_comment": "Column options: 'keep', 'remove', 'hash', 'uuid'",
      "columns": {
        "column1": "keep",
        "column2": "remove",
        "column3": "hash",
        "column4": "uuid"
      }
    }

    Column options:
      keep    Keep the column as is
      remove  Remove the column from the output
      hash    Hash the column using DuckDB's md5_number_upper (same input = same output, not reversible)
      uuid    Replace the column value with a deterministic UUID (same input = same output, not reversible)

EXAMPLE CONFIG:
    {
      "_comment": "Column options: 'keep', 'remove', 'hash', 'uuid'",
      "columns": {
        "BillingAccountId": "hash",
        "BillingAccountName": "hash",
        "SubAccountId": "hash",
        "SubAccountIdName": "hash",
        "InvoiceId": "hash",
        "tag": "hash",
        "ResourceId": "keep",
        "ServiceName": "keep"
      }
    }

NOTES:
- By default, columns like BillingAccountId, BillingAccountName, SubAccountId, SubAccountIdName, InvoiceId, and tag are hashed for anonymisation; all others are kept unless changed in the config.
- Output can be Parquet or CSV, depending on the file extension you provide.
- Designed for generic cost/usage data, especially Azure/Focus-style exports.
"""

import argparse
import duckdb
import json
import os
import sys
import uuid
from anonymiser_common import parse_args, validate_input_file, generate_config_entry, build_uuid_mapping, generate_config, AnonymiserInputError

HELP_TEXT = """
Anonymise tabular files (Parquet/CSV) with generic options.

Flags:
  --input           Path to the input Parquet file (required)
  --output          Path to the output file (required unless --create-config is used)
  --config          Path to the JSON config file (required unless --create-config is used)
  --create-config   Generate a config file from the input Parquet file and exit

Config file options:
  The config file is a JSON file with this structure:
  {
    "_comment": "Column options: 'keep', 'remove', 'hash', 'uuid'",
    "columns": {
      "column1": "keep",
      "column2": "remove",
      "column3": "hash",
      "column4": "uuid"
    }
  }

  Column options:
    keep              Keep the column as is
    remove            Remove the column from the output
    hash              Hash the column using DuckDB's md5_number_upper (same input = same output, not reversible)
    uuid              Replace the column value with a deterministic UUID (same input = same output, not reversible)

Examples:
  Create a config file:
    python focusanonymiser.py --input rawdata.parquet --create-config --config config.json

  Run anonymisation:
    python focusanonymiser.py --input rawdata.parquet --output anonymised.parquet --config config.json
"""

def main():
    # Error handling for input validation is now done via AnonymiserInputError
    try:
        args = parse_args("Anonymise tabular files (Parquet/CSV) with generic options.", HELP_TEXT)

        if args.create_config:
            if not args.input:
                print("Error: --input is required for --create-config", file=sys.stderr)
                sys.exit(1)
            generate_config_entry(args.input, args.config, mode="focus")
            sys.exit(0)

        if not args.config or not args.output or not args.input:
            print("Error: --input, --config and --output are required unless --create-config is used.\n", file=sys.stderr)
            print(HELP_TEXT, file=sys.stderr)
            sys.exit(1)

        validate_input_file(args.input)

        with open(args.config, 'r') as f:
            config = json.load(f)
        column_actions = config["columns"]

        con = duckdb.connect()
        ext = os.path.splitext(args.input)[1].lower()
        if ext == ".csv":
            con.execute(f'CREATE TABLE data AS SELECT * FROM read_csv_auto(\'{args.input}\')')
        else:
            con.execute(f'CREATE TABLE data AS SELECT * FROM read_parquet(\'{args.input}\')')

        col_info = con.execute("PRAGMA table_info(data)").fetchall()
        if not col_info:
            print("Error: Input file has no columns (empty or header-only).", file=sys.stderr)
            sys.exit(1)
        # Header-only files (zero rows) are allowed; do not error.

        all_cols = [row[0] for row in con.execute("PRAGMA table_info(data)").fetchall()]
        keep_cols = [col for col, action in column_actions.items() if action in ("keep", "hash", "uuid")]
        hash_cols = [col for col, action in column_actions.items() if action == "hash"]
        uuid_cols = [col for col, action in column_actions.items() if action == "uuid"]

        mapping_tables = {}
        for col in uuid_cols:
            mapping_tables[col] = build_uuid_mapping(con, "data", col)

        select_cols = []
        join_clauses = []
        already_joined = set()
        for col in keep_cols:
            if col in uuid_cols:
                mt = mapping_tables[col]
                select_cols.append(f'COALESCE({mt}.fake, data."{col}") AS "{col}"')
                if mt not in already_joined:
                    join_clauses.append(f'LEFT JOIN {mt} ON data."{col}" = {mt}.original')
                    already_joined.add(mt)
            elif col in hash_cols:
                select_cols.append(f'md5_number_upper(data."{col}") AS "{col}"')
            else:
                select_cols.append(f'data."{col}"')

        select_sql = f'SELECT {', '.join(select_cols)} FROM data ' + ' '.join(join_clauses)

        output_file = args.output
        output_ext = os.path.splitext(output_file)[1].lower()

        if output_ext == ".csv":
            con.execute(f"COPY ({select_sql}) TO '{output_file}' (FORMAT CSV, HEADER 1)")
            print(f"Anonymised file written to {output_file} (CSV format)")
        else:
            con.execute(f"COPY ({select_sql}) TO '{output_file}' (FORMAT PARQUET)")
            print(f"Anonymised file written to {output_file} (Parquet format)")
    except AnonymiserInputError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 