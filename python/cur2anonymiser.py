# cur2anonymiser.py
# mostly done between DIA AI and chatGPT with some check from me
#
# MIT License
# 
# Copyright (c) 2025 Frank Contrepois  
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

#
# Usage Examples:
#
# Generate a config file:
#   python cur2anonymiser.py --input rawcur2.parquet --create-config --config config_cur2.json
#
# Run anonymisation:
#   python cur2anonymiser.py --input rawcur2.parquet --output anonymisedcur2.parquet --config config_cur2.json
#   python cur2anonymiser.py --input rawcur2.parquet --output anonymisedcur2.csv --config config_cur2.json
#
# Flags:
#   --input           Path to the input Parquet file (required)
#   --output          Path to the output file (required unless --create-config is used)
#   --config          Path to the JSON config file (required unless --create-config is used)
#   --create-config   Generate a config file from the input Parquet file and exit
#   --help            Show this help message and exit

import argparse
import duckdb
import json
import os
import sys
from anonymiser_common import parse_args, validate_input_file, generate_config_entry, build_awsid_mapping, build_arn_mapping, build_uuid_mapping, generate_config, AnonymiserInputError

HELP_TEXT = """
Anonymise AWS CUR2 Parquet files.

Flags:
  --input           Path to the input Parquet file (required)
  --output          Path to the output file (required unless --create-config is used)
  --config          Path to the JSON config file (required unless --create-config is used)
  --create-config   Generate a config file from the input Parquet file and exit

Config file options:
  The config file is a JSON file with this structure:
  {
    "_comment": "Column options: 'keep', 'remove', 'awsid_anonymise', 'awsarn_anonymise', 'hash', 'uuid'",
    "columns": {
      "column1": "keep",
      "column2": "remove",
      "column3": "awsid_anonymise",
      "column4": "awsarn_anonymise",
      "column5": "hash",
      "column6": "uuid"
    }
  }

  Column options:
    keep              Keep the column as is
    remove            Remove the column from the output
    awsid_anonymise   Anonymise as AWS account ID (12-digit fake, consistent)
    awsarn_anonymise  Anonymise as AWS ARN, using the fake account ID
    hash              Hash the column using DuckDB's md5_number_upper (same input = same output, not reversible)
    uuid              Replace the column value with a deterministic UUID (same input = same output, not reversible)

Examples:
  Create a config file:
    python cur2anonymiser.py --input rawcur2.parquet --create-config --config config_cur2.json

  Run anonymisation:
    python cur2anonymiser.py --input rawcur2.parquet --output anonymisedcur2.parquet --config config_cur2.json
"""

def main():
    # Error handling for input validation is now done via AnonymiserInputError
    try:
        args = parse_args("Anonymise AWS CUR Parquet files.", HELP_TEXT)

        if args.create_config:
            if not args.input:
                print("Error: --input is required for --create-config", file=sys.stderr)
                sys.exit(1)
            generate_config_entry(args.input, args.config, mode="cur2")
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
            con.execute(f'CREATE TABLE cur AS SELECT * FROM read_csv_auto(\'{args.input}\')')
        else:
            con.execute(f'CREATE TABLE cur AS SELECT * FROM read_parquet(\'{args.input}\')')

        col_info = con.execute("PRAGMA table_info(cur)").fetchall()
        if not col_info:
            print("Error: Input file has no columns (empty or header-only).", file=sys.stderr)
            sys.exit(1)
        # Header-only files (zero rows) are allowed; do not error.

        all_cols = [row[0] for row in con.execute("PRAGMA table_info(cur)").fetchall()]
        keep_cols = [col for col, action in column_actions.items() if action in ("keep", "awsid_anonymise", "awsarn_anonymise", "hash", "uuid")]
        anonymise_awsid_cols = [col for col, action in column_actions.items() if action == "awsid_anonymise"]
        anonymise_arn_cols = [col for col, action in column_actions.items() if action == "awsarn_anonymise"]
        hash_cols = [col for col, action in column_actions.items() if action == "hash"]
        uuid_cols = [col for col, action in column_actions.items() if action == "uuid"]

        mapping_tables = {}
        for col in anonymise_awsid_cols:
            mapping_tables[col] = build_awsid_mapping(con, "cur", col)

        for col in anonymise_arn_cols:
            possible_account_cols = [c for c in anonymise_awsid_cols if "account" in c.lower()]
            if not possible_account_cols:
                raise Exception(f"No account id column found for ARN column {col}")
            account_col = possible_account_cols[0]
            account_mapping_table = mapping_tables[account_col]
            mapping_tables[col] = build_arn_mapping(con, "cur", col, account_col, account_mapping_table)

        for col in uuid_cols:
            mapping_tables[col] = build_uuid_mapping(con, "cur", col)

        select_cols = []
        join_clauses = []
        already_joined = set()
        for col in keep_cols:
            if col in anonymise_awsid_cols or col in anonymise_arn_cols:
                mt = mapping_tables[col]
                select_cols.append(f"COALESCE({mt}.fake, CAST(cur.\"{col}\" AS VARCHAR)) AS \"{col}\"")
                if mt not in already_joined:
                    join_clauses.append(f"LEFT JOIN {mt} ON cur.\"{col}\" = {mt}.original")
                    already_joined.add(mt)
            elif col in uuid_cols:
                mt = mapping_tables[col]
                select_cols.append(f"COALESCE({mt}.fake, CAST(cur.\"{col}\" AS VARCHAR)) AS \"{col}\"")
                if mt not in already_joined:
                    join_clauses.append(f"LEFT JOIN {mt} ON cur.\"{col}\" = {mt}.original")
                    already_joined.add(mt)
            elif col in hash_cols:
                select_cols.append(f"md5_number_upper(CAST(cur.\"{col}\" AS VARCHAR)) AS \"{col}\"")
            else:
                select_cols.append(f"cur.\"{col}\"")

        select_sql = f"SELECT {', '.join(select_cols)} FROM cur " + " ".join(join_clauses)

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
