# curanonymiser.py
# mostly done between DIA AI and chatGPT with some check from me

# curanonymiser.py
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

# curanonymiser.py

import argparse
import duckdb
import json
import os
import sys
from anonymiser_common import generate_fake_aws_account_id, generate_fake_arn, build_awsid_mapping, build_arn_mapping

HELP_TEXT = """
Anonymise AWS CUR Parquet files.

Flags:
  --input           Path to the input Parquet file (required)
  --output          Path to the output file (required unless --create-config is used)
  --config          Path to the JSON config file (required unless --create-config is used)
  --create-config   Generate a config file from the input Parquet file and exit

Config file options:
  The config file is a JSON file with this structure:
  {
    "_comment": "Column options: 'keep', 'remove', 'awsid_anonymise', 'awsarn_anonymise', 'hash'",
    "columns": {
      "column1": "keep",
      "column2": "remove",
      "column3": "awsid_anonymise",
      "column4": "awsarn_anonymise",
      "column5": "hash"
    }
  }

  Column options:
    keep              Keep the column as is
    remove            Remove the column from the output
    awsid_anonymise   Anonymise as AWS account ID (12-digit fake, consistent)
    awsarn_anonymise  Anonymise as AWS ARN, using the fake account ID
    hash              Hash the column using DuckDB's md5_number_upper (same input = same output, not reversible)

Examples:
  Create a config file:
    python curanonymiser.py --input rawcur.parquet --create-config --config config.json

  Run anonymisation:
    python curanonymiser.py --input rawcur.parquet --output anonymisedcur.parquet --config config.json
"""

def parse_args():
    parser = argparse.ArgumentParser(
        description="Anonymise AWS CUR Parquet files.",
        add_help=False,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=HELP_TEXT
    )
    parser.add_argument('--input', required=False, help='Input Parquet file')
    parser.add_argument('--output', required=False, help='Output file (Parquet or CSV)')
    parser.add_argument('--config', required=False, help='JSON config file for column handling')
    parser.add_argument('--create-config', action='store_true', help='Create a config file from the input Parquet file')
    parser.add_argument('--help', action='store_true', help='Show this help message and exit')
    return parser.parse_args()

def generate_config(input_file, config_file=None):
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{input_file}') LIMIT 0").fetchdf()
    columns = list(df.columns)
    config = {
        "_comment": (
            "Column options: 'keep' (keep as is), 'remove' (remove column), "
            "'awsid_anonymise' (anonymise as AWS account ID), "
            "'awsarn_anonymise' (anonymise as AWS ARN using fake account ID), "
            "'hash' (hash the column using DuckDB's md5_number_upper)"
        ),
        "columns": {}
    }
    for col in columns:
        col_lower = col.lower()
        if "account_id" in col_lower:
            config["columns"][col] = "awsid_anonymise"
        elif "resourceTags" in col_lower:
            config["columns"][col] = "hash"
        elif "resource_tags" in col_lower:
            config["columns"][col] = "hash"
        elif "a_r_n" in col_lower:
            config["columns"][col] = "awsarn_anonymise"
        else:
            config["columns"][col] = "keep"
    if config_file:
        with open(config_file, "w") as f:
            json.dump(config, f, indent=2)
        print(f"Config file created at {config_file}")
    else:
        print(json.dumps(config, indent=2))

def main():
    args = parse_args()

    if args.help:
        print(HELP_TEXT)
        sys.exit(0)

    if args.create_config:
        if not args.input:
            print("Error: --input is required for --create-config")
            sys.exit(1)
        # If --config is not provided, print to stdout
        if args.config:
            generate_config(args.input, args.config)
        else:
            generate_config(args.input, None)
        sys.exit(0)

    if not args.config or not args.output or not args.input:
        print("Error: --input, --config and --output are required unless --create-config is used.\n")
        print(HELP_TEXT)
        sys.exit(1)

    with open(args.config, 'r') as f:
        config = json.load(f)
    column_actions = config["columns"]

    con = duckdb.connect()
    con.execute(f"CREATE TABLE cur AS SELECT * FROM read_parquet('{args.input}')")

    all_cols = [row[0] for row in con.execute("PRAGMA table_info(cur)").fetchall()]
    keep_cols = [col for col, action in column_actions.items() if action in ("keep", "awsid_anonymise", "awsarn_anonymise", "hash")]
    anonymise_awsid_cols = [col for col, action in column_actions.items() if action == "awsid_anonymise"]
    anonymise_arn_cols = [col for col, action in column_actions.items() if action == "awsarn_anonymise"]
    hash_cols = [col for col, action in column_actions.items() if action == "hash"]

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

    select_cols = []
    join_clauses = []
    already_joined = set()
    for col in keep_cols:
        if col in anonymise_awsid_cols or col in anonymise_arn_cols:
            mt = mapping_tables[col]
            select_cols.append(f"COALESCE({mt}.fake, cur.\"{col}\") AS \"{col}\"")
            if mt not in already_joined:
                join_clauses.append(f"LEFT JOIN {mt} ON cur.\"{col}\" = {mt}.original")
                already_joined.add(mt)
        elif col in hash_cols:
            select_cols.append(f"md5_number_upper(cur.\"{col}\") AS \"{col}\"")
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

if __name__ == "__main__":
    main()
