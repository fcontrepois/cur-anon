# curanonymiser.py

import argparse
import duckdb
import json
import os
import random
import re
import sys

def parse_args():
    parser = argparse.ArgumentParser(description="Anonymise AWS CUR Parquet files.")
    parser.add_argument('--input', required=True, help='Input Parquet file')
    parser.add_argument('--output', required=False, help='Output Parquet file')
    parser.add_argument('--config', required=False, help='JSON config file for column handling')
    parser.add_argument('--create-config', action='store_true', help='Create a config file from the input Parquet file')
    return parser.parse_args()

def generate_config(input_file, config_file):
    con = duckdb.connect()
    # Read only the schema
    df = con.execute(f"SELECT * FROM read_parquet('{input_file}') LIMIT 0").fetchdf()
    columns = list(df.columns)
    config = {"columns": {col: "keep" for col in columns}}
    with open(config_file, "w") as f:
        json.dump(config, f, indent=2)
    print(f"Config file created at {config_file}")

def generate_fake_aws_account_id(original_id):
    random.seed(str(original_id))
    return ''.join(random.choices('0123456789', k=12))

def generate_fake_arn(original_arn, fake_account_id):
    arn_regex = r"^arn:([^:]+):([^:]*):([^:]*):([^:]*):(.+)$"
    m = re.match(arn_regex, original_arn)
    if m:
        parts = list(m.groups())
        parts[3] = fake_account_id
        return f"arn:{':'.join(parts)}"
    else:
        return original_arn

def build_awsid_mapping(con, table, col):
    unique_ids = con.execute(f"SELECT DISTINCT \"{col}\" FROM {table} WHERE \"{col}\" IS NOT NULL").fetchall()
    mapping = []
    for (orig_id,) in unique_ids:
        fake_id = generate_fake_aws_account_id(orig_id)
        mapping.append((orig_id, fake_id))
    mapping_table = f"map_{col.replace('/', '_')}"
    con.execute(f"CREATE TEMP TABLE {mapping_table} (original TEXT, fake TEXT)")
    con.executemany(f"INSERT INTO {mapping_table} (original, fake) VALUES (?, ?)", mapping)
    return mapping_table

def build_arn_mapping(con, table, col, account_col, account_mapping_table):
    unique_arns = con.execute(
        f"SELECT DISTINCT cur.\"{col}\", cur.\"{account_col}\" FROM {table} cur WHERE cur.\"{col}\" IS NOT NULL"
    ).fetchall()
    account_map = dict(con.execute(f"SELECT original, fake FROM {account_mapping_table}").fetchall())
    mapping = []
    for orig_arn, orig_account_id in unique_arns:
        fake_account_id = account_map.get(str(orig_account_id), generate_fake_aws_account_id(orig_account_id))
        fake_arn = generate_fake_arn(orig_arn, fake_account_id)
        mapping.append((orig_arn, fake_arn))
    mapping_table = f"map_{col.replace('/', '_')}"
    con.execute(f"CREATE TEMP TABLE {mapping_table} (original TEXT, fake TEXT)")
    con.executemany(f"INSERT INTO {mapping_table} (original, fake) VALUES (?, ?)", mapping)
    return mapping_table

def main():
    args = parse_args()

    if args.create_config:
        config_file = args.config or "config.json"
        generate_config(args.input, config_file)
        sys.exit(0)

    if not args.config or not args.output:
        print("Error: --config and --output are required unless --create-config is used.")
        sys.exit(1)

    with open(args.config, 'r') as f:
        config = json.load(f)
    column_actions = config["columns"]

    con = duckdb.connect()
    con.execute(f"CREATE TABLE cur AS SELECT * FROM read_parquet('{args.input}')")

    all_cols = [row[0] for row in con.execute("PRAGMA table_info(cur)").fetchall()]
    keep_cols = [col for col, action in column_actions.items() if action in ("keep", "awsid_anonymise", "awsarn_anonymise")]
    anonymise_awsid_cols = [col for col, action in column_actions.items() if action == "awsid_anonymise"]
    anonymise_arn_cols = [col for col, action in column_actions.items() if action == "awsarn_anonymise"]

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
        else:
            select_cols.append(f"cur.\"{col}\"")

    select_sql = f"SELECT {', '.join(select_cols)} FROM cur " + " ".join(join_clauses)
    con.execute(f"COPY ({select_sql}) TO '{args.output}' (FORMAT PARQUET)")
    print(f"Anonymised file written to {args.output}")

if __name__ == "__main__":
    main()

