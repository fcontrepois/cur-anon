# anonymiser_common.py
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

import random
import re
import os  # Ensure os is available for all functions
from typing import Any, List, Optional

class AnonymiserInputError(Exception):
    """Raised when input file validation fails for anonymiser."""
    pass

# =====================
# Core Anonymiser Logic
# =====================

def generate_fake_aws_account_id(original_id: Any) -> str:
    """
    Generate a deterministic fake 12-digit AWS account ID based on the original ID.
    """
    random.seed(str(original_id))
    return ''.join(random.choices('0123456789', k=12))


def generate_fake_arn(original_arn: str, fake_account_id: str) -> str:
    """
    Generate a fake ARN by replacing the account-id part with the fake account ID, if present.
    """
    original_arn = str(original_arn)  # Ensure string type
    arn_regex = r"^arn:([^:]+):([^:]*):([^:]*):([^:]*):(.+)$"
    m = re.match(arn_regex, original_arn)
    if m:
        parts = list(m.groups())
        if parts[3]:
            parts[3] = fake_account_id
            return f"arn:{':'.join(parts)}"
        else:
            return original_arn
    else:
        return original_arn


def build_awsid_mapping(con: Any, table: str, col: str) -> str:
    """
    Build a mapping table in DuckDB for AWS account IDs to fake IDs.
    """
    unique_ids = con.execute(f'SELECT DISTINCT "{col}" FROM {table} WHERE "{col}" IS NOT NULL').fetchall()
    mapping = []
    for (orig_id,) in unique_ids:
        fake_id = generate_fake_aws_account_id(orig_id)
        mapping.append((orig_id, fake_id))
    mapping_table = f"map_{col.replace('/', '_').replace('.', '_')}"
    con.execute(f"CREATE TEMP TABLE {mapping_table} (original TEXT, fake TEXT)")
    if mapping:
        con.executemany(f"INSERT INTO {mapping_table} (original, fake) VALUES (?, ?)", mapping)
    return mapping_table


def build_arn_mapping(con: Any, table: str, col: str, account_col: str, account_mapping_table: str) -> str:
    """
    Build a mapping table in DuckDB for ARNs to fake ARNs using the fake account ID mapping.
    """
    unique_arns = con.execute(
        f'SELECT DISTINCT cur."{col}", cur."{account_col}" FROM {table} cur WHERE cur."{col}" IS NOT NULL'
    ).fetchall()
    account_map = dict(con.execute(f'SELECT original, fake FROM {account_mapping_table}').fetchall())
    mapping = []
    for orig_arn, orig_account_id in unique_arns:
        fake_account_id = account_map.get(str(orig_account_id), generate_fake_aws_account_id(orig_account_id))
        fake_arn = generate_fake_arn(orig_arn, fake_account_id)
        mapping.append((orig_arn, fake_arn))
    mapping_table = f"map_{col.replace('/', '_').replace('.', '_')}"
    con.execute(f"CREATE TEMP TABLE {mapping_table} (original TEXT, fake TEXT)")
    if mapping:
        con.executemany(f"INSERT INTO {mapping_table} (original, fake) VALUES (?, ?)", mapping)
    return mapping_table 


def build_uuid_mapping(con: Any, table: str, col: str) -> str:
    """
    Build a mapping table in DuckDB for a column, mapping each unique value to a deterministic UUID (consistent for each unique input value).
    """
    import uuid
    unique_values = con.execute(f'SELECT DISTINCT "{col}" FROM {table} WHERE "{col}" IS NOT NULL').fetchall()
    mapping = []
    for (orig_val,) in unique_values:
        fake_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(orig_val)))
        mapping.append((orig_val, fake_uuid))
    mapping_table = f"uuid_map_{col.replace('/', '_').replace('.', '_')}"
    con.execute(f"CREATE TEMP TABLE {mapping_table} (original TEXT, fake TEXT)")
    if mapping:
        con.executemany(f"INSERT INTO {mapping_table} (original, fake) VALUES (?, ?)", mapping)
    return mapping_table 


def generate_config(columns: List[str], mode: str = "legacy") -> dict:
    """
    Generate a config dict for anonymisation. Never assign 'uuid' by default.
    mode: 'legacy', 'cur2', or 'focus' (affects AWS-specific logic)
    """
    focus_hash_cols = {"BillingAccountId", "BillingAccountName", "SubAccountId", "SubAccountIdName", "InvoiceId", "tag"}
    config = {
        "_comment": (
            "Column options: 'keep' (keep as is), 'remove' (remove column), "
            "'awsid_anonymise' (anonymise as AWS account ID), "
            "'awsarn_anonymise' (anonymise as AWS ARN using fake account ID), "
            "'hash' (hash the column using DuckDB's md5_number_upper), "
            "'uuid' (replace with consistent UUID, only if explicitly set)"
        ),
        "columns": {}
    }
    for col in columns:
        col_lower = col.lower()
        if mode in ("legacy", "cur2"):
            if "account_id" in col_lower or "usageaccountid" in col_lower or "payeraccountid" in col_lower:
                config["columns"][col] = "awsid_anonymise"
            elif "resourcetags" in col_lower or "resource_tags" in col_lower:
                config["columns"][col] = "hash"
            elif "a_r_n" in col_lower or "arn" in col_lower:
                config["columns"][col] = "awsarn_anonymise"
            else:
                config["columns"][col] = "keep"
        elif mode == "focus":
            if col in focus_hash_cols:
                config["columns"][col] = "hash"
            else:
                config["columns"][col] = "keep"
    return config 

# =====================
# Shared CLI and File Utilities
# =====================

def parse_args(description: str, epilog: str):
    """
    Parse CLI arguments for anonymiser scripts.
    """
    import argparse
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog
    )
    parser.add_argument('--input', required=False, help='Input file (CSV or Parquet)')
    parser.add_argument('--output', required=False, help='Output file (CSV or Parquet)')
    parser.add_argument('--config', required=False, help='JSON config file for column handling')
    parser.add_argument('--create-config', action='store_true', help='Create a config file from the input file')
    parser.add_argument('--version', action='version', version='anonymiser 1.0')
    return parser.parse_args()

def validate_input_file(input_file: str) -> None:
    """
    Validate that the input file is not empty (0 bytes).
    Raises AnonymiserInputError if invalid.
    """
    size = os.path.getsize(input_file)
    if size == 0:
        raise AnonymiserInputError("Input file is empty (0 bytes).")

def generate_config_entry(input_file: str, config_file: Optional[str] = None, mode: str = "legacy") -> None:
    """
    Generate a config file for the input file and mode. Raises AnonymiserInputError if file is empty or has no columns.
    """
    import duckdb
    import json
    size = os.path.getsize(input_file)
    con = duckdb.connect()
    ext = os.path.splitext(input_file)[1].lower()
    if ext == ".csv":
        df = con.execute(f"SELECT * FROM read_csv_auto('{input_file}') LIMIT 0").fetchdf()
        columns = list(df.columns)
        # If file is 0 bytes or only column is 'column0' and file is 0 bytes, treat as empty
        if size == 0 or (columns == ['column0'] and size == 0):
            raise AnonymiserInputError("Input file is empty (0 bytes, or DuckDB default column0 on empty file).")
    else:
        df = con.execute(f"SELECT * FROM read_parquet('{input_file}') LIMIT 0").fetchdf()
        columns = list(df.columns)
        if not columns:
            raise AnonymiserInputError("Input file has no columns (empty or header-only).")
    config = generate_config(columns, mode=mode)
    if config_file:
        with open(config_file, "w") as f:
            json.dump(config, f, indent=2)
        print(f"Config file created at {config_file}")
    else:
        print(json.dumps(config, indent=2)) 