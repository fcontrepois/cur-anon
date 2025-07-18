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
from typing import Any

def generate_fake_aws_account_id(original_id: Any) -> str:
    """Generate a deterministic fake 12-digit AWS account ID based on the original ID."""
    random.seed(str(original_id))
    return ''.join(random.choices('0123456789', k=12))


def generate_fake_arn(original_arn: str, fake_account_id: str) -> str:
    """Generate a fake ARN by replacing the account-id part with the fake account ID, if present."""
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
    """Build a mapping table in DuckDB for AWS account IDs to fake IDs."""
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
    """Build a mapping table in DuckDB for ARNs to fake ARNs using the fake account ID mapping."""
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