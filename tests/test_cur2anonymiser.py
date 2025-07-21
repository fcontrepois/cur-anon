# test_cur2anonymiser.py
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

import pytest
import re
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'python')))

# Test generate_fake_aws_account_id
@pytest.mark.parametrize("original_id", ["123456789012", "000000000000", "999999999999", "abc", 123456789012])
def test_generate_fake_aws_account_id_length_and_digits(original_id):
    import importlib
    cur2anonymiser = importlib.import_module('cur2anonymiser')
    fake_id = cur2anonymiser.generate_fake_aws_account_id(original_id)
    assert isinstance(fake_id, str)
    assert len(fake_id) == 12
    assert fake_id.isdigit()

# Test that the same input always gives the same output
@pytest.mark.parametrize("original_id", ["123456789012", "000000000000", "999999999999", "abc", 123456789012])
def test_generate_fake_aws_account_id_deterministic(original_id):
    import importlib
    cur2anonymiser = importlib.import_module('cur2anonymiser')
    fake_id1 = cur2anonymiser.generate_fake_aws_account_id(original_id)
    fake_id2 = cur2anonymiser.generate_fake_aws_account_id(original_id)
    assert fake_id1 == fake_id2

# Test generate_fake_arn
@pytest.mark.parametrize(
    "original_arn,fake_account_id,expected_pattern",
    [
        ("arn:aws:iam::123456789012:user/Bob", "000000000000", r"^arn:aws:iam::000000000000:.*$"),
        ("arn:aws:s3:::mybucket", "111111111111", r"^arn:aws:s3:::[^:]+$"),
        ("not-an-arn", "222222222222", r"^not-an-arn$")
    ]
)
def test_generate_fake_arn(original_arn, fake_account_id, expected_pattern):
    import importlib
    cur2anonymiser = importlib.import_module('cur2anonymiser')
    fake_arn = cur2anonymiser.generate_fake_arn(original_arn, fake_account_id)
    assert re.match(expected_pattern, fake_arn)

# Test generate_config logic (mocking duckdb)
def test_generate_config_assigns_column_types(tmp_path):
    import subprocess
    import json
    import os
    # Use the real sample file (CSV or Parquet)
    input_file = os.path.join(os.path.dirname(__file__), 'sample_cur2.parquet')
    config_file = tmp_path / 'config.json'
    subprocess.run([
        'python3', os.path.join(os.path.dirname(__file__), '../python/cur2anonymiser.py'),
        '--input', input_file,
        '--create-config',
        '--config', str(config_file)
    ], check=True)
    with open(config_file) as f:
        config = json.load(f)
    cols = config["columns"]
    assert cols["line_item_usage_account_id"] == "awsid_anonymise"
    assert cols["bill_payer_account_id"] == "awsid_anonymise"
    assert cols["line_item_resource_id"] == "keep" or cols["line_item_resource_id"] == "awsarn_anonymise"
    assert cols["product_instance_type"] == "keep"
    assert cols["resource_tags"] == "hash"
    assert cols["product_region_code"] == "keep"

def test_full_csv_anonymisation(tmp_path):
    import shutil
    import csv
    import os
    # Copy config to temp dir
    config_json = os.path.join(os.path.dirname(__file__), 'config_cur2.json')
    output_csv = tmp_path / 'output.csv'
    config_path = tmp_path / 'config.json'
    shutil.copy(config_json, config_path)

    # Use the pre-converted Parquet file as input
    input_parquet = os.path.join(os.path.dirname(__file__), 'sample_cur2.parquet')

    # Run anonymisation (simulate CLI call)
    import sys
    sys.argv = ['cur2anonymiser.py', '--input', input_parquet, '--output', str(output_csv), '--config', str(config_path)]
    import importlib
    cur2anonymiser = importlib.import_module('cur2anonymiser')
    cur2anonymiser.main()

    # Read output and check anonymisation
    with open(output_csv, newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Check that account IDs are anonymised and consistent
    payer_ids = set(row['bill_payer_account_id'] for row in rows)
    usage_ids = set(row['line_item_usage_account_id'] for row in rows)
    # All should be 12-digit numbers, not the original
    for pid in payer_ids:
        assert pid != '123456789012'
        assert len(pid) == 12 and pid.isdigit()
    for uid in usage_ids:
        assert uid not in ['234567890123', '345678901234', '456789012345']
        assert len(uid) == 12 and uid.isdigit()
    # Check that ARNs are anonymised (account id replaced)
    for row in rows:
        for arn_col in ['reservation_reservation_a_r_n', 'savings_plan_savings_plan_a_r_n']:
            arn = row[arn_col]
            assert '123456789012' not in arn
            assert arn.startswith('arn:') 

def test_uuid_column_anonymisation(tmp_path):
    import shutil
    import csv
    import os
    # Copy config to temp dir
    config_json = os.path.join(os.path.dirname(__file__), 'config_cur2.json')
    output_csv = tmp_path / 'output_uuid.csv'
    config_path = tmp_path / 'config_uuid.json'
    shutil.copy(config_json, config_path)

    # Use the pre-converted Parquet file as input
    input_parquet = os.path.join(os.path.dirname(__file__), 'sample_cur2.parquet')

    # Run anonymisation (simulate CLI call)
    import sys
    sys.argv = ['cur2anonymiser.py', '--input', input_parquet, '--output', str(output_csv), '--config', str(config_path)]
    import importlib
    cur2anonymiser = importlib.import_module('cur2anonymiser')
    cur2anonymiser.main()

    # Read output and check uuid anonymisation
    with open(output_csv, newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    uuids = [row['bill_billing_entity'] for row in rows]
    # All should be valid UUIDs (version 5, deterministic)
    import uuid
    for val in uuids:
        u = uuid.UUID(val)
        assert u.version == 5
    # All values should be the same UUID since all input values are 'AWS'
    assert len(set(uuids)) == 1 