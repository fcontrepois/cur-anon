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
import cur2anonymiser

# Test generate_fake_aws_account_id
@pytest.mark.parametrize("original_id", ["123456789012", "000000000000", "999999999999", "abc", 123456789012])
def test_generate_fake_aws_account_id_length_and_digits(original_id):
    fake_id = cur2anonymiser.generate_fake_aws_account_id(original_id)
    assert isinstance(fake_id, str)
    assert len(fake_id) == 12
    assert fake_id.isdigit()

# Test that the same input always gives the same output
@pytest.mark.parametrize("original_id", ["123456789012", "000000000000", "999999999999", "abc", 123456789012])
def test_generate_fake_aws_account_id_deterministic(original_id):
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
    fake_arn = cur2anonymiser.generate_fake_arn(original_arn, fake_account_id)
    assert re.match(expected_pattern, fake_arn)

# Test generate_config logic (mocking duckdb)
def test_generate_config_assigns_column_types(monkeypatch, tmp_path):
    class DummyCon:
        def execute(self, sql):
            class DummyDF:
                columns = [
                    "line_item_usage_account_id",
                    "bill_payer_account_id",
                    "line_item_resource_id",
                    "product_instance_type",
                    "resource_tags_user_costcentre",
                    "product_region"
                ]
                def fetchdf(self):
                    return self
            return DummyDF()
    monkeypatch.setattr(cur2anonymiser.duckdb, "connect", lambda: DummyCon())
    config_file = tmp_path / "config.json"
    cur2anonymiser.generate_config("dummy.parquet", str(config_file))
    import json
    with open(config_file) as f:
        config = json.load(f)
    cols = config["columns"]
    assert cols["line_item_usage_account_id"] == "awsid_anonymise"
    assert cols["bill_payer_account_id"] == "awsid_anonymise"
    assert cols["line_item_resource_id"] == "keep" or cols["line_item_resource_id"] == "awsarn_anonymise"
    assert cols["product_instance_type"] == "keep"
    assert cols["resource_tags_user_costcentre"] == "hash"
    assert cols["product_region"] == "keep" 