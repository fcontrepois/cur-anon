# test_curanonymiser_legacy.py
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

# (No tests for CLI entry point; see integration tests for end-to-end validation.) 
import subprocess
import csv
import os
import uuid
import shutil
import tempfile
import pytest
import json

@pytest.mark.parametrize("help_flag", ["--help", "-h"])
def test_curanonymiser_legacy_cli_help(help_flag):
    # Test that CLI help returns usage info and exits 0
    result = subprocess.run([
        'python3', os.path.join(os.path.dirname(__file__), '../python/curanonymiser_legacy.py'),
        help_flag
    ], capture_output=True, text=True)
    assert result.returncode == 0
    assert 'usage' in result.stdout.lower() or 'help' in result.stdout.lower()

def test_legacy_uuid_anonymisation():
    # Prepare temp files
    with tempfile.TemporaryDirectory() as temp_dir:
        input_parquet = os.path.join(os.path.dirname(__file__), 'sample_cur2.parquet')
        config_json = os.path.join(os.path.dirname(__file__), 'config_cur2.json')
        output_csv = os.path.join(temp_dir, 'output_legacy_uuid.csv')
        config_path = os.path.join(temp_dir, 'config_uuid.json')
        shutil.copy(config_json, config_path)
        # Run the legacy anonymiser CLI
        subprocess.run([
            'python3', os.path.join(os.path.dirname(__file__), '../python/curanonymiser_legacy.py'),
            '--input', input_parquet,
            '--output', output_csv,
            '--config', config_path
        ], check=True)
        # Assert output file exists and is not empty
        assert os.path.exists(output_csv)
        assert os.path.getsize(output_csv) > 0
        # Read output and check uuid anonymisation
        with open(output_csv, newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        uuids = [row['bill_billing_entity'] for row in rows]
        for val in uuids:
            u = uuid.UUID(val)
            assert u.version == 5
        # All values should be the same UUID since all input values are 'AWS'
        assert len(set(uuids)) == 1

@pytest.mark.parametrize("bad_json", [
    '{bad json',
    '',
    '{"columns": "notadict"}',
    '{"columns": {"col": "notanaction"}}'
])
def test_curanonymiser_legacy_negative_cases(bad_json):
    with tempfile.TemporaryDirectory() as temp_dir:
        input_parquet = os.path.join(os.path.dirname(__file__), 'sample_cur2.parquet')
        config_path = os.path.join(temp_dir, 'bad_config.json')
        output_csv = os.path.join(temp_dir, 'output.csv')
        # Malformed config
        with open(config_path, 'w') as f:
            f.write(bad_json)
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.run([
                'python3', os.path.join(os.path.dirname(__file__), '../python/curanonymiser_legacy.py'),
                '--input', input_parquet,
                '--output', output_csv,
                '--config', config_path
            ], check=True)
        # Missing file
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.run([
                'python3', os.path.join(os.path.dirname(__file__), '../python/curanonymiser_legacy.py'),
                '--input', 'nonexistent.parquet',
                '--output', output_csv,
                '--config', config_path
            ], check=True)
        # Config with missing column
        config_path2 = os.path.join(temp_dir, 'missing_col_config.json')
        with open(config_path2, 'w') as f:
            json.dump({"_comment": "test", "columns": {"NotAColumn": "hash"}}, f)
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.run([
                'python3', os.path.join(os.path.dirname(__file__), '../python/curanonymiser_legacy.py'),
                '--input', input_parquet,
                '--output', output_csv,
                '--config', config_path2
            ], check=True) 