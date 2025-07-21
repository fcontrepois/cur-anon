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

def test_legacy_uuid_anonymisation():
    # Prepare temp files
    temp_dir = tempfile.mkdtemp()
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