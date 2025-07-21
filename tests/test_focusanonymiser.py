import subprocess
import csv
import os
import shutil
import tempfile
import pytest
from collections import defaultdict
import json

def test_focusanonymiser_hash_columns():
    # Prepare temp files
    temp_dir = tempfile.mkdtemp()
    input_csv = os.path.join(os.path.dirname(__file__), 'sample_focus.csv')
    output_csv = os.path.join(temp_dir, 'output_focus.csv')
    # Generate config using focusanonymiser
    config_path = os.path.join(temp_dir, 'config_focus.json')
    subprocess.run([
        'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
        '--input', input_csv,
        '--create-config',
        '--config', config_path
    ], check=True)
    # Run the focusanonymiser CLI
    subprocess.run([
        'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
        '--input', input_csv,
        '--output', output_csv,
        '--config', config_path
    ], check=True)
    # Read input and output
    with open(input_csv, newline='') as f:
        reader = csv.DictReader(f)
        input_rows = list(reader)
    with open(output_csv, newline='') as f:
        reader = csv.DictReader(f)
        output_rows = list(reader)
    # Read config to get hash columns
    with open(config_path) as f:
        config = json.load(f)
    hash_cols = [col for col, action in config['columns'].items() if action == 'hash']
    # Only check columns that are present in the input data
    present_hash_cols = [col for col in hash_cols if col in input_rows[0] and col in output_rows[0]]
    for col in present_hash_cols:
        mapping = defaultdict(set)
        for inrow, outrow in zip(input_rows, output_rows):
            assert outrow[col] != inrow[col]
            assert outrow[col] != ''
            mapping[inrow[col]].add(outrow[col])
        # For each unique input value, there should be exactly one output value (hash is consistent)
        for outs in mapping.values():
            assert len(outs) == 1
    # Check that a keep column is unchanged
    for inrow, outrow in zip(input_rows, output_rows):
        if "BillingAccountType" in inrow and "BillingAccountType" in outrow:
            assert outrow["BillingAccountType"] == inrow["BillingAccountType"] 