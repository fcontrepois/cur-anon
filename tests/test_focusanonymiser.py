import subprocess
import csv
import os
import shutil
import tempfile
import pytest
from collections import defaultdict
import json

@pytest.mark.parametrize("help_flag", ["--help", "-h"])
def test_focusanonymiser_cli_help(help_flag):
    # Test that CLI help returns usage info and exits 0
    result = subprocess.run([
        'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
        help_flag
    ], capture_output=True, text=True)
    assert result.returncode == 0
    assert 'usage' in result.stdout.lower() or 'help' in result.stdout.lower()

def test_focusanonymiser_hash_columns():
    # Prepare temp files
    with tempfile.TemporaryDirectory() as temp_dir:
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
        # Assert output file exists and is not empty
        assert os.path.exists(output_csv)
        assert os.path.getsize(output_csv) > 0
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

def test_focusanonymiser_output_structure():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_csv = os.path.join(os.path.dirname(__file__), 'sample_focus.csv')
        output_csv = os.path.join(temp_dir, 'output_focus.csv')
        config_path = os.path.join(temp_dir, 'config_focus.json')
        subprocess.run([
            'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
            '--input', input_csv,
            '--create-config',
            '--config', config_path
        ], check=True)
        subprocess.run([
            'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
            '--input', input_csv,
            '--output', output_csv,
            '--config', config_path
        ], check=True)
        assert os.path.exists(output_csv)
        assert os.path.getsize(output_csv) > 0
        with open(input_csv, newline='') as f:
            reader = csv.DictReader(f)
            input_rows = list(reader)
            input_headers = reader.fieldnames
        with open(output_csv, newline='') as f:
            reader = csv.DictReader(f)
            output_rows = list(reader)
            output_headers = reader.fieldnames
        # Output should have same number of rows as input
        assert len(input_rows) == len(output_rows)
        # Output should have only columns present in config as 'keep', 'hash', or 'uuid'
        with open(config_path) as f:
            config = json.load(f)
        allowed_cols = [col for col, action in config['columns'].items() if action in ('keep', 'hash', 'uuid')]
        if output_headers:
            for col in output_headers:
                assert col in allowed_cols
            # No extra columns
            for col in output_headers:
                assert col in allowed_cols
            # All allowed columns present
            for col in allowed_cols:
                assert col in output_headers

@pytest.mark.parametrize("bad_json", [
    '{bad json',
    '',
    '{"columns": "notadict"}',
    '{"columns": {"col": "notanaction"}}'
])
def test_focusanonymiser_negative_cases(bad_json):
    with tempfile.TemporaryDirectory() as temp_dir:
        input_csv = os.path.join(os.path.dirname(__file__), 'sample_focus.csv')
        config_path = os.path.join(temp_dir, 'bad_config.json')
        output_csv = os.path.join(temp_dir, 'output.csv')
        # Malformed config
        with open(config_path, 'w') as f:
            f.write(bad_json)
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.run([
                'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
                '--input', input_csv,
                '--output', output_csv,
                '--config', config_path
            ], check=True)
        # Missing file
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.run([
                'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
                '--input', 'nonexistent.csv',
                '--output', output_csv,
                '--config', config_path
            ], check=True)
        # Config with missing column
        config_path2 = os.path.join(temp_dir, 'missing_col_config.json')
        with open(config_path2, 'w') as f:
            json.dump({"_comment": "test", "columns": {"NotAColumn": "hash"}}, f)
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.run([
                'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
                '--input', input_csv,
                '--output', output_csv,
                '--config', config_path2
            ], check=True)

def test_focusanonymiser_empty_and_header_only():
    with tempfile.TemporaryDirectory() as temp_dir:
        empty_csv = os.path.join(temp_dir, 'empty.csv')
        header_only_csv = os.path.join(temp_dir, 'header_only.csv')
        with open(header_only_csv, 'w') as f:
            f.write('BillingAccountId,BillingAccountName\n')
        with open(empty_csv, 'w') as f:
            f.write('')
        config_path = os.path.join(temp_dir, 'config.json')
        # Should not crash on header only
        subprocess.run([
            'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
            '--input', header_only_csv,
            '--create-config',
            '--config', config_path
        ], check=True)
        subprocess.run([
            'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
            '--input', header_only_csv,
            '--output', os.path.join(temp_dir, 'out.csv'),
            '--config', config_path
        ], check=True)
        # Should fail on truly empty file
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.run([
                'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
                '--input', empty_csv,
                '--create-config',
                '--config', config_path
            ], check=True)

def test_focusanonymiser_uuid_column():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_csv = os.path.join(os.path.dirname(__file__), 'sample_focus.csv')
        output_csv = os.path.join(temp_dir, 'output_uuid.csv')
        config_path = os.path.join(temp_dir, 'config_uuid.json')
        # Generate config and add a uuid column
        subprocess.run([
            'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
            '--input', input_csv,
            '--create-config',
            '--config', config_path
        ], check=True)
        with open(config_path) as f:
            config = json.load(f)
        # Pick a column to set as uuid (if available)
        uuid_col = None
        for col in config['columns']:
            if col not in ('BillingAccountId', 'BillingAccountName'):
                uuid_col = col
                break
        if uuid_col:
            config['columns'][uuid_col] = 'uuid'
            with open(config_path, 'w') as f:
                json.dump(config, f)
            subprocess.run([
                'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
                '--input', input_csv,
                '--output', output_csv,
                '--config', config_path
            ], check=True)
            assert os.path.exists(output_csv)
            assert os.path.getsize(output_csv) > 0
            with open(input_csv, newline='') as f:
                reader = csv.DictReader(f)
                input_rows = list(reader)
            with open(output_csv, newline='') as f:
                reader = csv.DictReader(f)
                output_rows = list(reader)
            import uuid
            mapping = defaultdict(set)
            for inrow, outrow in zip(input_rows, output_rows):
                mapping[inrow[uuid_col]].add(outrow[uuid_col])
                u = uuid.UUID(outrow[uuid_col])
                assert u.version == 5
            for outs in mapping.values():
                assert len(outs) == 1

def test_focusanonymiser_cross_format_consistency():
    with tempfile.TemporaryDirectory() as temp_dir:
        input_csv = os.path.join(os.path.dirname(__file__), 'sample_focus.csv')
        input_parquet = os.path.join(temp_dir, 'sample_focus.parquet')
        # Convert CSV to Parquet using DuckDB
        import duckdb
        duckdb.sql(f"COPY (SELECT * FROM read_csv_auto('{input_csv}')) TO '{input_parquet}' (FORMAT PARQUET)")
        config_path = os.path.join(temp_dir, 'config.json')
        subprocess.run([
            'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
            '--input', input_csv,
            '--create-config',
            '--config', config_path
        ], check=True)
        output_csv = os.path.join(temp_dir, 'output_csv.csv')
        output_parquet = os.path.join(temp_dir, 'output_parquet.csv')
        subprocess.run([
            'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
            '--input', input_csv,
            '--output', output_csv,
            '--config', config_path
        ], check=True)
        subprocess.run([
            'python3', os.path.join(os.path.dirname(__file__), '../python/focusanonymiser.py'),
            '--input', input_parquet,
            '--output', output_parquet,
            '--config', config_path
        ], check=True)
        assert os.path.exists(output_csv)
        assert os.path.exists(output_parquet)
        with open(output_csv, newline='') as f:
            reader = csv.DictReader(f)
            rows_csv = list(reader)
        with open(output_parquet, newline='') as f:
            reader = csv.DictReader(f)
            rows_parquet = list(reader)
        assert len(rows_csv) == len(rows_parquet)
        if rows_csv:
            for col in rows_csv[0]:
                vals_csv = [row[col] for row in rows_csv]
                vals_parquet = [row[col] for row in rows_parquet]
                assert vals_csv == vals_parquet 