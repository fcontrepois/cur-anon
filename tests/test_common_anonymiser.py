import os
import tempfile
import pytest
from tests.test_utils import run_cli, read_csv, read_json
from collections import defaultdict
import json
import uuid

ANONYMISERS = [
    {
        "name": "cur2",
        "script": "../python/cur2anonymiser.py",
        "sample": "sample_cur2.parquet",
        "config": "config_cur2.json",
        "format": "parquet"
    },
    {
        "name": "legacy",
        "script": "../python/curanonymiser_legacy.py",
        "sample": "sample_cur2.parquet",
        "config": "config_cur2.json",
        "format": "parquet"
    },
    {
        "name": "focus",
        "script": "../python/focusanonymiser.py",
        "sample": "sample_focus.csv",
        "config": "config_focus.json",
        "format": "csv"
    }
]

@pytest.mark.parametrize("anonymiser", ANONYMISERS, ids=[a["name"] for a in ANONYMISERS])
def test_cli_help(anonymiser):
    script = os.path.join(os.path.dirname(__file__), anonymiser["script"])
    result = run_cli(script, ["--help"], check=False, capture_output=True)
    assert result.returncode == 0
    assert "usage" in result.stdout.lower() or "help" in result.stdout.lower()

@pytest.mark.parametrize("anonymiser", ANONYMISERS, ids=[a["name"] for a in ANONYMISERS])
def test_config_generation(anonymiser):
    script = os.path.join(os.path.dirname(__file__), anonymiser["script"])
    sample = os.path.join(os.path.dirname(__file__), anonymiser["sample"])
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, 'config.json')
        run_cli(script, ["--input", sample, "--create-config", "--config", config_path], check=True)
        assert os.path.exists(config_path)
        config = read_json(config_path)
        assert "columns" in config

@pytest.mark.parametrize("anonymiser", ANONYMISERS, ids=[a["name"] for a in ANONYMISERS])
def test_negative_cases(anonymiser):
    script = os.path.join(os.path.dirname(__file__), anonymiser["script"])
    sample = os.path.join(os.path.dirname(__file__), anonymiser["sample"])
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, 'bad_config.json')
        output_path = os.path.join(temp_dir, 'output.csv')
        # Malformed config
        for bad_json in ['{bad json', '', '{"columns": "notadict"}', '{"columns": {"col": "notanaction"}}']:
            with open(config_path, 'w') as f:
                f.write(bad_json)
            with pytest.raises(Exception):
                run_cli(script, ["--input", sample, "--output", output_path, "--config", config_path], check=True)
        # Missing file
        with pytest.raises(Exception):
            run_cli(script, ["--input", 'nonexistent.file', "--output", output_path, "--config", config_path], check=True)
        # Config with missing column
        config_path2 = os.path.join(temp_dir, 'missing_col_config.json')
        with open(config_path2, 'w') as f:
            f.write(json.dumps({"_comment": "test", "columns": {"NotAColumn": "hash"}}))
        with pytest.raises(Exception):
            run_cli(script, ["--input", sample, "--output", output_path, "--config", config_path2], check=True)

@pytest.mark.parametrize("anonymiser", ANONYMISERS, ids=[a["name"] for a in ANONYMISERS])
def test_empty_and_header_only(anonymiser):
    script = os.path.join(os.path.dirname(__file__), anonymiser["script"])
    with tempfile.TemporaryDirectory() as temp_dir:
        if anonymiser["format"] == "csv":
            empty_file = os.path.join(temp_dir, 'empty.csv')
            header_only_file = os.path.join(temp_dir, 'header_only.csv')
            with open(header_only_file, 'w') as f:
                f.write('col1,col2\n')
            with open(empty_file, 'w') as f:
                f.write('')
        else:
            empty_file = os.path.join(temp_dir, 'empty.parquet')
            header_only_file = os.path.join(temp_dir, 'header_only.parquet')
            import duckdb
            table_name = f"t_{uuid.uuid4().hex[:8]}"
            con = duckdb.connect()
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} (col1 VARCHAR, col2 VARCHAR)")
            con.execute(f"COPY {table_name} TO '{header_only_file}' (FORMAT PARQUET)")
            con.close()
            with open(empty_file, 'wb') as f:
                pass
        config_path = os.path.join(temp_dir, 'config.json')
        # Should not crash on header only
        run_cli(script, ["--input", header_only_file, "--create-config", "--config", config_path], check=True)
        run_cli(script, ["--input", header_only_file, "--output", os.path.join(temp_dir, 'out.file'), "--config", config_path], check=True)
        # Should fail on truly empty file (no columns)
        with pytest.raises(Exception):
            run_cli(script, ["--input", empty_file, "--create-config", "--config", config_path], check=True) 

@pytest.mark.parametrize("anonymiser", ANONYMISERS, ids=[a["name"] for a in ANONYMISERS])
def test_hash_columns(anonymiser):
    script = os.path.join(os.path.dirname(__file__), anonymiser["script"])
    sample = os.path.join(os.path.dirname(__file__), anonymiser["sample"])
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = os.path.join(temp_dir, 'output.csv')
        config_path = os.path.join(temp_dir, 'config.json')
        # Generate config
        run_cli(script, ["--input", sample, "--create-config", "--config", config_path], check=True)
        # Run anonymiser
        run_cli(script, ["--input", sample, "--output", output_file, "--config", config_path], check=True)
        # Assert output file exists and is not empty
        assert os.path.exists(output_file)
        assert os.path.getsize(output_file) > 0
        # Read input and output (CSV only for focus, for others, output is always CSV)
        if anonymiser["format"] == "csv":
            input_rows = read_csv(sample)
        else:
            # For Parquet, convert to CSV using DuckDB for comparison
            import duckdb
            csv_temp = os.path.join(temp_dir, 'input_as_csv.csv')
            duckdb.sql(f"COPY (SELECT * FROM read_parquet('{sample}')) TO '{csv_temp}' (FORMAT CSV, HEADER 1)")
            input_rows = read_csv(csv_temp)
        output_rows = read_csv(output_file)
        # Read config to get hash columns
        config = read_json(config_path)
        hash_cols = [col for col, action in config['columns'].items() if action == 'hash']
        if input_rows and output_rows:
            present_hash_cols = [col for col in hash_cols if col in input_rows[0] and col in output_rows[0]]
            for col in present_hash_cols:
                mapping = defaultdict(set)
                for inrow, outrow in zip(input_rows, output_rows):
                    assert outrow[col] != inrow[col]
                    assert outrow[col] != ''
                    mapping[inrow[col]].add(outrow[col])
                for outs in mapping.values():
                    assert len(outs) == 1

@pytest.mark.parametrize("anonymiser", ANONYMISERS, ids=[a["name"] for a in ANONYMISERS])
def test_output_structure(anonymiser):
    script = os.path.join(os.path.dirname(__file__), anonymiser["script"])
    sample = os.path.join(os.path.dirname(__file__), anonymiser["sample"])
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = os.path.join(temp_dir, 'output.csv')
        config_path = os.path.join(temp_dir, 'config.json')
        run_cli(script, ["--input", sample, "--create-config", "--config", config_path], check=True)
        run_cli(script, ["--input", sample, "--output", output_file, "--config", config_path], check=True)
        assert os.path.exists(output_file)
        assert os.path.getsize(output_file) > 0
        if anonymiser["format"] == "csv":
            input_rows = read_csv(sample)
            input_headers = input_rows[0].keys() if input_rows else []
        else:
            import duckdb
            csv_temp = os.path.join(temp_dir, 'input_as_csv.csv')
            duckdb.sql(f"COPY (SELECT * FROM read_parquet('{sample}')) TO '{csv_temp}' (FORMAT CSV, HEADER 1)")
            input_rows = read_csv(csv_temp)
            input_headers = input_rows[0].keys() if input_rows else []
        output_rows = read_csv(output_file)
        output_headers = output_rows[0].keys() if output_rows else []
        assert len(input_rows) == len(output_rows)
        config = read_json(config_path)
        allowed_cols = [col for col, action in config['columns'].items() if action in ('keep', 'hash', 'uuid', 'awsid_anonymise', 'awsarn_anonymise')]
        if output_headers:
            for col in output_headers:
                assert col in allowed_cols
            for col in allowed_cols:
                assert col in output_headers

@pytest.mark.parametrize("anonymiser", ANONYMISERS, ids=[a["name"] for a in ANONYMISERS])
def test_cross_format_consistency(anonymiser):
    # Only run for focus (CSV/Parquet) as a demonstration; skip for others
    if anonymiser["name"] != "focus":
        pytest.skip("Cross-format consistency only relevant for focus test data.")
    script = os.path.join(os.path.dirname(__file__), anonymiser["script"])
    input_csv = os.path.join(os.path.dirname(__file__), 'sample_focus.csv')
    with tempfile.TemporaryDirectory() as temp_dir:
        input_parquet = os.path.join(temp_dir, 'sample_focus.parquet')
        import duckdb
        duckdb.sql(f"COPY (SELECT * FROM read_csv_auto('{input_csv}')) TO '{input_parquet}' (FORMAT PARQUET)")
        config_path = os.path.join(temp_dir, 'config.json')
        run_cli(script, ["--input", input_csv, "--create-config", "--config", config_path], check=True)
        output_csv = os.path.join(temp_dir, 'output_csv.csv')
        output_parquet = os.path.join(temp_dir, 'output_parquet.csv')
        run_cli(script, ["--input", input_csv, "--output", output_csv, "--config", config_path], check=True)
        run_cli(script, ["--input", input_parquet, "--output", output_parquet, "--config", config_path], check=True)
        rows_csv = read_csv(output_csv)
        rows_parquet = read_csv(output_parquet)
        assert len(rows_csv) == len(rows_parquet)
        if rows_csv:
            for col in rows_csv[0]:
                vals_csv = [row[col] for row in rows_csv]
                vals_parquet = [row[col] for row in rows_parquet]
                assert vals_csv == vals_parquet 