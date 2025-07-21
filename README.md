# cur-anon

A brisk, no-nonsense tool for anonymising AWS Cost and Usage Report (CUR) Parquet files. Remove or mask sensitive details, preserve data utility, and share your reports without breaking a sweat—or a compliance rule.

---

## ⚡️ Quick Usage Examples

**CUR2:**
- Generate config: `python python/cur2anonymiser.py --input rawcur2.parquet --create-config --config config_cur2.json`
- Anonymise: `python python/cur2anonymiser.py --input rawcur2.parquet --output anonymisedcur2.parquet --config config_cur2.json`
- Anonymise to CSV: `python python/cur2anonymiser.py --input rawcur2.parquet --output anonymisedcur2.csv --config config_cur2.json`

**Legacy CUR:**
- Generate config: `python python/curanonymiser_legacy.py --input rawcur_legacy.parquet --create-config --config config_legacy.json`
- Anonymise: `python python/curanonymiser_legacy.py --input rawcur_legacy.parquet --output anonymisedcur_legacy.parquet --config config_legacy.json`
- Anonymise to CSV: `python python/curanonymiser_legacy.py --input rawcur_legacy.parquet --output anonymisedcur_legacy.csv --config config_legacy.json`

---

## 🚀 Features

- Supports both legacy AWS CUR and CUR2 formats (see below)
- Reads and writes Parquet (and CSV) via DuckDB; no Spark, no Java, no drama
- Anonymises AWS Account IDs and ARNs, ensuring consistency across the dataset
- Lets you hash columns, drop columns, or keep them as you fancy
- Simple, human-editable JSON config, easy to share, easy to tweak
- Auto-generates a config file from your Parquet columns
- CLI with helpful flags and no unnecessary faff
- MIT licensed, open source

---

## 🏁 Quick Start

### 1. Install the essentials

```sh
pip install -r requirements.txt
```

### 2. Choose your CUR format

- **CUR2:** All-lowercase, underscore-separated columns (e.g., `line_item_blended_cost`)
- **Legacy CUR:** Mixed case, slashes, or camelCase columns (e.g., `lineItem/UsageAccountId`)

### 3. Generate a config file

**For CUR2:**
```sh
python python/cur2anonymiser.py --input rawcur2.parquet --create-config --config config_cur2.json
```

**For Legacy CUR:**
```sh
python python/curanonymiser_legacy.py --input rawcur_legacy.parquet --create-config --config config_legacy.json
```

This produces a config listing all columns and their suggested actions. Edit it to choose which columns to keep, remove, anonymise, or hash.

### 4. Edit your config

Each column can be set to one of:

- `keep` – leave the column untouched
- `remove` – drop the column entirely
- `awsid_anonymise` – swap for a fake, consistent 12-digit AWS account ID
- `awsarn_anonymise` – swap for a fake ARN, using the fake account ID
- `hash` – scramble the column with DuckDB’s `md5_number_upper`, so the same value always produces the same hash, but there is no way back—perfect for secrets, not for magicians.
- `uuid` – replace the column value with a deterministic UUID (same input = same output, not reversible)

### 5. Run the anonymiser

**For CUR2:**
```sh
python python/cur2anonymiser.py --input rawcur2.parquet --output anonymisedcur2.parquet --config config_cur2.json
```

**For Legacy CUR:**
```sh
python python/curanonymiser_legacy.py --input rawcur_legacy.parquet --output anonymisedcur_legacy.parquet --config config_legacy.json
```

Or, if you prefer CSV:
```sh
python python/cur2anonymiser.py --input rawcur2.parquet --output anonymisedcur2.csv --config config_cur2.json
```

Voilà! Your anonymised file is ready for sharing, analysis, or waving triumphantly at your compliance officer.

---

## 📝 Example Config (CUR2)

```json
{
  "_comment": "Column options: 'keep', 'remove', 'awsid_anonymise', 'awsarn_anonymise', 'hash', 'uuid'",
  "columns": {
    "line_item_usage_account_id": "awsid_anonymise",
    "bill_payer_account_id": "awsid_anonymise",
    "line_item_resource_id": "awsarn_anonymise",
    "product_instance_type": "remove",
    "product_region": "keep",
    "line_item_usage_type": "keep",
    "resource_tags": "hash",
    "column6": "uuid"
  }
}
```

---

## 🛠 Handy Commands

**See your Parquet columns:**
```sh
duckdb -c "SELECT * FROM 'yourfile.parquet' LIMIT 0;"
```

**Export the first 100 rows as CSV:**
```sh
duckdb -c "COPY (SELECT * FROM 'yourfile.parquet' LIMIT 100) TO STDOUT (HEADER, DELIMITER ',');"
```

---

## 🧐 How It Works

- The script reads your input Parquet file and applies the actions specified in the config.
- Account IDs are replaced with consistent, fake 12-digit numbers.
- ARNs are rebuilt using the fake account IDs, so relationships are preserved.
- Columns set to `hash` are hashed with DuckDB’s `md5_number_upper` function—irreversible, but consistent (not cryptographically secure).
- Columns set to `remove` vanish without a trace. Columns set to `keep` are left alone, as nature intended.
- Output can be Parquet or CSV, depending on your mood.

---

## ❓ Flags & Usage

**Flags:**
- `--input`           Path to the input Parquet file (required)
- `--output`          Path to the output file (required, unless using `--create-config`)
- `--config`          Path to the JSON config file (required, unless using `--create-config`)
- `--create-config`   Generate a config file from the input Parquet file and exit
- `--help`            Show help and exit

---

## 🧪 Testing & Validation

- **Unit tests:**
  - Run all tests: `pytest tests/`
- **Integration tests:**
  - Provide a small sample CUR2 and legacy CUR file in your repo (or generate one) and run the full anonymisation flow.
- **Config validation:**
  - (Planned) Add a command to validate your config file against your input file, warning about missing or misconfigured columns.

---

## 📦 Packaging & Installation

- Install dependencies: `pip install -r requirements.txt`
- (Planned) PyPI packaging for easy install and CLI entry points.

---

## ⚠️ Security Note

- The hashing function (`md5_number_upper`) is for anonymisation, not for cryptographic security. Do not use for secrets that require strong protection.

---

## 📜 Licence

MIT. Because life’s too short for restrictive licences.

---

## 🥳 Contributing

Pull requests, bug reports, and witty comments are welcome. If you spot a bug, fix it, or at least laugh at it in the issues section.

---

## 👀 Credits

Crafted by Frank Contrepois, with a little help from AI, caffeine, and the occasional existential crisis.
