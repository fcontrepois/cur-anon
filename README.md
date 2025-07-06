# cur-anon

A brisk, no-nonsense tool for anonymising AWS Cost and Usage Report (CUR) Parquet files. Remove or mask sensitive details, preserve data utility, and share your reports without breaking a sweat—or a compliance rule.

---

## 🚀 Features

- Reads and writes Parquet (and CSV) via DuckDB; no Spark, no Java, no drama
- Anonymises AWS Account IDs and ARNs, ensuring consistency across the dataset
- Lets you hash columns, drop columns, or keep them as you fancy
- Simple, human-editable JSON config, easy to share, easy to tweak
- Auto-generates a config file from your Parquet columns
- CLI with helpful flags and no unnecessary faff

---

## 🏁 Quick Start

### 1. Install the essentials

```sh
pip install duckdb pandas
```

### 2. Generate a config file

```sh
python curanonymiser.py --input rawcur.parquet --create-config --config config.json
```

This produces a config listing all columns and their suggested actions. Edit it to choose which columns to keep, remove, anonymise, or hash.

### 3. Edit your config

Each column can be set to one of:

- `keep` – leave the column untouched
- `remove` – drop the column entirely
- `awsid_anonymise` – swap for a fake, consistent 12-digit AWS account ID
- `awsarn_anonymise` – swap for a fake ARN, using the fake account ID
- `hash` – scramble the column with DuckDB’s `md5_number_upper`, so the same value always produces the same hash, but there is no way back—perfect for secrets, not for magicians.

### 4. Run the anonymiser

```sh
python curanonymiser.py --input rawcur.parquet --output anonymisedcur.parquet --config config.json
```

Or, if you prefer CSV:

```sh
python curanonymiser.py --input rawcur.parquet --output anonymisedcur.csv --config config.json
```

Voilà! Your anonymised file is ready for sharing, analysis, or waving triumphantly at your compliance officer.

---

## 📝 Example Config

```json
{
  "_comment": "Column options: 'keep', 'remove', 'awsid_anonymise', 'awsarn_anonymise', 'hash'",
  "columns": {
    "lineItem/UsageAccountId": "awsid_anonymise",
    "bill/PayerAccountId": "awsid_anonymise",
    "lineItem/ResourceId": "awsarn_anonymise",
    "product/instanceType": "remove",
    "product/region": "keep",
    "lineItem/UsageType": "keep",
    "resourceTags/user:CostCentre": "hash"
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
- Columns set to `hash` are hashed with DuckDB’s `md5_number_upper` function—irreversible, but consistent.
- Columns set to `remove` vanish without a trace. Columns set to `keep` are left alone, as nature intended.
- Output can be Parquet or CSV, depending on your mood.

---

## ❓ Flags & Usage

```sh
python curanonymiser.py --input INPUT.parquet --output OUTPUT.parquet --config config.json
```

- `--input`           Path to the input Parquet file (required)
- `--output`          Path to the output file (required, unless using `--create-config`)
- `--config`          Path to the JSON config file (required, unless using `--create-config`)
- `--create-config`   Generate a config file from the input Parquet file and exit
- `--help`            Show help and exit

---

## 📜 Licence

MIT. Because life’s too short for restrictive licences.

---

## 🥳 Contributing

Pull requests, bug reports, and witty comments are welcome. If you spot a bug, fix it, or at least laugh at it in the issues section.

---

## 👀 Credits

Crafted by Frank Contrepois, with a little help from AI, caffeine, and the occasional existential crisis.
