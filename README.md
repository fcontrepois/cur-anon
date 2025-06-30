# curanonymiser

A fast, no-fuss tool for anonymising AWS Cost and Usage Report (CUR) Parquet files. Strip out or mask sensitive details, keep your data useful, and share it with peace of mind.

---

## 🚀 Features

- Reads and writes Parquet using DuckDB—no Spark or Java hassle
- Anonymises AWS Account IDs and ARNs, keeping things consistent
- Lets you drop columns you don’t need
- Simple JSON config—easy to tweak, easy to share
- Auto-generates a config file for you

---

## 🏁 Quick Start

### 1. Install what you need

```sh
pip install duckdb pandas
```

### 2. Generate a config file

```sh
python curanonymiser.py --input rawcur.parquet --create-config --config config.json
```

This gives you a config listing all columns. Edit it to pick what to keep, remove, or anonymise.

### 3. Edit your config

Set each column to one of:

- `keep` – keep as is
- `remove` – drop the column
- `awsid_anonymise` – swap for a fake, consistent 12-digit AWS account ID
- `awsarn_anonymise` – swap for a fake ARN using the fake account ID

### 4. Run the anonymiser

```sh
python curanonymiser.py --input rawcur.parquet --output anonymisedcur.parquet --config config.json
```

Done! Your anonymised file is ready.

---

## 📝 Example Config

```json
{
  "_comment": "Column options: 'keep' (keep as is), 'remove' (remove column), 'awsid_anonymise' (anonymise as AWS account ID), 'awsarn_anonymise' (anonymise as AWS ARN using fake account ID)",
  "columns": {
    "lineItem/UsageAccountId": "awsid_anonymise",
    "bill/PayerAccountId": "awsid_anonymise",
    "lineItem/ResourceId": "awsarn_anonymise",
    "product/instanceType": "remove",
    "product/region": "keep",
    "lineItem/UsageType": "keep"
  }
}
```

---

## 🛠 Handy Commands

**See your Parquet columns:**

```sh
duckdb -c "SELECT * FROM 'yourfile.parquet' LIMIT 0;"
```

**Grab the first 100 rows as CSV:**

```sh
duckdb -c "COPY (SELECT * FROM 'yourfile.parquet' LIMIT 100) TO STDOUT (HEADER, DELIMITER ',');"
```

---

## ❓ Need Help?

Just run:

```sh
python curanonymiser.py --help
```

---

## 🤔 Why bother?

CUR data is gold for cost analysis, but it’s packed with sensitive stuff. curanonymiser lets you keep the insights, lose the risk. No cloud uploads, no headaches: just Python, DuckDB, and you.

---

## 📄 Licence

MIT

---

Pull requests, bug reports, and bright ideas are always welcome!
