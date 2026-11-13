# Assignment A: Interaction Ledger & Reporting Pipeline

## Requirements and installation

Use Python 3.11 or newer. The solution was exercised with Python 3.12.6. The shared
`requirements.txt` includes `cryptography` for Fernet encryption and `pytest` for tests; the
remaining dependencies are retained for the repository's Assignment B mock provider.

From the repository root:

```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# macOS/Linux (use this instead of the PowerShell command):
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## Configuration

The CLI reads these environment variables:

- `AU_FERNET_KEY` (required): a URL-safe base64 Fernet key. Keep it stable for the lifetime
  of a database; losing it makes encrypted values unreadable.
- `AU_HMAC_SECRET` (required): at least 32 UTF-8 bytes, used for workflow-scoped phone
  identity digests.
- `AU_DATABASE_PATH` (optional): SQLite database path. The default is
  `solution/assignment_a.sqlite3`.

Generate fresh secrets with Python's cryptographic random generator, then keep the values in
your local process environment or an approved secrets manager. These examples capture the
generated output into environment variables rather than printing the values:

```powershell
$env:AU_FERNET_KEY = (python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())").Trim()
$env:AU_HMAC_SECRET = (python -c "import secrets; print(secrets.token_hex(32))").Trim()
$env:AU_DATABASE_PATH = "solution/assignment_a.sqlite3"
```

For macOS/Linux:

```bash
export AU_FERNET_KEY="$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
export AU_HMAC_SECRET="$(python -c 'import secrets; print(secrets.token_hex(32))')"
export AU_DATABASE_PATH="solution/assignment_a.sqlite3"
```

Do not put secret values in source code, command-line arguments, reports, or SQLite. The
repository ignores `.env` files, but this application does not automatically load them; export
the variables into the environment before running the CLI. Use a secret manager for durable or
shared environments. Do not enable shell tracing while generating or exporting secrets.

## Run

The supplied generator creates two small synthetic drops (optional if you already have data):

```bash
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
```

Initialize the database, ingest drops in delivery order, and generate all five reports:

```bash
python -m solution.au_pipeline init-db
python -m solution.au_pipeline ingest data/drop_1 data/drop_2
python -m solution.au_pipeline generate-reports --output-dir reports
```

The reports are `batch_summary.csv`, `daily_overview.csv`, `lead_report.csv`, `rejections.csv`,
and `data_quality.json`. The output directory is created if needed. Re-ingesting a completed
drop with identical contents is a no-op; reusing its drop ID with changed contents fails.

Run the full test suite from the repository root:

```bash
python -m pytest solution/tests -q
```

Generated `data/`, reports, local databases, virtual environments, and local environment files
are excluded by the root `.gitignore`.
