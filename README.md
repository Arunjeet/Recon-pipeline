# Automated Accounting Intelligence Platform

A programmatic ETL and reconciliation system built on top of the Xero API and client-provided financial data. Designed to eliminate manual reconciliation, enforce data integrity across all client ledgers, and lay the foundation for automated risk assessment.

---

## What This Does

### Step 1 — Xero ETL Pipeline
Pulls live financial data from Xero, validates and transforms it, and loads it into a structured local database with a single CLI command.

- **OAuth2 authentication** with automatic token refresh — runs headlessly without re-authentication
- **5 extractors**: Journals (paginated), Manual Journals, Accounts, Trial Balance, P&L (with period/timeframe params)
- **Xero date parsing** — converts `/Date(ms)/` format to ISO across all extractors
- **Pandas transformation layer** — column normalisation, debit/credit splits, tracking category expansion
- **SHA-256 row hashing** — every row gets a deterministic unique ID derived from its content, enabling version control and safe re-runs without duplicates
- **Staging → production upsert pattern** — `NOT EXISTS` insert for new rows, `UPDATE` for changed fields, staging cleared after each run. ACID compliant
- **SQLite database** with typed schemas and `CHECK` constraints — Postgres-ready (connection config already in place)
- **Silver layer SQL** — report-level transforms built on top of the production tables (`shielded_expense`, `pnl_comp`)
- **Google Sheets push** — OAuth service account credentials, auto-resize, tab creation, frozen headers
- **One-command trigger** via `main.py` with subcommands (`tb`, `pnl`, `journal`, `manualjournal`, `account`)
- **CI/CD** via Docker and GitHub Actions with secrets management for sensitive credentials

### Step 2 — Client Reconciliation Engine
Ingests raw client bank and transaction files, validates every row through Pydantic, and cross-references against the Xero data to surface unreconciled items.

- **Dynamic Excel parser** — handles any file layout, variable header positions, marker-based data extraction, multi-sheet concatenation. No per-client configuration required
- **Column alias canonicalization** — maps any known variant of `date`, `description`, `amount`, `counterpart_coding`, `talos_name` to a standard schema in one place
- **Pydantic validation models** (`BankRawModel`, `ClientRawModel`) — row-level type enforcement, date coercion, comma-tolerant float parsing, boolean normalisation, `AliasChoices` for flexible field mapping
- **Same staging/upsert pattern** as Step 1 — consistent, predictable, idempotent writes
- **`bankunrec` table** — persistent ledger of unreconciled bank transactions. Auto-populates on new unreconciled items, auto-clears when the bank system marks them reconciled on a subsequent run
- **Entity extraction** from free-text `talos_name` — detects BVI / DE / LP markers, strips suffixes, produces clean client names
- **Internal encoding** — maps entity codes to account encoding strings (`bvi-1113`, `usa-1112`, `1115/1116`)
- **SQL transformation layer** — `clienttransform.py` enriches raw records into `clientprocessed` with coding, entity, keyword, and encoding columns

---

## Architecture

```
XERO SIDE
Xero API → OAuth2 + Refresh → Extract (Python) → Transform (Pandas + Hash) → SQLite Staging → Production DB → Silver Layer SQL → GSheets

CLIENT SIDE
Client Excel → Dynamic Parser → Pydantic Validation → Staging Table → Production DB → Transform SQL → Silver Layer

                        ↕ CROSS-SYSTEM RECONCILIATION ↕

OUTPUT & CI
GSheets Dump | main.py 1-cmd run | Docker + GitHub Actions | Secret Keys | Email Alerts | bankunrec log
```

---

## Project Structure

```
├── main.py                    # CLI entrypoint — tb / pnl / journal / manualjournal / account
├── bootstrap.py               # Schema creation and staging table teardown on startup
├── db_config.py               # SQLAlchemy engine config (SQLite now, Postgres-ready)
├── schemas.py                 # DDL for all production and staging tables
│
├── xerobootstrap.py           # OAuth2 consent flow + Flask /callback server
├── xerosummary_journals.py    # Paginated journal fetcher
├── xerosummary_manualjournals.py
├── xerosummary_accounts.py
│
├── process_journals.py        # JSON → flat rows (journals)
├── process_manualjournals.py  # JSON → flat rows (manual journals)
├── process_accounts.py        # JSON → flat rows (accounts)
│
├── transform.py               # Column normalisation, hashing, tenant_id injection
├── insertions.py              # Staging → production upsert logic
│
├── pnl_comp.py                # P&L comparison transform (pandasql)
├── shielded_expense.py        # Expense report — temp table → insert/update → GSheets
├── gsheet.py                  # Google Sheets push (TB, P&L, Account Transactions)
│
├── extraction.py              # bankfunc + clientfunc dynamic Excel parsers
├── models.py                  # Pydantic models — BankRawModel, ClientRawModel
├── transformations.py         # Bank + client staging/upsert pipeline
├── clienttransform.py         # Entity/coding/keyword enrichment into clientprocessed
├── script2.py                 # bankunrec population and clearance
│
├── trigger.sh                 # Shell orchestrator — full pipeline in one command
├── dockerfile                 # Container definition
└── xero_tokens.json           # OAuth token store (gitignored)
```

---

## Setup

### Prerequisites
- Python 3.11+
- Docker (for CI runs)
- Xero developer app with `offline_access`, `accounting.*` scopes
- Google service account JSON for Sheets access

### Environment Variables
```bash
XERO_CLIENT_ID=
XERO_CLIENT_SECRET=
XERO_REDIRECT_URI=http://localhost:8080/callback
tenant_id=                     # optional — auto-fetched from first connected org
```

### First Run (Auth)
```bash
python xerobootstrap.py        # Opens browser for Xero consent, saves tokens
```

### Run the Pipeline
```bash
python main.py journal
python main.py manualjournal
python main.py account
python main.py tb 2025-12-31
python main.py pnl 2025-01-01 2025-12-31 --periods 6 --timeframe MONTH
```

### Full CI Trigger
```bash
bash trigger.sh
```

---

## Known Issues (Fix Before Production)

- `bootstrap.py` drops `bankraw` on every run — production tables must not be dropped on restart. Only staging tables should be torn down
- `bankfunc` calls `drop_duplicates(subset=["ref_num"])` without checking if the column exists — will raise `KeyError` on files without that column
- `validate_bank_df` / `validate_client_df` iterate rows without `try/except` — a single `ValidationError` aborts the entire batch. Row-level quarantine logging needed
- `random_pad.py` is incomplete — `defaultdict` not imported, assignment operators use `==` instead of `=`. Either finish or remove before handover
- `xero_tokens.json` must be gitignored and moved to a secrets manager before multi-client deployment

---

## Next Steps

### Near-Term
- [ ] Migrate to **Postgres** (ElephantDB free tier) for concurrent multi-user access across the firm
- [ ] Add **Alembic** for schema migrations — schema changes without data loss
- [ ] Implement **structured run audit table** — log rows inserted, rows failed, duration, errors per run
- [ ] Add **Xero API rate limit handling** — exponential backoff on 429 responses
- [ ] Fix Pydantic row-level error isolation — quarantine bad rows, continue batch
- [ ] Add **page limiter and parallel async requests** to journal extractor for speed at scale

### Medium-Term
- [ ] **Master client registry table** — all pipeline runs parameterised by `client_id`, no hardcoded tenant IDs or account filters. Adding a client is a DB insert, not a code change
- [ ] **S3 as raw data sink** — store all incoming files, build partitioned bronze/silver layers using SQL or Spark
- [ ] **Webhooks** for out-of-CI triggers — client uploads a file, pipeline runs immediately
- [ ] **Version control per client** with rollback — append-only inserts with sync timestamps, queryable historic state for any prior date
- [ ] **Daily automated reminders** to clients for data submission
- [ ] Move token storage to a **secrets manager** (AWS Secrets Manager or HashiCorp Vault)

### Long-Term
- [ ] **Agentic risk assessment layer** — LLM reviews `bankunrec`, journal matching output, and `clientprocessed` to produce structured risk flags before period close. All claims grounded in DB rows, output validated by Pydantic
- [ ] **Automated audit pack generation** — pre-period-close report with matched reconciliations, outstanding exceptions, and risk assessment. Auditors review exceptions, not full ledgers
- [ ] **Real-time client notifications** — event-driven alerts (reconciliation complete, exception outstanding, anomaly detected) via email or Slack
- [ ] **Self-serve dashboards** — firm-level overview and per-client view
- [ ] **Triple reconciliation** — add blockchain record as a third immutable data source alongside Xero and client statements. System self-validates across all three, flags discrepancies without human trigger

---

## Design Decisions

**Why SHA-256 hashing instead of auto-increment IDs?**
Auto-increment IDs are connection-local and meaningless across re-runs. A hash derived from the row's content is deterministic — the same data always produces the same key, making idempotent upserts trivial and enabling future version diffing.

**Why a staging table pattern instead of direct upsert?**
Direct upsert (`ON CONFLICT DO UPDATE`) works but gives no visibility into what changed. The staging pattern makes the insert/update split explicit and auditable, and is easier to extend with logging or validation between stages.

**Why SQLite now?**
Zero infrastructure for development and single-user CI runs. The Postgres migration path is already written — it's a config switch. SQLite will not survive concurrent multi-client writes, which is the trigger for migrating.

**Why Pydantic for validation?**
Schema enforcement at the application layer catches type errors before they reach the DB. `AliasChoices` means the validation model is also the column alias map — one place to add a new field name variant.
