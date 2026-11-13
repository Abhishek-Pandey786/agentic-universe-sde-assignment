# Submission

## Candidate

- **Name:** Abhishek Pandey
- **Assignment chosen:** A (Data)
- **Time spent (honest estimate):** Approximately 3 hours 40 minutes.

Complete the name and personal time estimate before submitting.

## How to run

From a clean clone, use Python 3.11 or newer, create/activate a virtual environment, and run
`python -m pip install -r requirements.txt`. Set `AU_FERNET_KEY` and `AU_HMAC_SECRET` in the
environment as described in `solution/README.md`; `AU_DATABASE_PATH` is optional and defaults to
`solution/assignment_a.sqlite3`.

```bash
python -m solution.au_pipeline init-db
python -m solution.au_pipeline ingest data/drop_1 data/drop_2
python -m solution.au_pipeline generate-reports --output-dir reports
python -m pytest solution/tests -q
```

## Design

The application is a small Python package using `sqlite3`, `cryptography`/Fernet, and the
standard library. `schema.py` loads the supplied workflow and disposition catalogues;
`normalize.py` implements strict, pure value and timestamp parsing. The `leads.py` and
`events.py` stages accept the orchestrator's connection and never own drop transactions.
`pipeline.py` owns the whole drop lifecycle, and `reports.py` reads a consistent database
snapshot and writes the five contract files. `__main__.py` is an `argparse` adapter over those
APIs.

SQLite stores drops and per-drop quality, leads, append-only events, event-to-lead links,
mutable disposition classification, and PII-free lead rejections. Leads are unique by
`(workflow_id, phone_hmac)`. Phone and customer name are Fernet-encrypted; phone digests use
HMAC-SHA256 with an external secret. Event payloads are encrypted before persistence, with
separate encrypted phone and disposition fields for controlled linking and reporting. Event
rows have update/delete triggers; links and disposition state remain separately mutable for
late-arriving leads and overrides.

Each drop is content-hashed and processed under one `BEGIN IMMEDIATE` transaction. The
orchestrator claims it as `processing`, creates quality state, runs lead ingestion before event
ingestion, relinks retained events across drops, derives final quality, rechecks the input hash,
and changes the status to `complete` before commit. Exceptions roll back the claim and all
drop writes. A completed ID/hash pair is a no-op; a changed hash for an existing ID fails.

Reports are assembled from linked supported events and valid timestamps. Daily dates are IST.
Effective disposition resolution gives any override precedence over sets, selects the latest
event by timestamp and then lexicographically greatest event ID, and only then checks the
catalogue. An unknown winning disposition has no effective value and does not fall back.
Quality counters are summed across drop records; final orphan and unknown counts come from
current link/classification state. The output quality object has exactly the ten contract
fields.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| A drop ID is its directory name; accepted drop structure is `leads/` and `events/`. | Matches the supplied generator and makes identity visible in CLI use. | Pass a separate ID or accept incomplete directory layouts. |
| Only `complete` is the successful persisted status; `processing` is inside the owning transaction. | Uses the existing schema constraint and leaves no committed in-progress claim on failure. | Add a new status or a separate lifecycle table. |
| Equal disposition timestamps use the lexicographically greatest event ID. | Provides a stable tie-break independent of file enumeration. | Use source file/line order or SQLite row insertion order. |
| Select the winning disposition event before checking its code. | An unknown winning override must not expose an older set as effective. | Filter unknown codes first and fall back to an older known event. |
| Whitespace-only JSONL lines are blank and do not count toward event totals. | They carry no event record and are excluded consistently with blank CSV rows. | Count them as malformed. |
| An initiated event without `call_id` is treated as a distinct call keyed by its event ID. | It is a valid linked initiation, but cannot be joined to other events without a call ID. | Discard it from call totals. |
| Quality orphan and unknown counts are derived from final database state. | Late lead arrival can change linking after an earlier drop. | Persist provisional counts per drop. |

## Failure modes considered

- Required lead failures become PII-free rejection records; optional invalid fields become null
  with warning counts. CSV/JSONL read errors are reported without echoing row contents.
- Duplicate leads merge by workflow and phone digest; event IDs are globally unique and duplicate
  events are skipped. Completed drop retries are idempotent.
- Drop hashing and transaction boundaries detect persistent input changes before completion and
  roll back partial work. SQLite writer locking serializes concurrent claims.
- Orphan events remain in the ledger and can be linked after later leads arrive. Unresolved
  overrides retain `call_id` and remain pending until a linked event identifies the call's lead.
- Losing the Fernet key makes encrypted PII and event payloads unreadable. Key rotation and
  recovery are operational responsibilities and are not implemented.
- No large-scale performance run was performed. Report output replacement is file-by-file, not
  an atomic five-file directory swap.

## Testing

The suite covers normalization, schemas, database constraints, PII helpers, lead and event
stages, outer transaction rollback, cross-drop relinking, idempotency, reports, and CLI behavior.
The latest recorded regression run was `python -m pytest solution/tests -q`: **122 passed**.

For end-to-end validation, the supplied small generator was run with seed 42. Both drops were
ingested through the CLI, all five reports were produced, a retry skipped both identical drops,
and a repeated report generation was byte-identical. The two drops contained 2,057 nonblank lead
rows and 8,745 nonempty event lines; the resulting database had 1,892 leads and 8,500 retained
events. Quality counters reconciled with rejections, duplicate/malformed events, links, and
orphans. A plaintext PII scan of reports, event payload storage, and CLI output found no matches
among the synthetic PII values checked. Original Assignment A input files were hash-checked and
unchanged. The large generator was not run; no performance timing was collected.

The environment did not have persistent `AU_FERNET_KEY` or `AU_HMAC_SECRET` values configured
during validation. Temporary random values were used only in process environments and were not
saved. Therefore the generated local validation databases are not reusable; use stable secrets
and a fresh database for subsequent runs.

## Results

Small seed 42 quality metrics: 2,057 lead rows total, 108 rejected, 57 duplicate lead rows
merged, 122 field warnings, 8,745 events total, 8 malformed events, 237 duplicate events
dropped, 462 final orphans, and 18 linked unknown dispositions. Rejections were: 42
`invalid_phone`, 25 `missing_required:customer_name`, 13
`missing_required:order_id`, 14 `missing_required:phone`, and 14
`missing_required:policy_number`.

The report row counts were 6 batch summaries, 33 workflow/date summaries, 1,892 leads, and 108
rejections. Runtime timings were not recorded.

## If I had another day

1. Run the evaluator's fresh-seed dataset and compare all five files to its expected output.
2. Add a report-output staging/swap strategy so the five files are replaced as one unit.
3. Measure large-scale performance before choosing any optimization or SQL aggregation work.
4. Add an explicit key-rotation and database-backup procedure.
