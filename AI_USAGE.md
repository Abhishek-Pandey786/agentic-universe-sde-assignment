# AI Usage Disclosure

## Tools used

OpenAI Codex (GPT-6) in the ChatGPT coding workspace.

## What I used them for

The assistant helped inspect the assignment materials, design the SQLite data model and drop
transaction flow, implement the Python package stages and CLI, write unit/integration tests, and
perform read-only reviews of ingestion, orchestration, and reporting. It also ran the requested
small synthetic end-to-end validation and regression suite. The assistant did not run the large
generator or install dependencies.

## What I wrote or decided myself

I selected Assignment A, supplied the implementation constraints and acceptance checks, reviewed
and approved the architecture before implementation, and completed the Python environment setup.
I directed the assistant to preserve the supplied assignment materials, use synthetic data, keep
secrets out of source and Git, and report actual rather than assumed test results.

## Where the AI was wrong or unhelpful

During report implementation, the assistant initially filtered unknown disposition codes before
choosing the winning event. That could incorrectly fall back to an older known disposition when a
later override was unknown. A read-only review identified the issue; the assistant changed the
resolution order and added regression tests. While writing the cross-drop rejection-order test,
the assistant first violated the schema's per-drop rejection uniqueness constraint; it corrected
the fixture to place shared sort keys in separate drops.

During end-to-end validation, the configured environment lacked both required secrets. The
assistant used random process-only test values, did not persist them, and documented that the
resulting local validation databases cannot be reused for later decryption.

## How I verified AI-generated code

- Ran `python -m pytest solution/tests -q`; the latest recorded result was **122 passed**.
- Ran the supplied small generator with seed 42 and exercised database initialization, ordered
  ingestion of both drops, and report generation through the CLI.
- Verified exact report names and CSV headers, the ten quality fields, row and counter
  reconciliation, orphan/unknown counts, identical-drop no-op behavior, and byte-identical
  repeated reports.
- Scanned synthetic PII values against report contents, CLI output, and persisted event payloads;
  checked that original Assignment A data files were unchanged.

I did not run the large generator or collect a large-scale performance result.
