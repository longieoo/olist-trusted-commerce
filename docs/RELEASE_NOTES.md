# November 13, 2026 acceptance

The fixture-first platform passed both local Docker acceptance and [GitHub Actions run 43336061696](https://github.com/longieoo/olist-trusted-commerce/actions/runs/43336061696). The tested implementation is commit `fae190f11779d256e8923a1bd61401f0ee3f4827`; later overview/screenshot and release-note commits change documentation only. Fresh local receipts match the current implementation hashes.

| Check | Result |
|---|---|
| Unit and dashboard regression tests | 87 passed |
| Storage, queue and publication recovery integration | 4 passed |
| dbt build | 17 models and 2 data tests passed |
| dbt model parity | Zero differences across all 17 models |
| Fixture publication | 5 accepted orders, 121.60 BRL payments, per-order Decimal checks passed |
| Synthetic PostgreSQL CDC | 5 orders, 4 items and 3 payments match source |
| Consumer downtime drill | 2 new complete transactions, zero pending |
| CDC outcome | Payment 66.60 to 46.25 BRL; late flag 1 to 0 |
| Durable inbox replay | Reuses applied batches without duplicate application |
| Source/release checks | Syntax, documentation links, 9 fixture checksums and privacy checks passed |
| ZIP | Relative SHA-256 manifest, CRC/content verification and deterministic repeat build passed |

## Changes

- Replaced the unavailable MinIO community image dependency with a build from an exact upstream source revision.
- Added a portable runner: one command starts the fixture pipeline and dashboard, another runs the full acceptance sequence.
- Corrected the packaged multiline review fixture bytes and added source checksum checks to CI.
- Added an explicit synthetic fixture dashboard source, correct first-run selection and corrupt-publication rejection.
- Rejected unsupported fractional-second CDC timestamps and non-integer contract versions; fixed empty snapshots.
- Preserved baseline provenance and distinguished historical corrections, fixture corrections and PostgreSQL CDC in publication receipts.
- Replaced recursive packaging with tracked-public-source selection, private/runtime exclusions, content scanning and deterministic archives.
- Separated archived October evidence from fresh generated results. Failed verification invalidates a previous green report, including environment initialization failures. Source CI rejects forbidden tracked private paths.

## Reproduce

```sh
python scripts/lab.py demo
python scripts/lab.py verify
python scripts/package_final.py
python scripts/lab.py down
```

Docker's Linux engine and Python 3.10–3.12 are required. The first build needs internet access and compiles MinIO. The lab keeps volumes when stopped. Read the [runbook](FINAL_RUNBOOK.md) and [operating boundaries](OPERATIONS.md) before recovery exercises.

## Scope

The November acceptance uses small, explicitly synthetic inputs and real PostgreSQL WAL capture. Full historical Olist processing remains supported, with dated October evidence in `evidence/archive/2026-10/`; it was not rerun on all 1.55 million historical source records for this acceptance. No cloud deployment, production HA, capacity benchmark or distributed exactly-once guarantee is claimed. Aggregate marts and reference checks rebuild fully; publication copies the database. Cloud accounts, spend limits and any public hosting remain owner-controlled.
