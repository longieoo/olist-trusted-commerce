# Verification evidence

`archive/2026-10/` preserves dated results from the earlier historical-data checkout. These do not certify later code revisions.

Fresh runs write `current/ACCEPTANCE.json`, `current/fixture.json` and runtime test/dbt/CDC reports. They are generated, not committed. GitHub Actions uploads the fresh reports as artifacts. A failed verification overwrites an earlier green status. Use the report timestamp, source hashes and publication identity together.
