# Operating boundaries

The acceptance target is a reproducible local data platform: a clean clone can publish five synthetic orders, apply complete PostgreSQL transactions, reconcile money and display a verified publication. It does not depend on a paid account.

## Start, verify, stop

Use `python scripts/lab.py demo`, then `python scripts/lab.py verify`. Inspect `evidence/current/ACCEPTANCE.json` for the current result and the Actions page for hosted results. Use `python scripts/lab.py status` to inspect services and `python scripts/lab.py down` to stop only this lab, preserving volumes. `--project <name>` isolates Compose resources; the checkout's data directories remain shared, so do not run multiple writers against the same checkout.

Ports are bound to localhost. Keep Docker's Linux engine running and allow about 8 GiB to Docker. Do not publish the lab directly to the internet. Setup creates `.env` once and preserves existing credentials on later runs. Changing `.env` after PostgreSQL initialization does not rotate the database password; provision a controlled migration instead.

## What the checks prove

| Boundary | Expected behavior | Verification |
|---|---|---|
| CSV bytes changed | Manifest verification rejects changed input | Fixture checksum checks |
| Invalid rows / child references | Explicit rejection and accounting | Batch and integration checks |
| One order, multiple items and payments | Aggregate children before joins | Independent per-order Decimal checks |
| Raw/curated crash before commit | Orphan attempts never become published data | Four integration failure points |
| Commit succeeded, acknowledgment lost | Retry does not apply the batch again | Incremental unit tests |
| Deleted key, stale update | Tombstone prevents resurrection | Incremental unit tests |
| CDC END missing rows or BEGIN | Wait for a complete transaction | CDC regression tests |
| Fractional-second source timestamps | Reject unsupported precision | CDC normalization tests |
| Publication file corruption | Reader fails closed | Dashboard AppTest |
| Verification rerun fails | Old green status is replaced | Portable runner regression test |

## Recovery and retention

Keep the source CSVs, raw commits, durable CDC inbox, working database and publication pointer together for recovery. Do not delete a Kafka offset, inbox or tombstone to force a green report. The consumer acknowledges Kafka only after durable inbox capture; analytical publication is a separate boundary. Resume capture with the same namespace and source binding. A baseline change requires an explicit new state directory and a controlled source/slot migration.

Only one writer is supported. A leftover `.writer.lock` requires checking that the writer really stopped before removing that specific lock. There is no automatic stale-lock takeover. Transaction journals are bounded to 50,000 records and PostgreSQL slot WAL retention to 512 MB; beyond these limits, diagnose and perform a controlled resnapshot. Retention and backup rotation are manual lab responsibilities; no automatic deletion is configured.

The acceptance run measures correctness, not a latency SLO. Source-to-dashboard freshness depends on the bounded CDC consumer invocation. Larger data sets increase full reference checks, aggregate rebuilds and publication copies. No capacity or events-per-second claims are made from the small fixture.

## Data contracts and interpretation

Mutable CDC tables are orders, order items and order payments. Source positions determine order; ingestion clocks do not. Contract version must be an integer. Timestamps currently have whole-second precision. Currency uses fixed-point decimals. Dimensions are immutable in the correction engine.

Historical, fixture, correction and synthetic PostgreSQL sources keep distinct labels and baseline provenance. Cohorts describe observed order creation, including canceled and unavailable orders, not completed-purchase retention. Merchandise value, freight and payment are separate measures, not platform revenue or profit. Geography uses median ZIP observations and reports coverage; distance is straight-line. Delivery and review associations are descriptive, not causal.

## External dependencies

MinIO's community images were unavailable to the hosted runner during the November audit. The lab now compiles a pinned upstream revision; see `infra/Dockerfile.minio` and the upstream license. First build needs the Go module proxy and package registries. A registry outage should fail the run visibly, not trigger a silent replacement with another vendor's image.

Snowflake, hosted dashboards and Azure are optional extensions. They are not prerequisites or evidence of a completed deployment. Cloud credentials, subscription consent and a spending limit remain owner-controlled.
