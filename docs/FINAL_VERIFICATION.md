# Nghiệm thu Olist Trusted Commerce

## Current acceptance

The November upgrade uses `python scripts/lab.py verify` as the fixture-first acceptance entry point. Its current result is written to `evidence/current/ACCEPTANCE.json` after batch, tests, dbt parity and synthetic WAL CDC pass on unchanged code. A failed run records `failed`. GitHub Actions runs the same command and uploads fresh reports; check its actual conclusion rather than inferring success from older files.

The October results below are historical records of the earlier full Olist checkout. They do not certify the November code. Regenerate the corresponding reports with the runbook when validating historical data again. Packaging has separate syntax, source-fixture, privacy and archive checks.

## Historical October validation

Cập nhật 23/10/2026. Cổng nghiệm thu cuối là [FINAL_VERIFICATION.json](../evidence/archive/2026-10/FINAL_VERIFICATION.json), tạo bằng scripts/verify_upgrade.py, đã PASS lúc 03:00:43 UTC ngày 23/10/2026. Các báo cáo chạy hệ thống ngày 22/10 được cổng nghiệm thu xác nhận khớp hash mã nguồn hiện hành và đúng generation; không coi đây là chạy lại toàn bộ hệ thống ngày 23/10. Chỉ trạng thái passed với hash code hiện hành và đúng generation mới xác nhận bản bàn giao; không dùng pass count cũ để thay thế.

## Các phép kiểm tra

| Phần | Kết quả được kiểm | Evidence |
|---|---|---|
| Unit | 59 tests; contracts, money, grain, incremental, replay, transaction completeness và publication | [tests.xml](../evidence/archive/2026-10/tests.xml), [receipt](../evidence/archive/2026-10/tests.receipt.json) |
| Docker integration | 4 tests; Spark/MinIO, immutable publication/failure boundaries và queue | [integration XML](../evidence/archive/2026-10/integration-tests.xml), [receipt](../evidence/archive/2026-10/integration-tests.receipt.json) |
| dbt local | 17 models + 2 data tests; mọi model khớp canonical warehouse | [dbt verification](../evidence/archive/2026-10/upgrade-dbt.json) |
| Independent CSV oracle | 98.059 orders, 111.049 item rows, 102.453 payment rows; 0 per-order mismatch | [source oracle](../evidence/archive/2026-10/final-source-oracle.json) |
| Corrections trên historical copy | Payment +7.25 BRL, item delete, retry/stale decisions, publication failure giữ pointer trước | [correction report](../evidence/archive/2026-10/upgrade-corrections.json) |
| PostgreSQL WAL CDC | 12 snapshot records; 2 transactions mới trong mỗi demo; source parity cả 3 bảng, no pending | [CDC report](../evidence/archive/2026-10/upgrade-cdc.json) |
| CDC financial fixture | o1 payment 66.60 → 46.25 BRL, late 1 → 0; replay inbox không nhân đôi | [CDC report](../evidence/archive/2026-10/upgrade-cdc.json) |
| Connector role | REPLICATION + SELECT; không superuser hoặc table UPDATE | [CDC report](../evidence/archive/2026-10/upgrade-cdc.json) |
| Dashboard | Historical 98.059, BA 3.334, January 2018 7.244; chuyển nguồn simulation/CDC, missing-order trace | [dashboard report](../evidence/archive/2026-10/final-dashboard.json) |
| Offline integrity | Syntax, links, CSV/database checksums, PK/FK/money | [offline report](../evidence/archive/2026-10/CLEANUP_VERIFICATION.json) |

Code hash bao gồm source, SQL, tests, app, DAG, scripts, infra, CI và dependency/config files. Test receipt ghi hash trước/sau; code đổi trong lúc test thì lệnh thất bại dù pytest test cases đều pass. Không sửa receipt bằng tay.

## Những lỗi đã được kiểm tra có chủ đích

- Crash trước commit: rollback canonical state, ledger và facts.
- Commit thành công nhưng mất acknowledgment: retry không áp dụng lại.
- Lỗi trước đổi publication pointer: reader vẫn đọc generation trước.
- Delete rồi update cũ đến muộn: tombstone chặn resurrection.
- Event ID hoặc source position xung đột nội dung: fail closed.
- Parent delete còn references, schema drift hoặc nguồn sai namespace: bị chặn.
- END thiếu BEGIN hoặc thiếu rows: waiting_for_transactions, không báo catch-up.
- Multi-item/multi-payment không gây fan-out; lỗi +1/-1 ở hai order không bị aggregate reconciliation che mất.
- Cohort chưa đủ 90 ngày giữ denominator 0 và tỷ lệ null; ZIP observations không nhân số order.

## Benchmark và phạm vi

[Warm query benchmark](../evidence/archive/2026-10/final-local-benchmark.json) dùng 7 lượt, 2 DuckDB threads và kết quả tương đương. Lượt đo mới có median fact scan khoảng 5.24 ms, daily mart khoảng 17.77 ms. Mart không nhanh hơn trong phép đo này; không tuyên bố tối ưu tăng tốc. Đây là query execution+fetch, không phải whole-pipeline throughput hoặc cold OS-cache benchmark.

Correction invocation có thể dùng lại batch receipts đã kiểm chứng. invocation_seconds đo lần gọi hiện tại; seconds trong từng batch receipt là thời gian kiểm chứng khi batch đó được tạo. Không dùng số cached replay để quảng cáo latency xử lý một change mới. Máy còn có workload khác, timings phụ thuộc tài nguyên.

## Giới hạn còn lại

- CDC dùng WAL thật nhưng dữ liệu nguồn là simulator, không phải feed live của Olist.
- Impacted facts incremental; aggregate marts/reference validation vẫn full rebuild; publication copy toàn file.
- Dimensions/review cố định trong namespace; parent delete có references bị từ chối; journal giới hạn 50.000 records, chưa có archival tự động.
- dbt local là cổng build/parity, chưa thay transaction engine của CDC.
- Single host, single broker, single writer; không claim production HA hoặc end-to-end exactly-once.
- Chưa deploy Azure/Snowflake, chưa có hosted GitHub Actions run. Airflow và lifecycle replay giữ evidence có ngày riêng; lượt nghiệm thu này không chứng minh scheduler/stream chạy ổn định nhiều ngày.
- Phép đối soát CSV độc lập kiểm accepted values/money, không độc lập xác nhận toàn bộ lựa chọn quarantine.

Docker đã được khởi động và dùng để chạy kiểm tra mới; ghi chú lỗi Docker của lần dọn 19/10 không còn là trạng thái runtime hiện tại. Thư mục dự án duy nhất vẫn là olist-final; nguồn synced dưới sources không bị sửa.


Public release note (2026-11-11): reports in evidence/ record the October local checkout. Runtime databases and source CSVs are intentionally omitted; do not use an archived passed flag as proof of a fresh clone run. This release reran 59 unit tests successfully; 4 service integration tests were not rerun during packaging. Regenerate runtime receipts with the runbook before using verify_upgrade.py.
