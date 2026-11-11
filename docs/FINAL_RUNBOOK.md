# Runbook — Olist Trusted Commerce

Cập nhật ngày 22/10/2026. Chạy PowerShell tại root olist-final. Docker Desktop Linux containers; internet cho lần build đầu; khoảng 8 GiB RAM Docker. Dữ liệu thật không nằm trong Git: cần 9 CSV và manifest tại data/real.

## Chuẩn bị một lần

```powershell
./scripts/final.ps1 setup
./scripts/final.ps1 demo-fixture
./scripts/final.ps1 demo-real
```

Fixture ở data/local-fixture, historical ở data/local. Fixture cố ý có lỗi, dùng threshold 1; historical giữ threshold 0.02 theo bảng. Demo-real tạo historical publication và mở dashboard localhost:8510.

## Chạy toàn bộ nâng cấp và nghiệm thu

```powershell
./scripts/upgrade.ps1 corrections
./scripts/upgrade.ps1 cdc
./scripts/upgrade.ps1 dbt
./scripts/final.ps1 integration
./scripts/upgrade.ps1 verify
./scripts/upgrade.ps1 ui
```

- Corrections: bản sao historical, payment +7.25 BRL, delete item, duplicate/stale retry, publication failure và full-rebuild parity.
- CDC: khởi động Kafka/Connect, provision source và replication reader role, chạy snapshot/WAL demo. Mỗi lần chạy reset riêng order o1 trong schema synthetic rồi thực hiện 2 transactions khi consumer đã đóng; không sửa historical CSV.
- dbt: build image nhỏ kế thừa runtime, dbt build trên database copy và đối chiếu 17 models. Không cần Python/dbt cài trên Windows.
- Integration: fresh fault-injection buckets. Verify: tests, independent CSV oracle, dashboard tests, offline integrity và cổng evidence cuối.

Lần đầu chạy sẽ lâu hơn do tải image. Demo correction trên 98 nghìn đơn vẫn chạy full reference validation và copy database, không phải phép đo millisecond latency. Xem timings trong evidence.

## Các lệnh chi tiết

```powershell
$dc = @('compose','-p','olist-final','-f','compose.yaml','-f','infra/compose.clean.yaml','-f','infra/compose.cdc.yaml')
& docker @dc run --rm --no-deps job changes-init --source-root data/local --root data/simulation --namespace portfolio-demo
& docker @dc run --rm --no-deps job changes-apply --root data/simulation --file data/runtime/my-changes.json
& docker @dc run --rm --no-deps job changes-publish --root data/simulation
& docker @dc run --rm --no-deps job cdc-consume --seconds 20
& docker @dc run --rm --no-deps --entrypoint python3 job scripts/benchmark_local.py
```

Changes file là array full-image events; xem tests/test_incremental.py. Position phải theo source ordering, không dùng wall-clock ingestion. Parent delete còn FK references bị từ chối. Schema drift bị quarantine/fail closed.

CDC capture chạy hữu hạn theo seconds cho demo; invocation tiếp theo tiếp tục từ inbox/Kafka offsets. Không mô tả đây là managed always-on production service. Trạng thái waiting_for_transactions nghĩa là chưa đủ dữ liệu, chưa catch-up; không bỏ qua để báo pass.

## Phục hồi

- Lỗi trước commit: transaction rollback; raw batch còn để điều tra. Sửa nguyên nhân rồi retry cùng payload.
- Lỗi sau commit trước acknowledgment: batch receipt và event ledger chống apply lại.
- Lỗi trước publication: current.json giữ generation trước. Chạy changes-publish lại.
- Writer lock còn: kiểm tra không còn writer/process trước khi xóa lock đúng thư mục; không tự takeover.
- Quarantine tại data/simulation/quarantine hoặc data/cdc/quarantine; giữ raw khi điều tra.
- Không xóa inbox.sqlite, working.duckdb, replication slot hoặc Kafka volumes để “sửa” lỗi. Thay baseline cần namespace/state mới được kiểm soát; WAL retention vượt 512 MB có thể cần resnapshot.
- Sau sửa source/infra/dependency: chạy lại các demo và tests trước verify. Không cập nhật hash bằng tay để hợp thức hóa evidence cũ.

## Dừng

```powershell
./scripts/upgrade.ps1 down
```

Giữ named volumes, không xóa dữ liệu. Chỉ tác động project olist-final, không dừng containers thuộc project khác.

## Bảo mật và cloud

.env và data/cdc/connector-secret.json không được commit/package. Role connector không có superuser hoặc UPDATE bảng; Kafka/MinIO/PG local không mở host port trong clean override. Dashboard localhost8510 là read-only. Không public Connect API.

Azure/Snowflake là optional slice chưa triển khai. Cần subscription/tài khoản, budget và destination của bạn. Đọc [demo portfolio](PORTFOLIO_DEMO.md) để quay video và chuẩn bị câu hỏi phỏng vấn.
