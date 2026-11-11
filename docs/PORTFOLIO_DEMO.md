# Demo portfolio và cách trình bày năng lực

Thông điệp: một correction ở nguồn được phản ánh đúng vào analytics, retry không double-count, và mỗi kết quả có thể truy về nguồn/position/publication.

## Chuẩn bị

Chạy FINAL_RUNBOOK.md; mở localhost:8510. Không dành buổi phỏng vấn để tải image. Giữ evidence/FINAL_VERIFICATION.json và log dbt để người xem có thể kiểm chứng. Thời gian cold image pull/startup tách khỏi xử lý dữ liệu.

## Demo 8–10 phút sau khi chuẩn bị môi trường

1. Historical overview: chỉ ra 98.059 accepted orders, quality/quarantine và chênh lệch nguồn. Phân biệt merchandise, freight và payment.
2. Payment: chọn order nhiều items/payments, giải thích tại sao join trực tiếp gây fan-out; chỉ ra aggregate child trước khi join.
3. Geography/customer: ZIP nhiều observations, coverage; cohort chỉ dùng khách đủ thời gian quan sát; repeat order không phải repeat completed purchase.
4. Chạy upgrade.ps1 cdc: simulator reset order o1, snapshot/inbox catch-up, dừng consumer rồi commit hai transactions update/delete/insert. Kết quả mong đợi payment 66.60 → 46.25 BRL, late 1 → 0.
5. Chọn nguồn CDC, Order trace o1: xem before/after, source position, applied/stale, publication identity. Đọc upgrade-cdc.json: source parity, snapshot counts, complete transactions, no pending.
6. Chọn correction trên lịch sử: order ID trong upgrade-corrections.json; +7.25 BRL, xóa item, retry và stale event. Chỉ rõ đây là simulation, không phải sự kiện Olist live.
7. Trình bày tests fault injection và cổng FINAL_VERIFICATION: stale code/evidence bị từ chối. Mở dbt run_results và 17-model parity.

## Điểm nên ghi CV sau khi tự hiểu và chạy lại

- Built a reproducible Olist commerce data platform processing 1.55M historical source records with validation, quarantine and immutable analytical publications.
- Implemented PostgreSQL/Debezium CDC with a durable Kafka inbox, transaction completeness checks, idempotent updates/deletes, tombstones and before/after audit.
- Verified 17 analytical models through dbt-duckdb builds, full-rebuild parity and independent per-order Decimal reconciliation; delivered Streamlit payment, delivery, cohort and order-trace views.

Không ghi million events/sec, production-grade, exactly-once, cloud deployed hoặc CI hosted passed khi chưa đo/triển khai. Công khai dự án dưới tên bạn phải giữ attribution nguồn và hiểu được tradeoffs; không trình bày code do công cụ hỗ trợ như thành tích vận hành production cá nhân.

## Những câu hỏi cần tự trả lời khi phỏng vấn

- Vì sao payment_difference không tự động là lỗi pipeline?
- Kafka offset đã commit nhưng process chết trước publication thì phục hồi ra sao?
- Vì sao LSN đơn lẻ/chuỗi txID:LSN không đủ để ghép transaction?
- Delete rồi stale update đến muộn được xử lý thế nào?
- Vì sao full rebuild parity vẫn cần independent Decimal/raw-source oracle?
- Vì sao marts vẫn rebuild và publication còn copy full file? Điểm nghẽn tiếp theo là gì?
- Vì sao không đưa Kafka/Flink/Kubernetes vào mọi nhánh?

## Việc chỉ bạn mới quyết định/thực hiện

Tạo/chọn GitHub repository và quyền public, kiểm tra giấy phép trước khi đưa CSV thật lên mạng, quay demo bằng tài khoản của bạn, cung cấp Azure subscription/budget nếu muốn cloud slice. Local không phụ thuộc các bước này. Học SQL models và tự chạy failure drills trước khi đưa các bullet vào CV.
