# Thiết kế đã triển khai — Trusted Commerce Data Platform

Cập nhật 21/10/2026. Một repo, ba nguồn phục vụ được gắn nhãn rõ: Olist lịch sử, correction trên bản sao lịch sử, CDC từ PostgreSQL mô phỏng.

## Data products và grain

| Model | Grain | Quy tắc |
|---|---|---|
| fact_orders | order_id | Aggregate item/payment riêng trước khi join; Decimal(18,2) |
| fact_order_items | order_id + order_item_id | Giá hàng và freight ở cấp item |
| fact_payments | order_id + payment_sequential | Không nhân payment_value với installments |
| bridge_order_seller | order_id + seller_id | Không cộng số cặp thành số đơn |
| order_review_summary | order_id | Latest deterministic; lịch sử giữ trong silver_order_reviews |
| dim_zip | ZIP prefix | Median lat/lon, số observations; join sau aggregation |
| fact_order_geography | order_id | Thiếu tọa độ không loại đơn; giữ coverage |
| mart_corridors | seller_state + customer_state | Order–seller pairs, median/p90, khoảng cách đường chim bay |
| customer_observation | customer_unique_id | First/second observed order, cutoff=max purchase date |
| mart_customer_cohorts | tháng first-observed + horizon | 30/60/90 ngày; calendar spine; chỉ khách mature vào mẫu số |
| mart_payment_control | status + item/payment presence | Coverage và source discrepancy, không suy diễn refund |
| mart_review_experience | status + late flag | Coverage, average score; không suy ra nhân quả |

Cùng với dim_order_customer, dim_seller, dim_product, mart_delivery_daily và mart_seller_delivery: **17 model**. Cohort đo hành vi tạo đơn accepted, gồm canceled/unavailable, không phải repeat completed purchase. Observed merchandise là giá trị hàng trong đơn, không phải lợi nhuận, platform revenue hay LTV.

## Historical path

CSV checksum/inventory → versioned PostgreSQL source → bounded extraction → immutable MinIO raw → Spark local[2] validation/FK/dedup/quarantine → committed Parquet → immutable DuckDB generation → atomic current.json.

1.550.922 raw records trong nguồn người dùng cung cấp; 98.059 accepted orders. Batch lịch sử là full snapshot, không phải CDC. Curated batch hiện giữ contract v1; thay đổi semantics ingestion phải dùng identity/migration mới thay vì tái dùng commit cũ. Serving generation bao gồm hash SQL, model runner và publication code.

## Incremental path

Ba bảng mutable: orders, order_items, order_payments. Dimensions và review history cố định trong namespace này. Nhận full after-image hoặc delete, natural key đúng type, namespace, contract_version, event ID và position [source sequence, transaction order].

Raw batch được lưu trước apply. DuckDB transaction bao gồm canonical state, version/tombstone, audit before/after, impacted facts và aggregate marts. Duplicate ID khác payload bị chặn; cùng source position khác nội dung bị chặn; stale update không làm sống lại tombstone. Parent delete còn children/reviews bị chặn bởi FK; không cascade ngầm. Parent delete hợp lệ phải không còn bản ghi tham chiếu.

Facts/order review summary/geography refresh theo impacted order. Marts/customer aggregates rebuild đầy đủ để bao phủ nhóm cũ và nhóm mới. Đây chưa phải selective aggregation trên quy mô lớn. SQL/engine signature thay đổi sẽ full rebuild serving models trong transaction; raw source identity không bị sửa.

Mỗi batch kiểm PK/FK, per-order money, Python Decimal oracle và 17-model full rebuild parity tại cùng canonical state. Reference dùng DuckDB độc lập. Tiền so sánh chính xác; descriptive floats so ở độ chính xác 1e-8. Không dùng parity cùng SQL làm bằng chứng độc lập duy nhất: còn raw-CSV oracle và fixture expected values.

Single writer lock; không tự chiếm lock bỏ lại sau crash. Bảo đảm không còn writer trước khi xử lý lock thủ công. Publication copy/checksum toàn bộ database, rồi đổi pointer atomically; failed publication giữ generation trước. Không tuyên bố fsync thư mục/cross-platform power-loss durability.

## CDC thật, nguồn synthetic

PostgreSQL 16.10 `olist_sim` có typed tables, PK và FK → Debezium 3.2.4.Final/pgoutput → Kafka 3.9.1 → SQLite durable inbox → complete transaction apply → publication `data/cdc`.

Connector dùng role `olist_cdc_reader` với LOGIN/REPLICATION, USAGE schema và SELECT đúng ba bảng; admin chỉ provision publication và simulator. Secret sinh ngẫu nhiên nằm trong data/cdc bị Git/package loại; Connect REST không mở host port. Đây là local lab, chưa triển khai TLS/auth Kafka hoặc HA.

Kafka offset được commit sau khi inbox đã commit (`synchronous=FULL`), trước publication. Khởi động lại đọc lại inbox; engine idempotent xử lý phần đã hoàn tất. Transaction ID PostgreSQL dạng txID:LSN thay đổi theo operation: ghép theo txID trong khoảng BEGIN–END, kiểm count, total_order liên tục và collection counts. Không vượt qua END còn thiếu dữ liệu. Snapshot phải đủ baseline counts trước khi áp dụng. Truncate, dimension mutation và schema drift không được hỗ trợ, phải fail closed.

Journal giới hạn 50.000 records; triển khai thử nghiệm chỉ một consumer/journal, topics một partition. Chưa có retention/archive tự động; WAL slot giới hạn 512 MB. Không xóa inbox/checkpoint hoặc đổi baseline tùy tiện. Khi WAL bị mất phải resnapshot vào namespace được kiểm soát, không báo đã catch-up.

## dbt local

`sql/local_marts.sql` là nguồn SQL duy nhất. `models.export_dbt` sinh project với ref/source dependency, 17 models và tests. `verify_dbt_local.py` chạy dbt build trên bản sao database rồi đối chiếu mọi model. dbt 1.9.2 + dbt-duckdb 1.9.6 + DuckDB 1.2.2 đã được chạy.

Serving correction hiện dùng Python/DuckDB để transaction gồm state + audit + facts/marts nguyên khối. dbt là gate thực thi/parity; không gắn nhãn engine CDC chính. Snowflake/dbt cũ là optional cloud profile, không dùng số liệu cloud nhập từ trước để chứng minh bản local.

## Dashboard và giới hạn

Bảy tab: tổng quan, seller, payment, geography, customer, order trace, trust. Nguồn được chọn riêng; một render dùng một immutable generation. Filters và export nhất quán; cohorts/corridors/global pipeline health công bố rõ không theo bộ lọc order. Missing payment/coordinates/review có coverage riêng.

Airflow finite-job orchestration và lifecycle Kafka replay vẫn giữ. CDC continuous capture tách lifecycle khỏi Airflow DAG. Chưa claim production uptime, distributed exactly-once, cloud deployment, causal inference hoặc hosted CI pass.

Tham khảo cơ chế CDC: [Debezium PostgreSQL 3.2](https://debezium.io/documentation/reference/3.2/connectors/postgresql.html), [PostgreSQL logical decoding](https://www.postgresql.org/docs/16/logicaldecoding-explanation.html).
