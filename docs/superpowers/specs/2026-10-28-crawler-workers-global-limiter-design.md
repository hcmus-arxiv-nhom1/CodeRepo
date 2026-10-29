# Crawler workers với rate limiter toàn cục — Thiết kế

Ngày: 2026-10-28

## Mục tiêu

Chạy nhiều `crawler-worker` (mặc định 3) cùng tải bài từ arXiv, số worker cấu hình qua Docker Compose. Mọi request arXiv đi qua **một coordinator chung**: khoảng cách tối thiểu 3 giây và tối đa một request đang chạy. Nhiều worker không làm tăng tần suất request.

## Phạm vi

- Một producer khám phá metadata và đẩy job theo từng phiên bản vào Redis.
- Service `crawler-worker` lấy job, tải và giải nén.
- Coordinator cấp lượt (lease) cho mọi request: discovery, tải mới, resume, retry.
- Không thay đổi parser queue hiện có.

## Kiến trúc

### Thành phần

- **Producer**: discovery, đẩy job phiên bản vào Redis. Discovery cũng xin lượt từ coordinator.
- **crawler-worker** (N replica): lấy job, xin lease, gửi request, ghi kết quả và checkpoint.
- **Coordinator** (đúng 1 instance): cấp lease toàn cục, từ chối nhịp dưới 3 giây. Khi khởi động lại nó chờ đủ một nhịp (3 giây) trước khi cấp lease đầu tiên, nên không request nào gần hơn 3 giây so với request cuối trước khi nó dừng; không cần lưu trạng thái ở nơi khác.
- **Redis**: hàng đợi job.

Worker dùng `RemoteRateLimiter` trỏ tới coordinator; không có limiter cục bộ.

### Lease

- `acquire()` trả về `lease_id` và hạn dùng (`LEASE_TTL_S`, mặc định 60 giây).
- Chỉ cấp lease mới khi không có lease còn hiệu lực **và** đã qua `ARXIV_MIN_GAP_S` (3 giây) kể từ mốc muộn hơn giữa lúc cấp lease và lúc request kết thúc.
- `release(lease_id)` được gọi khi request xong, dù thành công hay lỗi.
- Worker chết: lease tự hết hạn, coordinator thu hồi và cấp cho worker kế tiếp.
- `release` hoặc `renew` với lease đã hết hạn bị từ chối, nên worker đến muộn không ghi đè lượt của worker khác.

### Job và retry

- Reliable queue trên Redis: pending → processing.
- Job chỉ rời processing sau khi kết quả và checkpoint đã ghi; ghi kết quả phải idempotent.
- Worker chết: job quay về pending sau `JOB_VISIBILITY_TIMEOUT_S`.
- `429` hoặc lỗi tạm thời: retry với backoff tăng dần. Backoff do coordinator giữ, nên cả cụm cùng chờ.
- Vượt `MAX_ATTEMPTS`: chuyển sang dead-letter queue kèm lý do.

### Coordinator không liên lạc được

Worker không tự gửi request. Worker chờ và thử kết nối lại.

## Docker Compose

- Service `crawler-worker`, cùng image với crawler hiện có, chế độ worker; `deploy.replicas: ${CRAWLER_WORKERS:-3}`. Có thể ghi đè bằng `docker compose up --scale crawler-worker=N`.
- Phụ thuộc `redis` và `coordinator` với `condition: service_healthy`.
- `coordinator` chạy đúng một instance, có healthcheck, không scale.
- Biến môi trường: `CRAWLER_WORKERS`, `ARXIV_MIN_GAP_S=3`, `LEASE_TTL_S=60`, `JOB_VISIBILITY_TIMEOUT_S`, `MAX_ATTEMPTS`, `COORDINATOR_URL`.
- `restart: unless-stopped` cho worker.
- Dữ liệu đầu ra dùng volume chung; mỗi worker ghi file tạm riêng rồi đổi tên nguyên tử.

## Kiểm thử

Test đơn vị/tích hợp:

- Không bao giờ có hai lease đồng thời.
- Khoảng cách tối thiểu 3 giây giữa hai request.
- Thu hồi lease khi worker chết, không cấp trùng.
- `429` làm cả cụm lùi lại.
- Job được chạy lại sau khi worker chết.
- Coordinator khởi động lại vẫn giữ khoảng cách (chờ một nhịp trước khi cấp lease đầu tiên).

Test thật với 100 ID, 3 worker, cấu hình như production:

1. `min_request_gap_s` ≥ 3, không có lease đồng thời, `breaker_trips` = 0.
2. `papers_complete` = 100, `failed_rows` = 0, dead-letter rỗng. Bài PDF-only (`no_tex`) là trạng thái hợp lệ.
3. Dừng một worker giữa chừng bằng `docker kill`: lease hết hạn, job chạy lại, kết quả vẫn đủ 100.
4. Thời gian chạy tối thiểu khoảng 5–7 phút (100–140 request, mỗi request cách nhau ≥ 3 giây). Nhanh hơn mức này nghĩa là limiter sai.

## Điểm cần xác nhận khi triển khai

- Mức coordinator hiện có (`src/crawler/coordinator.py`, `RemoteRateLimiter`) đã đáp ứng bao nhiêu phần trên; lease/hết hạn là phần cần bổ sung.
- Cách `scheduler.py` và `queue.py` hiện tại tách vai trò producer và worker.
