# Khám phá metadata theo bậc (tiered discovery) — Thiết kế

Ngày: 2026-10-28

## Bối cảnh

Lần chạy 100 ID thật (`crawler-producer`, 3 worker) mất 941 s, trong đó discovery chiếm 541 s. Producer gọi API arXiv một lần với cả 100 ID; arXiv trả `429` năm lần liên tiếp, `discover()` chuyển sang OAI-PMH `GetRecord` từng ID (200 request OAI trong log, mỗi request cách nhau 3 s). Giai đoạn tải sau đó chỉ mất 400 s cho 130 phiên bản, tức đã sát nhịp sàn 3 s mỗi request.

Hiện `produce()` khám phá toàn bộ ID rồi mới đẩy job, nên worker không có việc suốt thời gian discovery.

## Mục tiêu

- Job đầu tiên xuất hiện sớm: các request khám phá đầu tiên chỉ lấy 5–10 ID.
- Khi hàng đợi đã đủ việc cho worker, chuyển sang batch 20–100 ID để giảm số request khám phá.
- Gặp `429` thì hạ bậc thay vì dội tiếp một cú lớn, và dự phòng OAI chỉ áp dụng cho chunk nhỏ.

## Phạm vi

- Thêm `DiscoveryLadder` (hàm thuần, không I/O) và thay vòng `discover(tất cả ID)` của `produce()` bằng vòng theo chunk.
- `discover()` giữ nguyên hành vi, chỉ thêm trường đếm `api_failed_batches` vào `DiscoveryResult`.
- Không đổi worker, coordinator, định dạng job, hay `run_pipeline`.
- Không chặn producer khi tồn quá lớn (YAGNI): khám phá trước chỉ tốn vài request, job nằm trong Redis.

## Thiết kế

### Tín hiệu "đủ việc"

`backlog = download-jobs còn chờ + parser-jobs còn chờ`, mỗi vế lấy bằng `RedisJobQueue.depth()` (pending + chưa giao). Chọn tổng hai hàng đợi theo yêu cầu: hàng đợi parser đơn lẻ thường gần rỗng vì làn arXiv là nút thắt, nên không đủ để quyết định.

### DiscoveryLadder

Cấu hình qua biến môi trường (có mặc định):

| Biến | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `DISCOVERY_TIERS` | `5,10,20,50,100` | Cỡ batch của từng bậc, tăng dần; bậc cuối ≤ `ARXIV_API_BATCH` (100) |
| `DISCOVERY_BACKLOG_STEPS` | `10,30,60,100` | Tồn tối thiểu để lên bậc 2, 3, 4, 5 (số phần tử = số bậc − 1) |
| `DISCOVERY_COOLDOWN_ROUNDS` | `3` | Số vòng giữ bậc thấp sau một lần `429` |

Giao diện:

- `DiscoveryLadder(tiers, backlog_steps, cooldown_rounds)`
- `next_size(backlog: int) -> int`: bậc theo tồn, nhưng không vượt trần bậc khi đang cooldown. Mỗi lần gọi giảm bộ đếm cooldown.
- `on_rate_limited() -> None`: hạ trần xuống một bậc so với bậc vừa dùng và đặt cooldown. Hạ liên tiếp được phép, tối thiểu là bậc đầu.

Ví dụ: tồn 0 → 5 ID; tồn 12 → 10; tồn 40 → 20; tồn 70 → 50; tồn ≥ 100 → 100. Sau một `429` ở bậc 100, ba vòng tiếp theo bị chặn ở bậc 50 dù tồn lớn.

### Vòng của producer

```
remaining = ids chưa xử lý
while remaining:
    backlog = download.depth() + parser.depth()
    size = ladder.next_size(backlog)
    chunk = ID cache sẵn (miễn phí, đi hết) + tối đa `size` ID chưa có cache
    result = discover(chunk, ...)
    publish job cho các phiên bản chưa xong
    if result.api_failed_batches: ladder.on_rate_limited()
```

- ID đã có `metadata.json` không tốn request nên không tính vào `size`; chúng được đẩy job ngay.
- `wait_until_drained()` và phần ghi `run_summary.json` giữ nguyên, chạy sau khi hết ID.
- Khóa `WorkDirLock` giữ suốt vòng lặp như hiện tại.

### Thay đổi trong `discover()`

Thêm hai trường vào `DiscoveryResult`: `api_failed_batches: int = 0`, tăng ở cả hai khối `except (FetchError, XMLSyntaxError)` của các batch API (batch gốc và batch có hậu tố phiên bản); và `oai_fallbacks: int = 0`, tăng mỗi lần gọi `oai_get_record`. Không đổi gì khác.

### Số liệu mới trong `run_summary.json`

- `time_to_first_job_s`: từ lúc producer bắt đầu đến job đầu tiên được đẩy.
- `discovery_api_requests`, `discovery_oai_fallbacks`: số request API và số lần dự phòng OAI.
- `discovery_tier_history`: danh sách cỡ batch thực tế đã dùng.

`discovery_oai_fallbacks` cộng dồn `oai_fallbacks` của các chunk; `discovery_api_requests` là tổng `client.requests_sent` của các chunk trừ đi số `oai_fallbacks`.

## Xử lý lỗi

- `429` trên một chunk: `discover()` đã retry và dự phòng OAI cho các ID bị sót; với chunk 5–10 ID chi phí dự phòng tối đa 5–10 request. Producer hạ bậc.
- Redis không đọc được `depth()`: để lỗi nổi lên (producer dừng); không đoán tồn.
- Chạy lại sau khi bị ngắt: ID đã có metadata và phiên bản đã xong không tốn request, chunk đầu của lần chạy lại gần như toàn ID cache.

## Kiểm thử

Test đơn vị `DiscoveryLadder` (không mạng):
- Tồn → bậc theo `DISCOVERY_BACKLOG_STEPS`, gồm các biên (tồn đúng bằng ngưỡng).
- `on_rate_limited` hạ bậc, cooldown giữ trần đúng số vòng rồi cho tăng lại.
- Hạ liên tiếp dừng ở bậc đầu.
- Cấu hình sai (bậc không tăng dần, số ngưỡng không khớp) báo `ValueError`.

Test vòng producer với `discover` và hàng đợi giả:
- Chuỗi cỡ batch đúng thứ tự theo tồn giả lập (5, 5, 10, ...).
- ID đã cache không tính vào cỡ batch và vẫn được đẩy job.
- Job được đẩy sau mỗi chunk, không đợi hết ID.
- `api_failed_batches > 0` làm vòng kế tiếp dùng bậc thấp hơn.
- Các test producer hiện có vẫn pass.

Kiểm tra thật (cần xin phép vì gọi arXiv): chạy lại 100 ID với corpus trống, so với lần trước (discovery 541 s, 200 request OAI): `time_to_first_job_s` vài giây, `discovery_oai_fallbacks` gần 0, `min_gap_s` vẫn ≥ 3.0, `papers_complete` = 100.

## Rủi ro đã biết

- Nếu arXiv từ chối mọi cú gọi API (không phải do cỡ batch), thang bậc chỉ giảm thiệt hại (OAI cho chunk nhỏ), không loại được thời gian chờ.
- Batch nhỏ ở đầu tốn nhiều request hơn cho cùng số ID; đó là cái giá đổi lấy job sớm và ít `429`.
- Lên bậc dựa trên tồn tại một thời điểm; tồn có thể giảm nhanh ngay sau đó. Chấp nhận được vì bậc chỉ ảnh hưởng thời lượng một request khám phá.
