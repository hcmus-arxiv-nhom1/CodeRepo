<!-- Soạn với sự hỗ trợ của AI assistant (Claude, Anthropic), ghi nguồn theo quy định môn học. -->
# Schema CSV thống kê cho book (M1)

`src/analysis/stats.py` gộp dữ liệu của 4 dải thành các CSV nhỏ dưới đây, đặt trong `data/` của book repo. Notebook EDA chỉ đọc các file này (không đọc corpus), nên build book không cần Colab hay Drive. Mọi bảng đều có cột `range_owner` ∈ {`thien`, `tien`, `hieu`, `dang`}, trùng khoá trong `config.RANGES`; dòng tổng hợp toàn corpus dùng `all`.

Quy ước: ngày ISO 8601 UTC; kích thước tính bằng byte; tỉ lệ là số thực trong [0, 1], làm tròn 4 chữ số; ô trống = không có dữ liệu (không dùng 0 để thay).

## 1. `m1_summary_by_range.csv` — 5 dòng (4 dải + `all`)

| Cột | Kiểu | Nguồn / cách tính | Thống kê bắt buộc (mục 1.2) |
| --- | --- | --- | --- |
| `range_owner` | str | khoá dải | — |
| `ids_requested` | int | số ID của dải (5.000) | — |
| `papers_found` | int | ID có `metadata.json` hợp lệ | — |
| `papers_complete` | int | bài mà mọi version ở trạng thái `ok`/`no_tex`/`unavailable` và có thư mục `vN/` | số bài crawl thành công |
| `paper_success_rate` | float | `papers_complete / ids_requested` | success rate |
| `versions_total` | int | tổng `num_versions` | — |
| `versions_ok`, `versions_no_tex`, `versions_unavailable`, `versions_failed` | int | `versions.jsonl` (bản ghi mới nhất mỗi version) | — |
| `avg_versions_per_paper` | float | `versions_total / papers_found` | — |
| `avg_paper_bytes_before` | float | trung bình tổng `bytes_before` các version mỗi bài (chỉ bài complete) | kích thước TB trước khi xoá hình |
| `avg_paper_bytes_after` | float | như trên với `bytes_after` | kích thước TB sau khi xoá hình |
| `avg_refs_per_paper` | float | trung bình số reference (mọi reference) mỗi bài có `references.json` | số reference TB mỗi bài |
| `avg_refs_with_arxiv_id` | float | như trên, chỉ reference có `ref_arxiv_id` | — |
| `ref_success_rate` | float | bài có reference crawl xong / `papers_found` | success rate reference metadata |

`all` cộng các cột đếm và tính lại các cột trung bình / tỉ lệ trên toàn corpus (không lấy trung bình của trung bình).

## 2. `m1_papers.csv` — 1 dòng mỗi bài (tối đa 20.000 dòng)

Dùng cho phân phối, boxplot, ngoại lệ trong EDA.

| Cột | Kiểu | Nguồn |
| --- | --- | --- |
| `range_owner`, `arxiv_id` | str | khoá |
| `num_versions` | int | `metadata.json` |
| `submission_date` | str | `metadata.json` |
| `has_venue` | bool | `venue` khác null |
| `n_authors` | int | `len(authors)` |
| `versions_ok`, `versions_no_tex`, `versions_unavailable`, `versions_failed` | int | `versions.jsonl` |
| `complete` | bool | như `papers_complete` |
| `n_tex_files`, `n_bib_files` | int | file giữ lại, mọi version |
| `bytes_before`, `bytes_after` | int | tổng các version |
| `dropped_bytes` | int | `bytes_before - bytes_after` (cần trường file bị bỏ của spec 0002) |
| `n_refs`, `n_refs_with_arxiv_id` | int | `references.json`; trống nếu chưa crawl |
| `refs_status` | str | `ok` / `missing` / `failed` |
| `qa_errors` | int | số dòng `error` của bài trong `qa_report.csv` |

## 3. `m1_performance.csv` — 1 dòng mỗi lần chạy được báo cáo

Sinh từ `run_summary.json`, `runs.jsonl` và profiler. Chỉ lần chạy benchmark chính thức (`scope = benchmark`) mới được dùng làm số liệu hiệu năng trong report.

| Cột | Kiểu | Nguồn | Thống kê bắt buộc |
| --- | --- | --- | --- |
| `run_id`, `scope` | str | nhãn chạy; `trial` / `crawl-shift` / `benchmark` | — |
| `range_owner` | str | dải hoặc `all` | — |
| `ids` | int | số ID chạy | — |
| `wall_s` | float | `run_summary.wall_s`, end-to-end | thời gian chạy |
| `discovery_s` | float | `run_summary.discovery_s` | tổng thời gian entry discovery |
| `avg_paper_s` | float | `wall_s / ids` | thời gian TB mỗi bài |
| `mean_version_s` | float | `run_summary.mean_version_s` | thời gian xử lý một bài/version |
| `arxiv_requests`, `min_request_gap_s` | int, float | `run_summary` | chứng minh tuân thủ politeness |
| `ram_peak_mb`, `ram_mean_mb` | float | profiler: tổng RSS process chính và mọi process con | RAM lớn nhất / trung bình |
| `disk_peak_mb` | float | profiler: `data/` + `work/` | disk lớn nhất |
| `output_bytes` | int | `du` của corpus cuối | dung lượng output |
| `cpu_count`, `ram_total_gb` | int, float | `os.cpu_count()`, `psutil` | cấu hình testbed |
| `time_v_max_rss_kb` | int | `/usr/bin/time -v`, dùng đối chiếu RAM | kiểm chứng được |

## 4. `m1_profiler_samples.csv` — chuỗi thời gian của lần benchmark

| Cột | Kiểu | Ghi chú |
| --- | --- | --- |
| `run_id` | str | khoá tới `m1_performance.csv` |
| `t_s` | float | giây kể từ lúc bắt đầu |
| `rss_mb` | float | tổng RSS (process chính + process con) |
| `data_bytes`, `work_bytes` | int | dung lượng hai thư mục tại thời điểm lấy mẫu |

## 5. Ràng buộc kiểm tra tự động

- Mỗi `range_owner` trong `m1_papers.csv` có ≤ 5.000 dòng; tổng bằng `papers_found` của `all`.
- `paper_success_rate = papers_complete / ids_requested` khớp giữa bảng 1 và bảng 2.
- `bytes_after <= bytes_before` ở mọi dòng.
- `avg_paper_bytes_*` trong bảng 1 khớp trung bình tính lại từ bảng 2.
- `ram_peak_mb >= ram_mean_mb`; `ram_peak_mb` lệch `time_v_max_rss_kb` không quá một ngưỡng đã ghi trong report.
- Số liệu trong report lấy từ các CSV này qua code cell, không gõ tay.

## 6. Việc còn phụ thuộc nhóm khác

- Định dạng `references.json` (cột `n_refs`, `ref_success_rate`): chờ spec 0001 của Đăng.
- `dropped_bytes`, `n_tex_files`, `n_bib_files`: chờ trường file bị bỏ trong `versions.jsonl` (spec 0002 của Thiên).
- `ram_*`, `disk_peak_mb`, `m1_profiler_samples.csv`: chờ `src/utils/profiler.py` của Hiếu.
