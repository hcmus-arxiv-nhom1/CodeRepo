<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Spec 0002 — Tinh chỉnh crawler cho lần crawl chính thức

- **Tác giả:** Thiên (Member 1) · **Reviewer:** Đăng, Hiếu · **Trạng thái:** chờ duyệt
- **Ngày:** 27/10/2026 · **Branch:** `feat/crawler-tuning`

## Bối cảnh

Thầy xác nhận điều được chấm là pipeline cào có tối ưu không, đo trên Google Colab chỉ dùng CPU. Mọi nhóm đều bị giới hạn bởi quy định của arXiv: *"wait at least 3 seconds between successive calls and use one connection at a time"*, và giới hạn này tính trên *"all of the machines under your control as a whole"* [1]. Vì vậy chỉ còn hai hướng tối ưu: bỏ thời gian chờ thừa, và không bao giờ vô tình gọi arXiv song song.

## Thay đổi

| # | Thay đổi | Lý do | Kiểm chứng |
| --- | --- | --- | --- |
| 1 | `ARXIV_JITTER_S` 0,5 → **0,1 s** (`config.py`) | Khoảng cách trung bình giữa hai lần bắt đầu request giảm từ 3,25 s xuống 3,05 s, tức nhanh hơn khoảng 6 %. Khoảng cách tối thiểu vẫn là 3,0 s nên vẫn đúng ToU | `min_request_gap_s` trong `run_summary.json` ≥ 3,0 s |
| 2 | **Khoá `work/run.lock`** (`src/crawler/worklock.py`), dùng `flock` trên Linux và `msvcrt` trên Windows | Hai `run_pipeline` trên cùng một `work/` (vd. một ca bị bật hai lần) sẽ gọi arXiv song song. Khoá gắn với file đang mở nên tự nhả khi process chết, không có khoá "chết" | `test_worklock.py`; `test_second_run_on_same_work_dir_is_refused`; `test_lock_is_released_when_a_run_stops_on_an_error` |
| 3 | `semantic_scholar_limiter()` là **singleton** | Trước đây mỗi lần gọi tạo một limiter mới, nên hai luồng S2 có thể vượt 1 request/s | `test_semantic_scholar_limiter_is_shared` |
| 4 | Ghi **file bị bỏ** vào `versions.jsonl`: trường `dropped` = `{đuôi file: {"files", "bytes"}}`; source không có TeX ghi theo loại, vd. `"(pdf)"` | Report cần giải thích kích thước trước/sau khi xoá hình. Đề chỉ cho giữ `.tex` và `.bib` [2], nên số liệu phải cho thấy phần bị bỏ là gì (hình, `.bbl`, `.sty`…) | `test_targz_keeps_tex_and_bib_only_and_figure_commands`, test end-to-end |

## Không làm: worker giải nén riêng

Plan đề xuất tách giải nén sang một process thứ hai. Số đo trên 1.058 version chạy thử (19–20/10): thời gian giải nén **trung vị 19 ms, p90 66 ms, tối đa 0,57 s**, so với nhịp 3,05 s của làn arXiv. Giải nén chiếm chưa tới 1 % thời gian mỗi version, nên process thứ hai chỉ tốn thêm RAM (cũng là số liệu bị so sánh) và độ phức tạp, không làm crawl nhanh hơn. Phần chạy song song có lợi là Semantic Scholar (thread riêng, limiter riêng) và parser M2 (`multiprocessing` theo bài).

## Ảnh hưởng

- Không đổi schema `metadata.json`/`references.json`; `versions.jsonl` có thêm trường `dropped`.
- Không đổi nhịp tối thiểu 3 s và vẫn chỉ một kết nối tới arXiv.

## Nguồn đã xác minh

1. arXiv.org. *Terms of Use for arXiv APIs*. https://info.arxiv.org/help/api/tou.html (đọc 21/10/2026).
2. HCMUS CS TA. *Project Milestone 1 — Data Crawling*. https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/crawling (đọc 25/10/2026).
3. Python 3.12 docs: `fcntl.flock`, `msvcrt.locking`. https://docs.python.org/3.12/library/fcntl.html, https://docs.python.org/3.12/library/msvcrt.html
