<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Spec 0001 — Crawl reference metadata từ Semantic Scholar

- **Owner:** Đăng (Member 4) · **Soạn hộ:** Thiên, 27/10/2026, để kịp crawl chính thức · **Reviewer:** Thiên, Tiến · **Trạng thái:** chờ Đăng duyệt
- **Branch:** `feat/references` · Code: `src/crawler/references.py`, test: `src/tests/test_references.py`

## Yêu cầu của đề

- *"The reference metadata must additionally be crawled for each publication via the Semantic Scholar API."*
- Mỗi reference lưu *"its title, authors, submission date, arXiv ID when one exists, and Semantic Scholar ID"*; *"Keep all crawled references locally"*. Chỉ reference có `externalIds.ArXiv` mới vào bảng `references`.
- Field xin theo dạng phẳng `references.title, references.authors, references.year, references.publicationDate, references.venue, references.externalIds` (đề ghi dạng lồng như `authors.name` bị từ chối).
- Giới hạn: không key thì 1 request/s và 100 request/5 phút; có key thì ổn định 1 request/s.

## Spike (27/10/2026, không key)

| Endpoint | Kết quả |
| --- | --- |
| `GET /paper/arXiv:{id}` | 429 ngay ở request đầu (pool chung đang quá tải) |
| `POST /paper/batch` (5 ID) | 200. Mỗi phần tử là một bài kèm `references` đầy đủ: 218, 76, 0, 41 reference. ID không tồn tại trả `null` |
| `GET /paper/{id}/references` | 200, phân trang bằng `offset`/`next`, mỗi phần tử là `{"citedPaper": {...}}` |
| Chạy `references.py` trên 800 bài chạy thử | 700 bài trong 137 s với **18 request**; 100 bài còn bị 429 sau 5 lần thử và được để lại cho lần sau. 35,6 reference/bài, 49,9 % có arXiv ID, 13,5 % không resolve được (`paperId = null`), 9,5 % không có ngày |

## Thiết kế

1. **Một lô 100 ID mỗi request** qua `POST /paper/batch` (API cho tối đa 500). 20.000 bài ≈ 200 request, thay vì 20.000 request (≈ 6,7 giờ có key, ≈ 17 giờ không key) như plan cũ tính.
2. **Bài có danh sách lồng đạt `NESTED_REFERENCE_CAP` (1.000)** được lấy lại đầy đủ qua `/paper/{id}/references` theo trang. Con số 1.000 là **giả định**, cần xác minh với API key (việc của Đăng). Trên 700 bài đo được, bài nhiều nhất có 202 reference.
3. **Một limiter S2 dùng chung** (`semantic_scholar_limiter()`), tách khỏi limiter arXiv, nên chạy song song trong thread mà `run_pipeline` mở sẵn.
4. **Retry** 429/5xx/lỗi kết nối tối đa 5 lần, backoff `2^n` s + jitter, ưu tiên `Retry-After`. Lô vẫn lỗi thì **không** ghi file; chạy lại sẽ thử tiếp. Bài đã có `references.json` với `status` `ok`/`not_found` thì bỏ qua.
5. **Ghi nguyên tử** (file tạm rồi đổi tên). Schema: `docs/data_contract.md`, mục 4.
6. Mỗi request ghi một dòng `S2 <METHOD> <url> -> <status>` vào log để `stats.py` đếm request và mã lỗi.

## Định nghĩa dùng trong report

- **Success rate reference metadata** = số bài `status == ok` / số bài đã crawl. Kèm tỉ lệ reference resolve được (`s2_id` khác `null`).
- **Số reference trung bình mỗi bài** = trung bình `num_references` trên các bài `ok`.

## Việc Đăng cần làm khi duyệt

- [ ] Đăng ký và đặt `S2_API_KEY` (Colab Secrets). Không có key thì lô bị 429 liên tục ở giờ cao điểm.
- [ ] Với key: xác minh trần của danh sách lồng trong batch (thử một bài có hơn 1.000 reference), sửa `NESTED_REFERENCE_CAP` nếu khác.
- [ ] Duyệt định nghĩa success rate ở trên.

## Nguồn đã xác minh

1. HCMUS CS TA. *Project Milestone 1 — Data Crawling*. https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/crawling (đọc 25/10/2026).
2. Allen Institute for AI. *Semantic Scholar Academic Graph API*. https://api.semanticscholar.org/api-docs/ — endpoint `paper/batch` và `paper/{paper_id}/references`.
3. Spike và lần chạy trên 800 bài ở trên (27/10/2026).
