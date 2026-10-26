# CodeRepo — Nhóm 1, arXiv Project (Nhập môn Khoa học Dữ liệu, HCMUS)

<!-- README được soạn với sự hỗ trợ của AI assistant (Claude, Anthropic), ghi nguồn theo quy định môn học. -->

> **Không đổi tên organization hay repo đến hết hạn poster:** link nộp Moodle sẽ chết.

- **Book (link nộp Moodle):** <https://hcmus-arxiv-nhom1.github.io/arxiv-book/>
- **Code repo:** <https://github.com/hcmus-arxiv-nhom1/CodeRepo>
- **Đề bài:** <https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/>
- Data contract: [`docs/data_contract.md`](docs/data_contract.md) · Spec: [`specs/`](specs/)

## Môi trường

- **Python 3.12**, trùng với runtime của Google Colab. Mọi package được pin trong `src/requirements.txt`; công cụ dev (pytest, ruff, pre-commit) nằm trong `src/requirements-dev.txt`.
- Không cần GPU. Benchmark chính thức chạy trên **Google Colab CPU-only**.

```bash
# pip + venv
python -m venv .venv
source .venv/bin/activate              # Windows: .venv\Scripts\activate
pip install -r src/requirements.txt -r src/requirements-dev.txt
pre-commit install                     # ruff, nbstripout, chặn file > 500 KB

# hoặc uv
uv venv .venv --python 3.12
uv pip install -r src/requirements.txt -r src/requirements-dev.txt

# kiểm tra
ruff check . && ruff format --check . && pytest
```

### Secrets

Không commit secrets. Trên Colab đặt trong **Secrets** (biểu tượng chìa khoá); trên máy dùng biến môi trường.

| Biến | Dùng cho |
| --- | --- |
| `S2_API_KEY` | API key Semantic Scholar: nhịp 1,1 s thay vì 3 s, ít bị 429 |
| `ARXIV_CONTACT_EMAIL` | Email nhóm, gắn vào User-Agent gửi tới arXiv |
| `DATA_ROOT`, `WORK_DIR` | Thay mặc định `data/corpus` và `work` (tương đương `--data-root`, `--work-dir`) |

## Chạy trên Google Colab

| Notebook | Dùng khi |
| --- | --- |
| [`src/notebooks/colab_crawl.ipynb`](src/notebooks/colab_crawl.ipynb) | Crawl chính thức 4 dải bằng **một** instance, chia ca giữa các thành viên. Đồng bộ Google Drive sau mỗi 500 ID; dấu `SHIFT.json` trên Drive chặn hai ca chạy cùng lúc |
| [`src/notebooks/colab_benchmark.ipynb`](src/notebooks/colab_benchmark.ipynb) | Benchmark M1 trên runtime mới, chỉ CPU: `/usr/bin/time -v` + profiler, in đủ số liệu hiệu năng và xuất CSV cho book |

Mở bằng *File → Open notebook → GitHub*, dán link repo, chọn notebook, rồi **Run all**.

## QA, thống kê và đo hiệu năng

```bash
# Kiểm tra toàn vẹn một dải theo data contract -> work/qa_report.csv (exit 1 nếu có lỗi)
python -m src.qa.validate --range thien --data-root data/corpus --work-dir work

# Đo RAM (tổng mọi process) và disk của cả lần chạy
python -m src.utils.profiler --out work/profile --watch data/corpus --watch work -- \
    python -m src.run_pipeline --range thien --no-progress

# Xuất CSV thống kê cho book (thay CSV giả trong arxiv-book/data/m1)
python -m src.analysis.stats --data-root data/corpus --work-dir work \
    --profile-dir work/profile --out ../arxiv-book/data/m1

# Crawl lại reference Semantic Scholar riêng (vd. bù sau khi có API key)
python -m src.crawler.references --data-root data/corpus
```

## Crawler (M1)

Chạy trong thư mục gốc của repo, sau khi cài `pip install -r src/requirements.txt`.

```bash
# Xem danh sách ID của một dải (không gọi mạng)
python -m src.run_pipeline --range thien --dry-run

# Crawl cả dải của một thành viên (thien | tien | hieu | dang, định nghĩa trong src/config.py)
python -m src.run_pipeline --range thien --no-progress

# Chạy thử một phần dải: bỏ qua 1207 ID đầu, lấy 200 ID (vắt qua ranh giới tháng 6/7)
python -m src.run_pipeline --range thien --offset 1207 --limit 200 --work-dir work/trial-thien

# Dải tuỳ ý hoặc danh sách ID
python -m src.run_pipeline --start 2606.30734 --end 2607.03693 --month-end 2606=32040
python -m src.run_pipeline --ids 2606.30762,2607.00001
```

- **Resume:** bị ngắt (Colab mất phiên, Ctrl+C, kill) thì chạy lại **đúng lệnh cũ**: version đã xong được bỏ qua, version lỗi được thử lại. Trên Colab phải đồng bộ cả `data/corpus/` **và** `work/` thì mới resume được.
- `--month-end YYMM=N`: số thứ tự cuối của tháng mà dải đi qua. Tháng 6/2026 đã xác nhận bằng API là `2606.32040` (lưu sẵn trong `config.MONTH_END`).
- `--keep-ext .tex,.bib,.bbl`: đổi danh sách đuôi file được giữ (mặc định `.tex,.bib` theo đề M1).
- `--snapshot <file>`: dùng thêm Kaggle snapshot cho ngày các version (không bắt buộc).
- `--no-references`: bỏ bước Semantic Scholar; `--refresh-metadata`: chạy lại discovery cho ID đã có `metadata.json`.

Biến môi trường mà crawler đọc (trên Colab đặt trong **Secrets**, không commit):

| Biến | Dùng cho |
| --- | --- |
| `ARXIV_CONTACT_EMAIL` | Email nhóm trong User-Agent gửi tới arXiv |
| `S2_API_KEY` | Semantic Scholar API key (`semantic_scholar_limiter()` tự chọn nhịp 1,1 s khi có key, 3 s khi không) |
| `DATA_ROOT` | Thư mục corpus, mặc định `data/corpus` (tương đương `--data-root`) |
| `WORK_DIR` | Sổ sách crawler, mặc định `work` (tương đương `--work-dir`) |

### Cách crawler gọi arXiv

- Chỉ `export.arxiv.org` (API, `/src/`) và `oaipmh.arxiv.org`. Mọi request đi qua **một** `RateLimiter` dùng chung: tối đa 1 request đang chạy, các lần bắt đầu cách nhau ≥ 3 s + jitter 0–0,5 s. `run.log` ghi `gap` của từng request để kiểm chứng.
- Discovery: arXiv API `id_list` 100 ID/request (khoảng 50 request cho 5.000 ID). Bài có ≥ 3 version lấy ngày v2..v(N-1) bằng truy vấn ID có version (`2606.30762v2`); OAI-PMH `arXivRaw` là đường dự phòng.
- Tải `/src/{id}vN` cho từng version, nhận diện loại file bằng magic bytes (tar.gz, gz 1 file, tar, zip, PDF/PS, HTML), chỉ giữ file theo whitelist.
- Retry 429/5xx/timeout/đứt kết nối tối đa 5 lần, backoff `2^n` s, tôn trọng `Retry-After`. 3 lần 403 liên tiếp → dừng 15–30 phút, giảm nhịp còn 5 s; lặp lại quá 3 lần thì dừng hẳn (`BlockedError`) để resume sau.

### Reference metadata (Semantic Scholar)

`run_pipeline` tự chạy crawl reference trong một thread riêng nếu `src/crawler/references.py` có hàm:

```python
def crawl(arxiv_ids: list[str], data_root: Path) -> None:
    """Ghi <data_root>/<yymm-id>/references.json, bỏ qua bài đã có, dùng semantic_scholar_limiter()."""
```

Chưa có module này thì pipeline bỏ qua bước reference và ghi cảnh báo vào log.

### Output

```text
data/corpus/                 # --data-root: chỉ chứa dữ liệu M1
  2606-30734/                # arXiv ID dạng yymm-id (src/utils/ids.py)
    metadata.json            # arxiv_id, title, authors[], submission_date, revised_dates[], venue, num_versions, categories[], doi
    references.json          # Semantic Scholar (references.py)
    v1/                      # chỉ file .tex/.bib của version 1; rỗng nếu version không có TeX
    v2/
work/                        # --work-dir: sổ sách của crawler, không phải corpus
  versions.jsonl             # mỗi version 1 dòng: status, kind, files, bytes_before/after, thời gian
  discovery.jsonl            # mỗi ID 1 dòng: found/missing, nguồn (api/oai/snapshot)
  failed.csv                 # những gì còn lỗi sau lần chạy gần nhất
  run_summary.json           # số liệu lần chạy gần nhất (runs.jsonl giữ mọi lần)
  run.log                    # từng request arXiv kèm timestamp
```

Ngày trong `metadata.json` dạng ISO 8601 UTC (`2026-06-29T18:01:23Z`); schema chính thức nằm trong data contract. Trạng thái mỗi version trong `versions.jsonl`: `ok` (có TeX), `no_tex` (PDF/PS… → `vN/` rỗng), `unavailable` (arXiv trả 404 → `vN/` rỗng), `failed` (thử lại ở lần chạy sau).

## Multi-file gathering (M2)

```bash
# Tìm file .tex gốc của mọi version trong corpus, thống kê ca mơ hồ / \input không tìm thấy
python -m src.parser.gather data/corpus

# Chọn tập mẫu ~50 bài đa dạng cho M2 và chép sang thư mục riêng
python -m src.parser.sample_set --data-root data/corpus --work-dir work --out data/m2-sample
```

Trong code: `gather(version_dir)` trả về file gốc và danh sách file `.tex` tới được theo `\input`/`\include`; `expand(version_dir)` trả về toàn văn của file gốc với các `\input`/`\include` đã được thay bằng nội dung file con.

## Tài liệu tham khảo (crawler và gathering)

Các tài liệu trực tuyến mà code crawler và `gather.py` dựa vào:

1. HCMUS CS TA. (2026). *Project Milestone 1 — Data Crawling* và *Milestone 2 — Hierarchical Parsing and Standardization*. https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/ — yêu cầu chỉ giữ `.tex`/`.bib`, record rỗng, quy tắc tìm file gốc và `\input`/`\include`.
2. arXiv.org. *arXiv API User's Manual*. https://info.arxiv.org/help/api/user-manual.html — `id_list`, hậu tố version của ID, các trường Atom (`published`, `updated`, `journal_ref`, `doi`).
3. arXiv.org. *Terms of Use for arXiv APIs*. https://info.arxiv.org/help/api/tou.html — nhịp ≥ 3 giây giữa các request, một kết nối.
4. arXiv.org. *Open Archives Initiative (OAI) — arXiv info*. https://info.arxiv.org/help/oa/index.html — OAI-PMH `GetRecord` với `metadataPrefix=arXivRaw` tại `https://oaipmh.arxiv.org/oai`.
5. arXiv.org. *Media types*. https://info.arxiv.org/help/mimetypes.html — các dạng source mà `/src/` trả về (tar.gz, gzip một file, PDF, HTML).
6. Cornell University. (2025). *arXiv Dataset*. Kaggle. https://www.kaggle.com/datasets/Cornell-University/arxiv — Kaggle snapshot tuỳ chọn (`--snapshot`).
7. Allen Institute for AI. (2026). *Semantic Scholar Academic Graph API*. https://api.semanticscholar.org/api-docs/ — giới hạn tốc độ dùng cho `semantic_scholar_limiter()`.
8. Python Software Foundation. *tarfile*, *gzip*, *zipfile* — Python 3.12 standard library documentation. https://docs.python.org/3.12/library/archiving.html — đọc archive từng member, không dùng `extractall`.
