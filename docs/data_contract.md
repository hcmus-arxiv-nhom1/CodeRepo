<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Data contract — Nhóm 1

- **Owner:** Đăng (Member 4, A/R) · Thiên, Tiến (C) · Soạn: 27/10/2026, cần 4 approve
- Áp dụng cho dữ liệu làm việc của M1 và các file CSV mà book đọc. Đối chiếu đề: [M1 — Data Crawling](https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/crawling), [M2](https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/parsing), [M3](https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/matching).
- Đổi bất kỳ tên trường, kiểu dữ liệu hay đường dẫn nào ở đây phải có spec được duyệt trước (Rule 3).

## 1. Khoá và thư mục

- arXiv ID kiểu mới `2606.30734` ↔ thư mục `2606-30734`. ID kiểu cũ (`hep-th/9901001`) giữ nguyên. Mọi module dùng `src/utils/ids.py`.
- Version: hậu tố `v<N>`, bài chỉ có một version là `v1`.

```text
data/corpus/                      # --data-root (DATA_ROOT)
  2606-30734/
    metadata.json                 # mục 2
    references.json               # mục 4
    v1/  v2/ …                    # mục 3
work/                             # --work-dir (WORK_DIR): sổ sách, đồng bộ Drive cùng corpus
  versions.jsonl  discovery.jsonl  failed.csv  run_summary.json  runs.jsonl  run.log  run.lock
  profile/                        # profiler (mục 5)
  qa_report.csv                   # validate.py (mục 5)
```

## 2. `metadata.json` (một file cho mỗi bài)

| Trường | Kiểu | Ghi chú |
| --- | --- | --- |
| `arxiv_id` | string, bắt buộc | Trùng tên thư mục sau khi đổi `.` → `-` |
| `title` | string | Từ arXiv API |
| `authors` | list[string] | Theo thứ tự trên arXiv |
| `submission_date` | string ISO 8601 UTC `YYYY-MM-DDThh:mm:ssZ` | Ngày của `v1` |
| `revised_dates` | list[string ISO] | Ngày của `v2`…`vN`, tăng dần |
| `venue` | string \| null | `journal_ref` của arXiv |
| `num_versions` | int | Luôn bằng `1 + len(revised_dates)` |
| `categories` | list[string] | Thêm, không thuộc bảng `papers` |
| `doi` | string \| null | Thêm, không thuộc bảng `papers` |

Đề gợi ý lưu metadata *"for each version"*. Nhóm lưu theo bài vì bảng `papers` có một dòng mỗi bài, và ngày của từng version đã nằm trong `submission_date` + `revised_dates`. Câu hỏi này đang chờ TA (plan, Phụ lục C).

## 3. Thư mục version `vN/`

- Chỉ chứa `.tex` và `.bib` lấy nguyên byte từ archive, giữ cấu trúc thư mục con. Không sửa nội dung `.tex`.
- Version không có TeX (chỉ PDF/PS/HTML, hoặc arXiv trả 404) vẫn có thư mục `vN/`, **rỗng** hoặc chỉ có `.bib`: đây là *empty record*, không bỏ bài.
- Trạng thái và lý do của từng version nằm trong `work/versions.jsonl` (bản ghi cuối cùng của mỗi `(arxiv_id, version)` là bản có hiệu lực):

| Trường | Ý nghĩa |
| --- | --- |
| `status` | `ok` (có ít nhất một `.tex`), `no_tex`, `unavailable` (404/410), `failed` (sẽ thử lại) |
| `kind` | `tar.gz`, `gz`, `tar`, `zip`, `tex`, `pdf`, `ps`, `html.gz`, … |
| `files` | Đường dẫn các file đã giữ, tương đối với `vN/` |
| `archive_bytes` | Kích thước file tải về |
| `bytes_before` / `bytes_after` | Tổng kích thước giải nén của mọi file / của file đã giữ |
| `dropped` | `{đuôi: {"files", "bytes"}}` những gì whitelist bỏ đi (spec 0002) |
| `download_s`, `extract_s`, `total_s`, `attempts`, `finished_at` | Thời gian và số lần thử |

## 4. `references.json` (một file cho mỗi bài)

```json
{
  "arxiv_id": "2606.30762",
  "status": "ok",
  "s2_paper_id": "d7a3896bc1a28b40aa13c705ad0a7c72d216f25d",
  "fetched_at": "2026-10-27T01:58:45Z",
  "num_references": 218,
  "num_arxiv_references": 181,
  "references": [
    {
      "s2_id": "53c5d8e56ec75937eecd0aec9c6fe6379e505ab1",
      "title": "Realization of fractional Fermi seas",
      "authors": ["Yi Zeng", "Alvise Bastianello", "Sudipta Dhar", "…"],
      "submission_date": "2026-02-19",
      "year": 2026,
      "venue": null,
      "arxiv_id": "2602.17657",
      "external_ids": {"ArXiv": "2602.17657", "CorpusId": 285787392}
    }
  ]
}
```

- `status`: `ok` (S2 biết bài này, kể cả khi 0 reference) hoặc `not_found`. Lô bị lỗi thì **không** ghi file, để lần chạy sau thử lại.
- **Giữ mọi reference** S2 trả về, có hay không có arXiv ID (*"Keep all crawled references locally"*).
- `s2_id` có thể `null` khi S2 không resolve được trích dẫn (đo trên 700 bài: 13,5 %). Reference vẫn được giữ.
- `submission_date` = `publicationDate`, nếu không có thì là năm (`"1999"`), nếu không có nữa thì `null` (đo được 9,5 % là `null`).
- `arxiv_id` giữ nguyên dạng S2 trả về, kể cả ID kiểu cũ.

## 5. File sinh thêm

| File | Do ai sinh | Nội dung |
| --- | --- | --- |
| `work/profile/profile_summary.json` | `src/utils/profiler.py` | `wall_s`, `rss_max_mb`, `rss_mean_mb` (tổng RSS của process chính và mọi process con), `disk_max_mb`, `output_mb`, `cpu_count`, `ram_total_mb`, `exit_code` |
| `work/profile/profile_samples.csv` | profiler | `t_sec, rss_total_mb, n_procs, disk_mb` |
| `work/qa_report.csv` | `src/qa/validate.py` | `arxiv_id, version, severity (error/warning), check, detail` |

## 6. Ánh xạ sang bảng Hugging Face (M3)

| Bảng | Cột | Lấy từ |
| --- | --- | --- |
| `papers` | `arxiv_id`, `title`, `authors`, `submission_date`, `revised_dates`, `venue` | `metadata.json`, giữ nguyên tên trường |
| `references` | `arxiv_id` (bài trích dẫn), `ref_arxiv_id`, `ref_title`, `ref_authors`, `ref_submission_date`, `ref_s2_id` | `references.json`: chỉ các reference có `arxiv_id` khác `null`; `ref_arxiv_id = arxiv_id`, `ref_title = title`, `ref_authors = authors`, `ref_submission_date = submission_date`, `ref_s2_id = s2_id` |

M2 (để thống nhất từ bây giờ): version không có TeX vẫn phải được ghi nhận là rỗng (*"record the version as empty rather than skipping it"*), dựa trên `status` trong `versions.jsonl`.

## 7. CSV cho book (`arxiv-book/data/m1/`), do `src/analysis/stats.py` sinh

**`papers_stats.csv`** — mỗi dòng một bài trong dải:

| Cột | Định nghĩa |
| --- | --- |
| `arxiv_id`, `range_owner` | ID và người giữ dải (`config.RANGES`) |
| `n_versions` | `num_versions` (0 nếu discovery không tìm thấy bài) |
| `n_versions_ok` | Số version có trạng thái cuối `ok`, `no_tex` hoặc `unavailable` **và** có thư mục `vN/` |
| `has_source` | 1 nếu có ít nhất một version `ok` |
| `size_before_bytes`, `size_after_bytes` | Tổng `bytes_before` / `bytes_after` qua mọi version |
| `n_refs_total`, `n_refs_arxiv` | `num_references`, `num_arxiv_references` (0 nếu chưa có `references.json`) |
| `n_refs_meta_ok` | Số reference có `s2_id` khác `null`, tức S2 resolve được metadata |
| `ref_status` | `ok`, `not_found` hoặc `missing` (chưa crawl được) |
| `process_sec` | Tổng `total_s` của các version |

- **Bài crawl thành công:** `n_versions > 0` và `n_versions_ok == n_versions` (trùng `papers_complete` trong `run_summary.json`).
- **Success rate reference metadata:** tỉ lệ bài có `ref_status == ok`. Notebook còn báo thêm tỉ lệ `n_refs_meta_ok / n_refs_total`.

**`references_stats.csv`** — mỗi dòng một nguồn (`arxiv`, `semantic_scholar`): `source, n_requests, n_hit, n_miss, n_http_429, n_http_5xx`. Số request và mã HTTP được đếm từ `work/run.log`; `n_hit`/`n_miss` là số bài tìm thấy / không tìm thấy (`discovery.jsonl` cho arXiv, `references.json` cho S2).

**`benchmark_summary.csv`** — `metric, value, unit, tool` cho `wall_time_e2e`, `discovery_time`, `avg_paper_time`, `ram_max`, `ram_mean`, `disk_max`, `output_size`; lấy từ `profile_summary.json`, `run_summary.json` và (nếu có) output của `/usr/bin/time -v`.

**`resource_timeseries.csv`** — `t_sec, rss_total_mb, disk_mb`, lấy từ `profile_samples.csv`.
