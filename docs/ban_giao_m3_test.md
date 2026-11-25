# Bàn giao đánh giá Test M3

**Thiên chạy lượt Test chính thức và xuất `predictions`. Đăng xác nhận cấu hình đã khóa, đọc chéo kết quả và phụ trách phân tích lỗi.** Đây là đề xuất chia vai cụ thể trong phần việc chung của Đăng và Thiên. Dùng một lượt xuất `predictions` để chấm Test từ các dòng đã ghi.

## Đầu vào

- Code và mô hình từ [PR #59](https://github.com/hcmus-arxiv-nhom1/CodeRepo/pull/59), commit `d8898a8f62a5eef315afd8d609320b4e257796e9`. Sau khi PR merge, checkout revision trên `main` chứa mô hình này và ghi lại SHA thực tế.
- Artifact `src/artifacts/m3-ranker/model.json` và ranker `src.m3.model:rank_paper`. Giữ nguyên hệ số, feature và ngưỡng no-match đã chọn trên Valid.
- `M3/partitions.csv` trên Drive, SHA-256 `0d5163b18856060f55af383941c66334e7d2aae0d2d8be6d67ff6ef83fe11058`. Test gồm 4 bài manual và 4 bài auto, tương ứng 183 và 163 mục BibTeX. Giữ đủ ba tập trong CSV để xuất bảng dự đoán chính thức.
- `M3/labels/`, M1 `M3/m1_tables_all/data/references/` và M2 `m2/derived/m2-full-no-empty-01/out/`. Chỉ cần các shard `bibtex.parquet` của M2.

## Thiên chạy và lưu đầu ra

Mở [notebook pipeline của Thiên](../src/notebooks/arxiv_nhom1_m3_thien_pipeline.ipynb). Dùng phần cấu hình và chuẩn bị đầu vào ở mục 1 để có `M3`, `LABELS` và `M2_OUT`. Bỏ qua các bước tạo lại nhãn hoặc chia tập. Nếu dùng bản restore BibTeX sẵn, xác nhận nó thuộc `m2-full-no-empty-01`.

Thay cell xuất ở mục 5 bằng đoạn dưới. Nếu đã có thư mục kết quả, dùng một đường dẫn mới và báo lại cho người đóng gói.

```python
import hashlib
import os
import subprocess
import sys
from pathlib import Path

parts = M3 / "partitions.csv"
artifact = Path("src/artifacts/m3-ranker/model.json").resolve()
refs = M3 / "m1_tables_all" / "data" / "references"
predictions = M3 / "predictions_by_range"

assert hashlib.sha256(parts.read_bytes()).hexdigest() == (
    "0d5163b18856060f55af383941c66334e7d2aae0d2d8be6d67ff6ef83fe11058"
)
assert artifact.is_file() and any(refs.glob("*.parquet"))
assert any(M2_OUT.glob("shard-*/bibtex.parquet"))
assert all((LABELS / method).is_dir() for method in ("manual", "auto"))
assert not predictions.exists(), f"Choose a fresh output path: {predictions}"

print("Revision", subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip())
print("Model SHA-256", hashlib.sha256(artifact.read_bytes()).hexdigest())
subprocess.run([
    sys.executable, "-m", "src.m3.predictions",
    "--m2-dir", str(M2_OUT), "--refs-dir", str(refs),
    "--labels-dir", str(LABELS), "--partitions", str(parts),
    "--out-dir", str(predictions), "--split-by-range",
    "--ranker", "src.m3.model:rank_paper",
], env={**os.environ, "M3_MODEL_ARTIFACT": str(artifact)}, check=True)
```

Kiểm tra `predictions_summary.json` ghi 2.020 bài được yêu cầu, 2.020 bài được ghi và 0 bài thiếu trong M2. Kiểm tra đủ bốn thư mục `<dải>/data/predictions/`, cột `partition` kiểu dictionary và chỉ có `train`, `valid`, `test`.

## Thiên và Đăng chấm từ Parquet đã xuất

Đọc MRR@5 ở `metrics.by_partition_and_method.test_manual` và `test_auto` trong `predictions_summary.json`. Ghi cả hai mẫu số, toàn bộ mục đã gán nhãn và chỉ các mục có match. Đối chiếu số hàng của hai nhóm với 183 và 163. Nếu có hàng ngoài ma trận nhãn, tính lại từ các hàng thực sự đã gán.

Dùng `(arxiv_id, citation_key)` để nối các hàng Test với `labels/manual/` hoặc `labels/auto/`. Một hàng nhãn toàn `no` là no-match thật. Dự đoán no-match khi `cand_1` là null. Lưu số TP, FP, FN và precision, recall, F1 cho từng nhóm manual/auto. Nếu precision hoặc recall không có mẫu số, ghi `N/A` kèm số đếm.

Đăng rà các trường hợp từ chối nhầm, nhận nhầm hoặc xếp sai thứ hạng. Ghi `arxiv_id`, `citation_key`, ID đúng, danh sách ứng viên dự đoán và nguyên nhân nếu xác định được. Ghi riêng lỗi metadata hoặc lỗi phát sinh từ bộ ứng viên. Giữ nguyên artifact trong quá trình phân tích Test.

## Bàn giao đầu ra

- **Thiên** gửi đường dẫn Drive của `predictions_by_range/`, báo cáo tóm tắt, revision và SHA-256 của mô hình cho Đăng và Hiếu.
- **Đăng** bàn giao số Test manual/auto, chỉ số no-match và ví dụ lỗi để cập nhật README tiếng Anh và poster.
- **Hiếu** dùng đúng đường dẫn `predictions` đã xác nhận khi đóng gói sáu bảng.
