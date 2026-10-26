# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Shared constants. Secrets (Semantic Scholar key, Kaggle token) come from env / Colab Secrets."""

from __future__ import annotations

import os

# arXiv: only the export mirror and the new OAI-PMH endpoint, never arxiv.org.
ARXIV_EXPORT = "https://export.arxiv.org"
ARXIV_SRC_URL = ARXIV_EXPORT + "/src/{arxiv_id}v{version}"
ARXIV_API_URL = ARXIV_EXPORT + "/api/query"
ARXIV_OAI_URL = "https://oaipmh.arxiv.org/oai"
ARXIV_API_BATCH = 100  # IDs per id_list request
ARXIV_MIN_INTERVAL_S = 3.0  # Terms of Use: >= 3 s between calls, one connection.
ARXIV_JITTER_S = 0.1  # spec 0002: mean gap 3.05 s instead of 3.25 s (about 6% faster)

SEMANTIC_SCHOLAR_API = "https://api.semanticscholar.org/graph/v1"
SEMANTIC_SCHOLAR_API_KEY = os.environ.get("S2_API_KEY") or None

# Descriptive User-Agent; the contact email is only added when explicitly configured.
CONTACT_EMAIL = os.environ.get("ARXIV_CONTACT_EMAIL", "").strip()
USER_AGENT = "HCMUS-IntroDS-Group01-crawler/0.1 (university course project{})".format(
    f"; mailto:{CONTACT_EMAIL}" if CONTACT_EMAIL else ""
)

# Statement M1: "extract only the .tex and .bib files from each source archive".
# The team plan proposes also keeping .bbl: override with --keep-ext once the team decides.
KEEP_EXTENSIONS = frozenset({".tex", ".bib"})

DEFAULT_DATA_ROOT = os.environ.get("DATA_ROOT", "data/corpus")
DEFAULT_WORK_DIR = os.environ.get("WORK_DIR", "work")

# Assigned ID ranges (inclusive). Thiên's range crosses the June/July 2026 border.
RANGES = {
    "thien": ("2606.30734", "2607.03693"),
    "tien": ("2606.25734", "2606.30733"),
    "hieu": ("2606.05734", "2606.10733"),
    "dang": ("2604.20535", "2604.25534"),
}

# Last sequence number of each month crossed by a range, confirmed with the arXiv API
# (python -m src.spike month-end 2606) on 2026-10-18: 2606.32040 exists, 2606.32041.. do not.
MONTH_END = {"2606": 32040}
