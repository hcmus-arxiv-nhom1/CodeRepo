<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Spec 0001 — Reference metadata from Semantic Scholar

- **Owner:** Đăng (Member 4) · **Drafted by:** Thiên, 27/10/2026, to keep the official crawl on schedule · **Reviewers:** Thiên, Tiến · **Status:** awaiting Đăng's approval
- **Branch:** `feat/references` · Code: `src/crawler/references.py`, tests: `src/tests/test_references.py`

## What the statement asks

- *"The reference metadata must additionally be crawled for each publication via the Semantic Scholar API."*
- For each reference: *"its title, authors, submission date, arXiv ID when one exists, and Semantic Scholar ID"*; *"Keep all crawled references locally"*. Only references with `externalIds.ArXiv` enter the `references` table.
- Fields are requested flat: `references.title, references.authors, references.year, references.publicationDate, references.venue, references.externalIds` (nested forms such as `authors.name` are rejected).
- Limits: without a key 1 request/s and 100 requests per 5 minutes; with a key a steady 1 request/s.

## Spike (27/10/2026, no API key)

| Endpoint | Result |
| --- | --- |
| `GET /paper/arXiv:{id}` | 429 on the very first request (shared pool overloaded) |
| `POST /paper/batch` (5 IDs) | 200. Each element is one paper with its complete `references`: 218, 76, 0, 41 references; an unknown ID returns `null` |
| `GET /paper/{id}/references` | 200, paginated with `offset`/`next`, each element is `{"citedPaper": {...}}` |
| `references.py` on the 800 test papers | 800 papers with **21 requests** (one batch was retried in a later run after five 429s). 35.6 references per paper, 49.9 % with an arXiv ID, 13.5 % unresolved (`paperId = null`), 9.5 % without a date |

## Design

1. **100 IDs per request** through `POST /paper/batch` (the API accepts up to 500). 20,000 papers ≈ 200 requests, instead of 20,000 requests (≈ 6.7 h with a key, ≈ 17 h without) in the earlier plan.
2. **Papers whose nested list reaches `NESTED_REFERENCE_CAP` (1,000)** are completed through the paginated `/paper/{id}/references`. The value 1,000 is an **assumption** to verify with an API key (Đăng). The longest list among the 800 test papers has 202 references.
3. **One shared S2 limiter** (`semantic_scholar_limiter()`), separate from the arXiv limiter, so it runs in parallel in the thread `run_pipeline` already starts.
4. **Retries** on 429/5xx/connection errors, at most 5 attempts, `2^n` s backoff plus jitter, honouring `Retry-After`. A batch that still fails writes **no** file; the next run retries it. Papers whose `references.json` has `status` `ok` / `not_found` are skipped.
5. **Atomic writes** (temporary file, then rename). Schema: `docs/data_contract.md`, section 4.
6. Each request logs one line `S2 <METHOD> <url> -> <status>`, which `stats.py` counts.

## Definitions used in the report

- **Reference metadata success rate** = papers with `status == ok` / papers crawled, plus the share of references S2 resolved (`s2_id` not null).
- **Mean references per paper** = mean `num_references` over papers with `status == ok`.

## To do when approving (Đăng)

- [ ] Register and set `S2_API_KEY` (Colab Secrets). Without a key the shared pool often answers 429.
- [ ] With the key, check the cap of nested lists in batch answers (a paper with more than 1,000 references) and adjust `NESTED_REFERENCE_CAP` if needed.
- [ ] Approve the success-rate definition above.

## Verified sources

1. HCMUS CS TA. *Project Milestone 1 — Data Crawling*. https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/crawling (read 25/10/2026).
2. Allen Institute for AI. *Semantic Scholar Academic Graph API*. https://api.semanticscholar.org/api-docs/ — endpoints `paper/batch` and `paper/{paper_id}/references`.
3. The spike and the 800-paper run above (27/10/2026).
