# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import pytest
import requests

from src.crawler.http_client import BlockedError, FetchError
from src.tests.helpers import FakeClock, FakeResponse, make_client


def scripted(responses, clock, starts):
    items = list(responses)

    def handler(url):
        starts.append(clock.t)
        item = items.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    return handler


def test_fetch_result_reports_rate_limiter_wait_separately(monkeypatch):
    clock, starts = FakeClock(), []
    monkeypatch.setattr("src.crawler.http_client.time.perf_counter", clock)
    client = make_client(
        scripted([FakeResponse(200, b"first"), FakeResponse(200, b"second")], clock, starts),
        clock,
    )
    client.get("https://export.arxiv.org/first")
    result = client.get("https://export.arxiv.org/second")

    assert result.wait_s == 3.0
    assert result.elapsed_s < 0.1


def test_fetch_timing_includes_failed_attempt_work_without_double_counting_wait(monkeypatch):
    clock, starts = FakeClock(), []
    monkeypatch.setattr("src.crawler.http_client.time.perf_counter", clock)

    def reply(url):
        starts.append(clock())
        clock.sleep(1)  # one second transferring each response
        return FakeResponse(503 if len(starts) == 1 else 200, b"body")

    result = make_client(reply, clock).get("https://export.arxiv.org/test")
    assert result.attempts == 2
    assert result.elapsed_s == pytest.approx(2)
    assert result.elapsed_s + result.wait_s == pytest.approx(clock())


def test_retry_after_is_honoured():
    clock, starts = FakeClock(), []
    client = make_client(
        scripted(
            [FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, b"ok")],
            clock,
            starts,
        ),
        clock,
    )
    res = client.get("https://export.arxiv.org/api/query?id_list=x")
    assert res.status == 200 and res.content == b"ok" and res.attempts == 2
    assert starts == [0.0, 7.0]


def test_5xx_uses_exponential_backoff_on_top_of_pacing():
    clock, starts = FakeClock(), []
    client = make_client(
        scripted([FakeResponse(503), FakeResponse(503), FakeResponse(200, b"ok")], clock, starts),
        clock,
    )
    assert client.get("https://export.arxiv.org/x").status == 200
    # 2**1 = 2 s is below the 3 s pacing; 2**2 = 4 s after the second call wins.
    assert starts == [0.0, 3.0, 7.0]


def test_timeout_then_success():
    clock, starts = FakeClock(), []
    client = make_client(
        scripted([requests.ConnectTimeout("slow"), FakeResponse(200, b"ok")], clock, starts), clock
    )
    assert client.get("https://export.arxiv.org/x").attempts == 2


def test_404_is_returned_without_retry():
    clock, starts = FakeClock(), []
    client = make_client(
        scripted([FakeResponse(404, b"<html>not found</html>")], clock, starts), clock
    )
    res = client.get("https://export.arxiv.org/src/2606.00001v9")
    assert res.status == 404 and res.attempts == 1 and b"not found" in res.content


def test_gives_up_after_max_attempts():
    clock, starts = FakeClock(), []
    client = make_client(scripted([FakeResponse(503)] * 5, clock, starts), clock)
    with pytest.raises(FetchError) as err:
        client.get("https://export.arxiv.org/x")
    assert err.value.attempts == 5 and err.value.status == 503


def test_circuit_breaker_after_three_403s():
    clock, starts = FakeClock(), []
    responses = [FakeResponse(403)] * 3 + [FakeResponse(200, b"ok")]
    client = make_client(scripted(responses, clock, starts), clock, breaker_pause_s=(900.0, 900.0))
    assert client.get("https://export.arxiv.org/x").status == 200
    assert client.breaker_trips == 1
    assert client.limiter.min_interval_s == 5.0
    assert starts[3] - starts[2] >= 900.0


def test_blocked_error_when_403_persists():
    clock, starts = FakeClock(), []
    client = make_client(
        scripted([FakeResponse(403)] * 10, clock, starts),
        clock,
        breaker_pause_s=(1.0, 1.0),
        max_breaker_trips=1,
    )
    # First URL: trip #1 at the 3rd 403, then gives up after 5 attempts.
    with pytest.raises(FetchError) as err:
        client.get("https://export.arxiv.org/x")
    assert not isinstance(err.value, BlockedError)
    # Next URL: the 6th consecutive 403 trips the breaker again -> stop the run.
    with pytest.raises(BlockedError):
        client.get("https://export.arxiv.org/y")


def read_all(body, headers):
    return body.read()


def test_body_ended_early_is_resumed_with_range(tmp_path):
    clock, starts = FakeClock(), []
    body = b"0123456789"
    responses = [
        FakeResponse(200, body[:5], headers={"Content-Length": "10"}),  # connection cut short
        FakeResponse(206, body[5:], headers={"Content-Range": "bytes 5-9/10"}),
    ]
    client = make_client(scripted(responses, clock, starts), clock)
    res = client.stream("https://export.arxiv.org/src/x", read_all)
    assert res.value == body and res.attempts == 1 and res.resumes == 1 and res.nbytes == 10
    assert client.session.sent_headers[1]["Range"] == "bytes=5-"


def test_cut_body_resumes_with_a_range_request_and_no_backoff():
    """arXiv /src/ answers Range with 206: only the missing bytes are fetched again."""
    clock, starts = FakeClock(), []
    body = b"0123456789"
    cut = requests.exceptions.ChunkedEncodingError("Connection broken: IncompleteRead")
    responses = [
        FakeResponse(200, body, headers={"Content-Length": "10"}, fail_midway=cut),
        FakeResponse(206, body[5:], headers={"Content-Range": "bytes 5-9/10"}),
    ]
    client = make_client(scripted(responses, clock, starts), clock)
    res = client.stream("https://export.arxiv.org/src/z", read_all)
    assert res.status == 200 and res.value == body and res.resumes == 1
    assert starts == [0.0, 3.0]  # only the 3 s pace, no exponential backoff


def test_resume_survives_several_cuts():
    clock, starts = FakeClock(), []
    cut = requests.exceptions.ChunkedEncodingError("cut")
    responses = [
        FakeResponse(200, b"abcdefgh", headers={"Content-Length": "8"}, fail_midway=cut),
        FakeResponse(206, b"efgh", headers={"Content-Range": "bytes 4-7/8"}, fail_midway=cut),
        FakeResponse(206, b"gh", headers={"Content-Range": "bytes 6-7/8"}),
    ]
    client = make_client(scripted(responses, clock, starts), clock)
    res = client.stream("https://export.arxiv.org/src/w", read_all)
    assert res.value == b"abcdefgh" and res.resumes == 2
    assert [h.get("Range") for h in client.session.sent_headers] == [None, "bytes=4-", "bytes=6-"]


def test_server_ignoring_range_restarts_from_zero():
    clock, starts = FakeClock(), []
    cut = requests.exceptions.ChunkedEncodingError("cut")
    responses = [
        FakeResponse(200, b"abcdef", headers={"Content-Length": "6"}, fail_midway=cut),
        FakeResponse(200, b"abcdef", headers={"Content-Length": "6"}),  # Range ignored
        FakeResponse(200, b"abcdef", headers={"Content-Length": "6"}),
    ]
    client = make_client(scripted(responses, clock, starts), clock)
    res = client.stream("https://export.arxiv.org/src/v", read_all)
    assert res.value == b"abcdef" and res.attempts == 2  # not "abcabcdef"
    assert "Range" not in client.session.sent_headers[2]


def test_wrong_content_range_restarts_from_zero():
    clock, starts = FakeClock(), []
    cut = requests.exceptions.ChunkedEncodingError("cut")
    responses = [
        FakeResponse(200, b"abcdef", headers={"Content-Length": "6"}, fail_midway=cut),
        FakeResponse(206, b"zz", headers={"Content-Range": "bytes 0-1/6"}),  # wrong start
        FakeResponse(200, b"abcdef", headers={"Content-Length": "6"}),
    ]
    client = make_client(scripted(responses, clock, starts), clock)
    assert client.stream("https://export.arxiv.org/src/u", read_all).value == b"abcdef"


def test_resume_rejected_with_429_waits_and_retries_the_same_range():
    """A 429 on the Range request must not throw away the bytes already received."""
    clock, starts = FakeClock(), []
    cut = requests.exceptions.ChunkedEncodingError("cut")
    responses = [
        FakeResponse(200, b"abcdefgh", headers={"Content-Length": "8"}, fail_midway=cut),
        FakeResponse(429, headers={"Retry-After": "10"}),
        FakeResponse(206, b"efgh", headers={"Content-Range": "bytes 4-7/8"}),
    ]
    client = make_client(scripted(responses, clock, starts), clock)
    res = client.stream("https://export.arxiv.org/src/r", read_all)
    assert res.value == b"abcdefgh" and res.attempts == 1  # no restart from byte 0
    assert [h.get("Range") for h in client.session.sent_headers] == [None, "bytes=4-", "bytes=4-"]
    assert starts == [0.0, 3.0, 13.0]  # Retry-After honoured after the rejected resume


def test_resume_rejected_with_503_backs_off_and_retries():
    clock, starts = FakeClock(), []
    cut = requests.exceptions.ChunkedEncodingError("cut")
    responses = [
        FakeResponse(200, b"abcdefgh", headers={"Content-Length": "8"}, fail_midway=cut),
        FakeResponse(503),
        FakeResponse(206, b"efgh", headers={"Content-Range": "bytes 4-7/8"}),
    ]
    client = make_client(scripted(responses, clock, starts), clock)
    res = client.stream("https://export.arxiv.org/src/s", read_all)
    assert res.value == b"abcdefgh" and res.attempts == 1


def test_resume_gives_up_after_repeated_rejections_and_restarts():
    clock, starts = FakeClock(), []
    cut = requests.exceptions.ChunkedEncodingError("cut")
    responses = [FakeResponse(200, b"abcdefgh", headers={"Content-Length": "8"}, fail_midway=cut)]
    responses += [FakeResponse(429, headers={"Retry-After": "1"}) for _ in range(3)]
    responses += [FakeResponse(200, b"abcdefgh", headers={"Content-Length": "8"})]
    client = make_client(scripted(responses, clock, starts), clock)
    res = client.stream("https://export.arxiv.org/src/q", read_all)
    assert res.value == b"abcdefgh" and res.attempts == 2


def test_consumer_errors_propagate():
    clock, starts = FakeClock(), []
    client = make_client(scripted([FakeResponse(200, b"x")], clock, starts), clock)

    def broken(body, headers):
        raise ValueError("corrupt archive")

    with pytest.raises(ValueError):
        client.stream("https://export.arxiv.org/src/t", broken)


def test_no_backoff_after_the_last_attempt():
    """A fetch that gives up does not leave a pause behind: the retries already waited."""
    clock, starts = FakeClock(), []
    client = make_client(scripted([FakeResponse(429)] * 2, clock, starts), clock)
    with pytest.raises(FetchError) as err:
        client.get("https://export.arxiv.org/x", max_attempts=2)
    assert err.value.attempts == 2 and starts == [0.0, 3.0]  # one backoff (2 s), pace wins
    next_start = client.limiter._next_start  # noqa: SLF001
    assert next_start == 3.0 + 3.0  # the 3 s pace only, no 4 s backoff after attempt 2


def test_arxiv_retry_budget_outlasts_a_minutes_long_429_window():
    """30/10 and 01/11: 5 attempts spanned 33 s while arXiv kept answering 429 for longer."""
    from src import config

    clock, starts = FakeClock(), []
    responses = [FakeResponse(429)] * 7 + [FakeResponse(200, b"ok")]
    client = make_client(
        scripted(responses, clock, starts),
        clock,
        max_attempts=config.ARXIV_MAX_ATTEMPTS,
        max_backoff_s=config.ARXIV_MAX_BACKOFF_S,
    )
    res = client.get("https://export.arxiv.org/src/2604.20926v3")
    assert res.status == 200 and res.attempts == 8
    assert starts[-1] == 3 + 4 + 8 + 16 + 32 + 64 + 120  # 2**n s backoff, capped at 120 s


def _ok_client(statuses, clock, **kw):
    responses = [FakeResponse(s, b"ok") for s in statuses]
    return make_client(scripted(responses, clock, []), clock, **kw)


def test_adaptive_pacing_slows_after_429_and_wins_the_pace_back():
    clock = FakeClock()
    client = _ok_client([429] + [200] * 40, clock, adaptive_pacing=True, pacing_streak=5)
    url = "https://export.arxiv.org/x"
    client.get(url)
    assert client.limiter.min_interval_s == 4.5  # 3.0 x 1.5 after the 429
    for _ in range(4):  # the first success after the 429 counts too: 5 in a row
        client.get(url)
    assert client.limiter.min_interval_s == 4.0
    for _ in range(10):
        client.get(url)
    assert client.limiter.min_interval_s == 3.0  # never below the starting pace
    for _ in range(10):
        client.get(url)
    assert client.limiter.min_interval_s == 3.0


def test_adaptive_pacing_is_capped_and_off_by_default():
    clock = FakeClock()
    capped = _ok_client([429] * 3 + [200], clock, adaptive_pacing=True, pacing_max_s=5.0)
    capped.get("https://export.arxiv.org/x")
    assert capped.limiter.min_interval_s == 5.0  # 3 -> 4.5 -> 5.0, then no further growth
    plain = _ok_client([429, 200], FakeClock())
    plain.get("https://export.arxiv.org/x")
    assert plain.limiter.min_interval_s == 3.0


def test_adaptive_pacing_never_goes_below_the_breaker_pace():
    clock = FakeClock()
    client = _ok_client(
        [403] * 3 + [200] * 12,
        clock,
        adaptive_pacing=True,
        pacing_streak=3,
        breaker_pause_s=(1.0, 1.0),
    )
    client.get("https://export.arxiv.org/x")  # three 403s trip the breaker, then one success
    assert client.breaker_trips == 1 and client.limiter.min_interval_s == 5.0
    for _ in range(11):
        client.get("https://export.arxiv.org/x")
    assert client.limiter.min_interval_s == 5.0  # the breaker pace is the floor now
