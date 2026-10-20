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


def test_stream_to_file_retries_truncated_body(tmp_path):
    clock, starts = FakeClock(), []
    body = b"0123456789"
    responses = [
        FakeResponse(200, body[:5], headers={"Content-Length": "10"}),  # connection cut short
        FakeResponse(200, body, headers={"Content-Length": "10"}),
    ]
    client = make_client(scripted(responses, clock, starts), clock)
    dest = tmp_path / "x.src"
    res = client.get("https://export.arxiv.org/src/x", dest=dest)
    assert res.attempts == 2 and dest.read_bytes() == body
    assert not (tmp_path / "x.src.part").exists()


def test_connection_drop_mid_body_is_retried(tmp_path):
    clock, starts = FakeClock(), []
    responses = [
        FakeResponse(200, b"abcdef", fail_midway=requests.exceptions.ChunkedEncodingError("cut")),
        FakeResponse(200, b"abcdef"),
    ]
    client = make_client(scripted(responses, clock, starts), clock)
    dest = tmp_path / "y.src"
    assert client.get("https://export.arxiv.org/src/y", dest=dest).attempts == 2
    assert dest.read_bytes() == b"abcdef"
