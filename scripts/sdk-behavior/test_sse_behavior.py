"""G04 behavior tests for the generated Python SDK (live HTTP + chunk matrix).

Covers checklist G04-01..G04-04 against the real `clients/python/qcg_client.py`
`stream_run_events` path (no parser replica: the client itself is exercised).
"""
from __future__ import annotations

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

CLIENT = Path(__file__).resolve().parents[2] / "clients" / "python"
sys.path.insert(0, str(CLIENT))
from qcg_client import QcgClient, QcgError, RunSnapshot, QueuePositionQuality  # noqa: E402


class FakeRaw:
    """Minimal HTTPResponse double driving the client's read1/read/close path."""

    def __init__(self, chunks: list[bytes]):
        self._chunks = list(chunks)
        self.close_calls = 0
        self.read1_calls = 0

    def read1(self, size: int):
        self.read1_calls += 1
        if not self._chunks:
            return b""
        head = self._chunks.pop(0)
        assert len(head) <= size, "test chunk exceeds the client read size"
        return head

    def read(self, size: int):  # fallback path when read1 is absent
        return self.read1(size)

    def close(self):
        self.close_calls += 1


def collect_with_fake(payload_bytes: bytes, chunk_sizes: list[int]) -> list:
    """Run one event stream through FakeRaw with the given chunking."""
    chunks: list[bytes] = []
    step = 0
    pos = 0
    while pos < len(payload_bytes):
        size = chunk_sizes[step % len(chunk_sizes)]
        chunks.append(payload_bytes[pos : pos + size])
        pos += size
        step += 1
    raw = FakeRaw(chunks)
    client = QcgClient(base_url="http://127.0.0.1:9")
    client._open = lambda request: raw  # type: ignore[method-assign]
    try:
        return list(client.stream_run_events("r1"))
    finally:
        assert raw.close_calls >= 1, "the stream must release the connection"


def sse_bytes(text: str, ending: str) -> bytes:
    body = text.replace("\n", ending)
    return body.encode("utf-8")


def test_g04_02_split_positions_match_unsplit() -> None:
    payloads = [
        ("ascii", {"q": "hello"}),
        ("japanese", {"q": "質問です"}),
        ("emoji", {"q": "🎉🚀"}),
    ]
    endings = [("\n", "\n"), ("crlf", "\r\n"), ("cr", "\r")]
    for pname, obj in payloads:
        for ename, ending in endings:
            wire = sse_bytes("data: " + json.dumps(obj, ensure_ascii=False) + ending + ending, ending)
            expected = collect_with_fake(wire, [1 << 16])
            assert expected == [obj], f"unsplit baseline failed for {pname}/{ename}"
            for split in range(1, len(wire)):
                got = collect_with_fake(wire, [split, len(wire)])
                assert got == expected, (
                    f"split at {split}/{len(wire)} changed the event stream for {pname}/{ename}: "
                    f"{got!r} != {expected!r}"
                )
    print("G04-02 split-position matrix passed")


def test_g04_03_multi_data_and_eof() -> None:
    # Multiple data: lines join with LF.
    wire = b"data: {\"a\":\ndata: 1}\n\n"
    got = collect_with_fake(wire, [3, 5])
    assert got == [{"a": 1}], f"multi-data lines must concatenate, got {got!r}"
    # Comment-only and empty frames yield nothing.
    wire = b": ping\n\n" + b"data: {\"b\": 2}\n\n"
    got = collect_with_fake(wire, [2, 4])
    assert got == [{"b": 2}], f"comment frames must be skipped, got {got!r}"
    # EOF with an unterminated tail fabricates no event (SSE spec).
    for tail in (b"data: {\"c\": 3}\n", b"data: {\"c\": 3}", b"data: {\"c\": 3}\r"):
        got = collect_with_fake(tail, [2])
        assert got == [], f"EOF tail must be discarded, got {got!r} for {tail!r}"
    print("G04-03 multi-data/EOF passed")


def test_g04_04_release_on_break_and_error() -> None:
    wire = b"data: {\"a\": 1}\n\ndata: {\"b\": 2}\n\n"
    raw = FakeRaw([wire[:10], wire[10:]])
    client = QcgClient(base_url="http://127.0.0.1:9")
    client._open = lambda request: raw  # type: ignore[method-assign]
    stream = client.stream_run_events("r1")
    first = next(stream)
    assert first == {"a": 1}
    stream.close()  # generator close -> finally releases the connection
    assert raw.close_calls >= 1, "breaking the consumer must release the connection"
    # A parse error still releases the connection.
    raw2 = FakeRaw([b"data: not-json\n\n"])
    client._open = lambda request: raw2  # type: ignore[method-assign]
    try:
        list(client.stream_run_events("r1"))
    except json.JSONDecodeError:
        pass
    else:
        raise AssertionError("malformed JSON must raise")
    assert raw2.close_calls >= 1, "a parse error must release the connection"
    print("G04-04 release passed")


class FlushHandler(BaseHTTPRequestHandler):
    """Real loopback SSE endpoint proving immediacy with timestamps.

    The handler flushes the first tens-of-bytes event, records the instant,
    then waits (up to 3 s) for the client to prove receipt before sending
    the second event. A client that blocks until the 64 KiB read fills or
    EOF arrives can only receive the first event after the handler has
    already sent the second one, so the receipt-vs-second-send ordering
    discriminates immediate delivery from EOF-gated delivery.
    """

    protocol_version = "HTTP/1.1"
    got_first: threading.Event | None = None
    marks: dict | None = None

    def do_GET(self):  # noqa: N802
        import time

        assert FlushHandler.got_first is not None and FlushHandler.marks is not None
        first = json.dumps({"q": "first"}).encode()
        second = json.dumps({"q": "second"}).encode()
        total = len(b"data: " + first + b"\n\n") + len(b"data: " + second + b"\n\n")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(total))
        self.end_headers()
        self.wfile.write(b"data: " + first + b"\n\n")
        self.wfile.flush()
        FlushHandler.marks["first_sent"] = time.perf_counter()
        # Wait for the client to prove it received the first event while
        # the stream is still open (no more bytes flow meanwhile). Record
        # whether the wait ended by signal (immediate delivery) or by
        # timeout (EOF-gated delivery): the flag discriminates even on
        # coarse clocks where all three instants share one tick.
        signaled = FlushHandler.got_first.wait(timeout=3.0)
        FlushHandler.marks["wait_signaled"] = signaled
        self.wfile.write(b"data: " + second + b"\n\n")
        self.wfile.flush()
        FlushHandler.marks["second_sent"] = time.perf_counter()

    def log_message(self, *args):
        pass


def test_g04_01_immediate_delivery_over_real_http() -> None:
    import time

    got_first = threading.Event()
    marks: dict = {}
    FlushHandler.got_first = got_first
    FlushHandler.marks = marks
    server = HTTPServer(("127.0.0.1", 0), FlushHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        client = QcgClient(base_url=f"http://127.0.0.1:{port}")
        stream = client.stream_run_events("r1")
        received: list = []

        def pump():
            try:
                for event in stream:
                    if not received:
                        marks["first_received"] = time.perf_counter()
                        got_first.set()
                    received.append(event)
            finally:
                server.shutdown()

        worker = threading.Thread(target=pump, daemon=True)
        worker.start()
        worker.join(timeout=10)
        assert received == [{"q": "first"}, {"q": "second"}], f"unexpected stream: {received!r}"
        assert marks.get("wait_signaled") is True, (
            "the server must observe receipt of the flushed first event while "
            f"the stream is still open (EOF-gated delivery times out): {marks!r}"
        )
        assert "first_received" in marks and "second_sent" in marks, f"missing marks: {marks!r}"
        assert marks["first_received"] <= marks["second_sent"], (
            "the flushed first event must be delivered while the stream is still open, "
            f"before the second is sent: {marks!r}"
        )
    finally:
        server.server_close()
    print("G04-01 immediate delivery passed")


class FakeHttpResponse:
    """Double for the urllib response used by _request_bytes_raw."""

    def __init__(self, status: int, payload: bytes, headers: dict | None = None):
        self.status = status
        self._payload = payload
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_typed_readers() -> None:
    """G07-04: JSON/text/bytes/NDJSON/empty/304 readers over mocked transport."""
    from typing import get_args, get_type_hints
    assert get_type_hints(RunSnapshot)["queue_position_quality"] == QueuePositionQuality
    assert set(get_args(QueuePositionQuality)) == {"exact", "estimated", "unavailable"}
    assert "running" in get_args(get_type_hints(RunSnapshot)["state"])
    assert {"run_id", "generator_id", "state"} <= RunSnapshot.__required_keys__
    client = QcgClient(base_url="http://127.0.0.1:9")

    def serve(status: int, payload: bytes):
        client._open = lambda request: FakeHttpResponse(status, payload)  # type: ignore[method-assign]

    serve(200, json.dumps({"ok": True}).encode())
    assert client.health() == {"ok": True}
    serve(200, "up\n".encode())
    assert client.metrics() == "up\n"
    serve(200, bytes([1, 2, 3]))
    assert client.download_run_bundle("r1") == bytes([1, 2, 3])
    serve(200, b'{"a":1}\n{"b":2}\n')
    assert client.read_run_journal("r1") == [{"a": 1}, {"b": 2}]
    serve(204, b"")
    assert client.delete_run("gone") is None
    serve(304, b"")
    assert client.health() is None
    print("typed readers passed")


def test_frame_limit() -> None:
    try:
        collect_with_fake(b"data:" + b"x" * (16 * 1024 * 1024 + 1), [65536])
    except QcgError as error:
        assert "SSE frame exceeds" in str(error)
    else:
        raise AssertionError("unterminated oversized SSE frame was accepted")


if __name__ == "__main__":
    test_g04_02_split_positions_match_unsplit()
    test_g04_03_multi_data_and_eof()
    test_g04_04_release_on_break_and_error()
    test_g04_01_immediate_delivery_over_real_http()
    test_typed_readers()
    test_frame_limit()
    print("Python SSE behavior: all G04 checks passed")

for fixture in json.loads((Path(__file__).resolve().parents[1] / "fixtures/sse.json").read_text(encoding="utf-8")):
    wire = bytes.fromhex(fixture["wire_hex"]) if "wire_hex" in fixture else fixture["wire"].encode("utf-8")
    for size in range(1, len(wire) + 1):
        assert collect_with_fake(wire, [size]) == fixture["payloads"], fixture["name"]
