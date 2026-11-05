"""Log names and log line times are UTC, whatever the process time zone is."""

from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

from spark_quick_start.spark_logging import _ListBufferHandler, _now_strings


@pytest.fixture
def kyiv_process_zone() -> Iterator[None]:
    """Run the test under a process time zone that is never UTC (UTC+2/+3)."""
    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset is unavailable on this platform")
    before = os.environ.get("TZ")
    os.environ["TZ"] = "Europe/Kyiv"
    time.tzset()
    try:
        yield
    finally:
        if before is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = before
        time.tzset()


def test_base_name_stamp_is_utc(kyiv_process_zone: None) -> None:
    now, stamp = _now_strings()
    assert now.utcoffset() == UTC.utcoffset(None)
    expected = datetime.now(UTC)
    parsed = datetime.strptime(stamp, "%Y%m%d_T_%H%M%S").replace(tzinfo=UTC)
    assert abs((expected - parsed).total_seconds()) < 5


def test_log_line_asctime_is_utc_and_marked(kyiv_process_zone: None) -> None:
    handler = _ListBufferHandler(static_ctx={}, ctx_keys=[])
    record = logging.LogRecord("t", logging.INFO, "", 0, "m", (), None)
    record.created = datetime(2026, 10, 5, 12, 0, 0, tzinfo=UTC).timestamp()
    record.msecs = 123.0
    handler.emit(record)
    asctime = handler.buffer[0]["asctime"]
    assert asctime == "2026-10-05 12:00:00.123Z"
    assert re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}Z", asctime)
