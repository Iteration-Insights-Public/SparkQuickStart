"""
test_staging_logging.py — the staging helper's logging set-up wired to the real spark_logging.

Contains: `initialize_logging` driven with the real `init_logs`, in-memory, so nothing is
    written outside tmp.
Does NOT contain: the staging pipeline itself (tests/unit/test_staging_pipeline.py).
"""
from __future__ import annotations

from spark_quick_start.orchestration.staging_pipeline import initialize_logging


def test_initialize_logging_works_with_the_real_init_logs(tmp_path, monkeypatch):
    """F126: ``log_init_fn=init_logs`` with a ``log_file_dir`` in the context must not raise.

    ``init_logs`` names its directory ``lakehouse_dir``; the helper used to pass
    ``log_file_dir=`` and every staging notebook returned an error result. In-memory
    mode ignores the directory, so nothing is written under it.
    """
    import spark_quick_start.spark_logging as sl

    monkeypatch.setattr(sl, "_CURRENT", None)  # restored after; no session leaks out
    log_dir = tmp_path / "logs"
    try:
        logger = initialize_logging(
            {"log_file_name": "F126", "logging_level": "INFO", "log_file_dir": str(log_dir)},
            "corr-126",
            {"stages": []},
            sl.init_logs,
        )
        assert logger.name == "F126"
        assert sl._CURRENT is not None
    finally:
        if sl._CURRENT is not None:
            sl._CURRENT.finalize()
    assert not log_dir.exists()
