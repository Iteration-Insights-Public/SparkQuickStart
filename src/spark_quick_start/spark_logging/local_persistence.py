"""Local filesystem log persistence adapter.

Contains: LocalFilePersistenceAdapter — pure pathlib/shutil, for testing and
local development. Does NOT contain: orchestration logic or any Fabric/
Databricks-specific code.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

from spark_quick_start.spark_logging.persistence_adapter import LogPersistenceAdapter


class LocalFilePersistenceAdapter(LogPersistenceAdapter):
    """Local filesystem adapter using ``pathlib`` — for testing and local development."""

    def mkdir(self, path: str) -> None:
        Path(path).mkdir(parents=True, exist_ok=True)

    def write(self, path: str, content: str) -> None:
        Path(path).write_text(content, encoding="utf-8")

    def copy(self, src: str, dst: str) -> None:
        src_clean = src.removeprefix("file:")
        shutil.copy2(src_clean, dst)

    def notebook_exit(self, value: str) -> None:
        pass  # No-op in local mode

    def get_notebook_name(self) -> str | None:
        try:
            from IPython import get_ipython  # type: ignore
            ip = get_ipython()
            if ip and hasattr(ip, "user_ns"):
                nb_file = ip.user_ns.get("__vsc_ipynb_file__")
                if nb_file:
                    return os.path.basename(nb_file)
                # Fabric sets __session__ when running under IPython. This adapter is
                # reached on Fabric — and on Databricks — whenever log_file_dir is a
                # POSIX path: OrchestratorLogger.from_context replaces the platform
                # persistence adapter with LocalFilePersistenceAdapter() wholesale at
                # logger/orchestrator_logger.py:221. There is no fallback chain between
                # adapters; LogSession takes exactly one and raises without it. So on a
                # Fabric run with a POSIX log dir this probe is the only notebook-name
                # source there is, which is why the branch moved here rather than being
                # deleted with the rest of _best_effort_notebook_name() (F1).
                #
                # isinstance-guarded rather than coerced. The branch was dead code
                # before this move — its function had no caller — so str() had never
                # run, and the result now lands in LogSession.notebook_name and from
                # there in the notebook_name column of every persisted log row. If
                # Fabric's __session__ is ever a session object rather than a name,
                # coercing it would write "<... object at 0x...>" into that column
                # where the dead code wrote nothing. Cannot be confirmed without a live
                # Fabric workspace, so the guard is the answer instead of an assumption.
                session = ip.user_ns.get("__session__")
                if isinstance(session, str) and session:
                    return session
        except Exception:
            pass
        return None
