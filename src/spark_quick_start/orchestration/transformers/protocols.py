"""
protocols.py — Structural protocols for optional transformer capabilities.

Transformers that implement these protocols are discovered by LoadOrchestrator
at the end of a pipeline run. No registration is required — implement the
attributes and the orchestrator will pick them up automatically.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class HWMProvider(Protocol):
    """Transformer that reports high-watermark values to the orchestrator.

    Implement this on any transformer that knows the HWM for a pipeline run.
    LoadOrchestrator scans all registered transformers after the pipeline
    completes. When the load context has an ``hwm_context``, the first one
    whose ``hwm_column`` equals ``hwm_context.hwm_column_name`` owns the
    watermark and wins whatever it found, even ``hwm_after`` None (a first run
    that staged nothing, F138). Otherwise, or when none reports for that
    column, it uses the first one whose ``hwm_after`` is not None.

    Priority is determined by transformer registration order — transformers
    registered earlier take precedence. Source transformers are registered before SCD
    merge transformers, so a source's watermark wins when both provide a value.

    A transformer wrapped in ``ConditionalTransformer``/``.add_if()`` or
    ``ConditionalSourceTransformer``/``.set_source()`` is unwrapped before this
    scan runs, so an HWMProvider chosen conditionally is still found — see
    LoadOrchestrator.run()'s ``_unwrap`` helper.

    Example::

        class MyCustomTransformer(SourceTransformer, HWMProvider):
            hwm_before: str | None = None
            hwm_after:  str | None = None
            hwm_column: str | None = None

            def _produce(self, ctx) -> PipelineState:
                self.hwm_before = ...
                self.hwm_after  = ...
                self.hwm_column = "my_date_column"
                ...
    """
    hwm_before: str | None
    hwm_after: str | None
    hwm_column: str | None


@runtime_checkable
class SourceMetricsProvider(Protocol):
    """Source transformer that reports row counts to the orchestrator.

    Implement this on a SourceTransformer that knows how many rows it
    produced. LoadOrchestrator scans registered transformers after the
    pipeline completes and uses the first one found — unlike HWMProvider,
    there's no "which of several mutually-exclusive candidates actually
    ran" ambiguity to resolve here, since a pipeline has exactly one active
    source (StagingTransformer or DQDTransformer, possibly picked at
    runtime via ``.set_source()``/``ConditionalSourceTransformer``, which
    ``LoadOrchestrator.run()``'s ``_unwrap`` helper already resolves to the
    one actually selected).

    ``source_count``/``target_count`` may legitimately be ``None`` — that
    means "not computed" (e.g. DQDTransformer without ``hwm_context``), not
    "zero rows". LoadOrchestrator collapses ``None`` to ``0`` at the point it
    writes ``ExecutionMetrics.source_count``/``target_count`` (those fields are
    typed ``int``, not ``Optional[int]``) — so an uncounted DQD run and a
    genuinely empty run both show ``0`` in the final result. Both fields
    are always populated together when a source can produce them at all —
    a transformer that has no distinct "target" step (like DQDTransformer)
    should mirror ``target_count`` from ``source_count`` rather than
    leaving it ``None`` while ``source_count`` is set. See
    ``ExecutionMetrics``'s docstring for the full explanation.

    Example::

        class MyCustomSource(SourceTransformer, SourceMetricsProvider):
            source_count: int | None = None
            target_count: int | None = None

            def _produce(self, ctx) -> PipelineState:
                df = ...
                self.source_count = df.count()  # or read for free if you wrote it somewhere
                ...
    """
    source_count: int | None
    target_count: int | None


@runtime_checkable
class SCDMetricsProvider(Protocol):
    """Transformer that reports SCD write statistics to the orchestrator.

    Implement this on any transformer that writes to the target table and knows
    how many rows it inserted and updated. LoadOrchestrator scans the registered
    transformers after the pipeline completes and harvests the first one that
    both implements this protocol and actually ran.

    "Actually ran" matters: when mutually-exclusive SCD transformers are
    registered via ``.add_if()`` branches (append-only vs merge, say), a plain
    scan would pick whichever was registered first regardless of which branch
    executed, reporting the never-executed one's ``__init__`` defaults of 0.
    LoadOrchestrator therefore takes the first provider whose ``inserted`` and ``updated``
    are not ``None`` (F117): declare both as ``None`` in ``__init__`` and set them inside
    ``_run()``, so "ran" is proved by the counts themselves and not by a watermark read that
    is allowed to fail. Every shipped provider follows this convention.

    ``updated`` is part of the protocol even for load styles that never update
    anything: an append-only writer sets ``updated = 0`` in ``_run()``. Keeping it required
    means the orchestrator reads a declared attribute rather than probing with
    ``getattr(tx, "updated", 0)`` and silently treating "attribute missing" and
    "genuinely zero" as the same thing.

    Example::

        class MyWriter(ProcessTransformer, SCDMetricsProvider):
            def __init__(self) -> None:
                self.inserted: int | None = None
                self.updated: int | None = None

            def _run(self, state, ctx):
                ...
                self.inserted, self.updated = ...
                return state
    """
    inserted: int | None
    updated: int | None
