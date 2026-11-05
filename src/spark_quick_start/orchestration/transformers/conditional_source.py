"""conditional_source.py — ConditionalSourceTransformer.

Selects between two SourceTransformers at runtime based on a predicate,
allowing the pipeline source to vary without pre-parsing the context JSON.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any, Union

from spark_quick_start.orchestration.transformers.base import LoadContext, SourceTransformer
from spark_quick_start.orchestration.transformers.state import PipelineState

Predicate = Union[bool, Callable[[LoadContext], bool]]


class ConditionalSourceTransformer(SourceTransformer):
    """Select between two SourceTransformers based on a predicate.

    When the predicate evaluates to True, ``source_if_true`` is used.
    When False, ``source_if_false`` is used.  Both branches receive the
    same spark / adapter / logger injection as a normal SourceTransformer.

    Args:
        predicate: A plain ``bool`` or ``Callable[[LoadContext], bool]``.
        source_if_true: SourceTransformer to run when predicate is True.
        source_if_false: SourceTransformer to run when predicate is False.
        label: Optional name for log messages.

    After ``_produce()`` runs, ``selected_source`` holds whichever of
    ``source_if_true`` / ``source_if_false`` was actually used — e.g. so
    ``LoadOrchestrator`` can find a wrapped ``DQDTransformer``'s HWM values,
    which live on that inner instance, not on this wrapper.

    Example::

        .set_source(
            lambda ctx: ctx.dqd_ctx is not None,
            DQDTransformer(),
            StagingTransformer(),
        )
    """

    _ctx_manager: Any = None

    def __init__(
        self,
        predicate: Predicate,
        source_if_true: SourceTransformer,
        source_if_false: SourceTransformer,
        label: str | None = None,
    ) -> None:
        if isinstance(predicate, bool):
            self._predicate: Callable[[LoadContext], bool] = lambda _: predicate
        elif callable(predicate):
            self._predicate = predicate
        else:
            raise TypeError(
                f"ConditionalSourceTransformer: predicate must be bool or "
                f"Callable[[LoadContext], bool], got {type(predicate).__name__}"
            )
        self._source_if_true = source_if_true
        self._source_if_false = source_if_false
        self._label = label
        self.selected_source: SourceTransformer | None = None

    @property
    def _name(self) -> str:
        return self._label or (
            f"{type(self._source_if_true).__name__}|{type(self._source_if_false).__name__}"
        )

    def _select_source(self, ctx: LoadContext) -> SourceTransformer:
        """Evaluate the predicate, wire services into the chosen source, return it."""
        try:
            should_use_true = self._predicate(ctx)
        except Exception as exc:
            raise RuntimeError(
                f"ConditionalSourceTransformer: predicate raised while selecting "
                f"source for {self._name}: {exc}"
            ) from exc

        source = self._source_if_true if should_use_true else self._source_if_false
        self.selected_source = source
        source._spark = self._spark
        source._adapter = self._adapter
        source._logger = self._logger
        source._metrics_collector = self._metrics_collector
        source._stage_index = self._stage_index
        if hasattr(source, "_ctx_manager"):
            source._ctx_manager = getattr(self, "_ctx_manager", None)

        if self._logger:
            self._logger.info(f"[{self._name}] selected {type(source).__name__}")
        return source

    def produce(self, ctx: LoadContext) -> PipelineState:
        """Delegate the whole SourceTransformer lifecycle to the selected source.

        This deliberately does not call ``super().produce``. Like
        ``ConditionalTransformer``, this wrapper is routing rather than a
        pipeline stage: running our own ``before_run`` -> ``_produce`` ->
        ``_validate_source_columns`` -> ``after_run`` template would either
        double-record StageMetrics or suppress the selected source's own hooks.

        Delegating gives exactly one lifecycle — the selected source's — so an
        override of ``before_run``/``after_run`` on that source contributes its
        metrics, and a missing-SCD-column error names the real transformer
        rather than this wrapper.
        """
        return self._select_source(ctx).produce(ctx)

    def _produce(self, ctx: LoadContext) -> PipelineState:
        """Selection + inner ``_produce`` only, without the inner's hooks.

        ``produce()`` above is the normal path; this exists to satisfy
        ``SourceTransformer``'s abstract method and to keep a direct
        ``_produce()`` call (as the orchestrator's own tests make) working.
        """
        return self._select_source(ctx)._produce(ctx)
