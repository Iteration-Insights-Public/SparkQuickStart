"""
state.py — PipelineState: the immutable bundle that flows through the pipeline.
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from spark_quick_start.orchestration.transformers._constants import Constants

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

__all__ = ["PipelineState"]

_TARGET = Constants.TARGET_FRAME_KEY


@dataclass
class PipelineState:
    """Carries the primary DataFrame, any named secondary DataFrames, and whether the target exists.

    Immutable by convention — use with_df / with_frame / with_target / with_halted /
    with_updates / with_debug_columns / with_hash_separator / with_cached to produce new states. Every one of them carries every field, so a step
    that only touches ``df`` cannot lose what an earlier step stated.

    ``frames`` never holds ``None``. "The target table does not exist" is stated by
    ``target_exists=False`` with the ``"target"`` key absent, set by :meth:`with_target`
    (F28); before it, a ``None`` frame stood for that, and was indistinguishable from a
    key a step had nulled by mistake. Construction enforces the invariants: a ``None``
    frame value raises ``TypeError``, ``target_exists=True`` without the key and
    ``target_exists=False`` with it raise ``ValueError``. Writes through ``with_frame`` or
    ``with_updates`` under the target key route to ``with_target``, so only direct
    construction can reach those ``ValueError``s.

    Attributes:
        df: The primary DataFrame flowing through the pipeline.
        frames: Named secondary DataFrames (e.g. "target" for the Silver table). Never
            holds ``None``.
        halted: When True, downstream ProcessTransformers skip execution and pass
            the state through unchanged. Set via with_halted().
        halt_reason: Human-readable explanation of why the pipeline was halted.
        target_exists: ``None`` until a target step has run; then ``True`` (the
            ``"target"`` frame is present, possibly empty) or ``False`` (the table does
            not exist and the key is absent). An existing but empty table is ``True``.
            Set via with_target().
        debug_columns: Names of columns on ``df`` that exist for troubleshooting and must never
            be written to a table, such as the detector's ``debug_hash_string_column`` (F111). A
            step that adds one declares it with :meth:`with_debug_columns`; a writer drops these
            before it stages its rows. Empty by default.
        hash_separator: The separator the SCD change detector hashed the candidates with, stated
            by it with :meth:`with_hash_separator` so the writer can seal what was really used
            (F211); ``None`` until it runs.
        cached: Frames a step persisted in memory (``MaterializeTransformer`` and
            ``MaterializeFrameTransformer`` under ``runtime_context.materialize_strategy``
            ``"cache"``), declared with :meth:`with_cached`. ``LoadOrchestrator.run()`` unpersists
            every one when the run ends, whatever its outcome. A step that caches a frame and
            then fails releases it itself before raising, so this holds only completed steps'
            frames. Empty by default.

    Raises:
        TypeError: A ``None`` value in ``frames``.
        ValueError: ``target_exists`` contradicts the presence of the ``"target"`` key.
    """

    df: DataFrame
    frames: dict[str, DataFrame] = field(default_factory=dict)
    halted: bool = False
    halt_reason: str = ""
    target_exists: bool | None = None
    debug_columns: frozenset[str] = frozenset()
    hash_separator: str | None = None
    cached: tuple[DataFrame, ...] = ()

    def __post_init__(self) -> None:
        _refuse_none_frames(self.frames)
        present = _TARGET in self.frames
        if self.target_exists is True and not present:
            raise ValueError(
                f"PipelineState(target_exists=True) has no frames[{_TARGET!r}]: a present target "
                "is a frame. Set both with with_target(frame)."
            )
        if self.target_exists is False and present:
            raise ValueError(
                f"PipelineState(target_exists=False) holds frames[{_TARGET!r}]: an absent target "
                "has no frame. Replace the target with with_target(frame), or state it absent "
                "with with_target(None)."
            )

    def with_df(self, df: DataFrame) -> PipelineState:
        """Return a new PipelineState with df replaced; every other field carried."""
        return replace(self, df=df, frames=dict(self.frames))

    def with_frame(self, name: str, df: DataFrame) -> PipelineState:
        """Return a new PipelineState with an additional named frame; every other field carried.

        A write under the target key is routed to :meth:`with_target`, the setter every
        target write goes through, so it works from any prior ``target_exists``.

        Args:
            name: The frame's name.
            df: A DataFrame; ``None`` is refused (``TypeError``). The one sanctioned legacy
                form, ``with_frame("target", None)``, still works but emits a
                ``DeprecationWarning`` and is ``with_target(None)``.

        Raises:
            TypeError: ``df`` is ``None`` and ``name`` is not the target key.
        """
        if name == _TARGET:
            if df is None:
                _warn_none_target("with_frame('target', None)", stacklevel=3)
            return self.with_target(df)
        _refuse_none_frames({name: df})
        return replace(self, frames={**self.frames, name: df})

    def with_target(self, frame: DataFrame | None) -> PipelineState:
        """Return a new PipelineState stating whether the target exists; every target write ends here.

        ``with_frame("target", ...)`` and ``with_updates(target=...)`` route to this method.

        Args:
            frame: The target table's DataFrame (possibly empty) when the table exists, or
                ``None`` when it does not.

        Returns:
            With a frame: ``frames["target"]`` set and ``target_exists=True``. With ``None``:
            ``"target"`` removed from the copy's frames and ``target_exists=False``. Every
            other field is carried.
        """
        frames = {k: v for k, v in self.frames.items() if k != _TARGET}
        if frame is None:
            return replace(self, frames=frames, target_exists=False)
        frames[_TARGET] = frame
        return replace(self, frames=frames, target_exists=True)

    def with_debug_columns(self, *names: str) -> PipelineState:
        """Return a new PipelineState that also declares *names* as debug columns; every other field carried.

        A debug column rides on ``df`` for inspection and is dropped by the writer before the
        write, so it never reaches a target table (F111).
        """
        return replace(self, frames=dict(self.frames), debug_columns=self.debug_columns | frozenset(names))

    def with_hash_separator(self, separator: str) -> PipelineState:
        """Return a new PipelineState stating the separator the change hash used; every other field carried."""
        return replace(self, frames=dict(self.frames), hash_separator=separator)

    def with_cached(self, df: DataFrame) -> PipelineState:
        """Return a new PipelineState that also declares *df* as persisted; every other field carried.

        The orchestrator unpersists every declared frame when the run ends. Declaring the same
        frame twice records it once.
        """
        if any(c is df for c in self.cached):
            return replace(self, frames=dict(self.frames))
        return replace(self, frames=dict(self.frames), cached=(*self.cached, df))

    def with_halted(self, reason: str) -> PipelineState:
        """Return a new PipelineState with halted=True and the given reason; every other field carried."""
        return replace(self, frames=dict(self.frames), halted=True, halt_reason=reason)

    def with_updates(
        self,
        df: DataFrame | None = None,
        **frames: DataFrame,
    ) -> PipelineState:
        """Return a new PipelineState with df and/or named frames replaced in one step.

        Args:
            df: Replacement primary DataFrame. When ``None``, the current ``df`` is kept.
            **frames: Named secondary DataFrames to add or overwrite. Existing frames
                not mentioned here are preserved unchanged. A ``target`` value is routed
                to :meth:`with_target` (a DataFrame sets ``target_exists=True``; ``None``
                emits a ``DeprecationWarning`` and is ``with_target(None)``). A ``None``
                for any other key is refused.

        Raises:
            TypeError: A ``None`` value for a key other than the target.

        Example::

            # Replace df and stash a side-car in one call
            return state.with_updates(df=candidates_df, candidates=candidates_df)

            # Add a frame without touching df
            return state.with_updates(side_car=other_df)
        """
        has_target = _TARGET in frames
        target = frames.pop(_TARGET, None)
        if has_target and target is None:
            _warn_none_target("with_updates(target=None)", stacklevel=3)
        _refuse_none_frames(frames)
        state = replace(
            self,
            df=df if df is not None else self.df,
            frames={**self.frames, **frames},
        )
        return state.with_target(target) if has_target else state


def _warn_none_target(form: str, *, stacklevel: int) -> None:
    """Emit the deprecation for a ``None`` target written through a generic setter."""
    warnings.warn(
        f"{form} is deprecated: use with_target(None), which also sets target_exists=False (F28).",
        DeprecationWarning,
        stacklevel=stacklevel,
    )


def _refuse_none_frames(frames: dict[str, DataFrame]) -> None:
    """Raise ``TypeError`` for a ``None`` frame, naming with_target for the target key."""
    for name, frame in frames.items():
        if frame is None:
            hint = (
                "state an absent target with with_target(None)"
                if name == _TARGET
                else "None is not a frame; the only absence a state carries is the target's, "
                "stated with with_target(None)"
            )
            raise TypeError(f"PipelineState.frames[{name!r}] is None: {hint} (F28).")
