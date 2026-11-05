# tests/test_pipeline_state.py
from __future__ import annotations

import warnings

import pytest

from spark_quick_start.orchestration.transformers._constants import Constants
from spark_quick_start.orchestration.transformers.state import PipelineState

KEY = Constants.TARGET_FRAME_KEY


def _df():
    # Identity only: these tests assert which object a state carries, never what it does.
    return object()


def test_pipeline_state_holds_df():
    df = _df()
    state = PipelineState(df=df)
    assert state.df is df


def test_pipeline_state_frames_default_empty():
    state = PipelineState(df=_df())
    assert state.frames == {}


def test_with_df_replaces_primary():
    old_df = _df()
    new_df = _df()
    state = PipelineState(df=old_df)
    next_state = state.with_df(new_df)
    assert next_state.df is new_df


def test_with_df_preserves_frames():
    target = _df()
    state = PipelineState(df=_df(), frames={"target": target})
    next_state = state.with_df(_df())
    assert next_state.frames["target"] is target


def test_with_frame_adds_named_df():
    target = _df()
    state = PipelineState(df=_df())
    next_state = state.with_frame("target", target)
    assert next_state.frames["target"] is target


def test_with_frame_preserves_primary_df():
    df = _df()
    state = PipelineState(df=df)
    next_state = state.with_frame("target", _df())
    assert next_state.df is df


def test_with_df_is_immutable():
    """Original state is unchanged after with_df."""
    df = _df()
    state = PipelineState(df=df)
    state.with_df(_df())
    assert state.df is df


def test_with_frame_is_immutable():
    """Original frames dict is unchanged after with_frame."""
    state = PipelineState(df=_df())
    state.with_frame("target", _df())
    assert "target" not in state.frames


# --- with_updates -----------------------------------------------------------

def test_with_updates_replaces_df():
    new_df = _df()
    state = PipelineState(df=_df())
    next_state = state.with_updates(df=new_df)
    assert next_state.df is new_df


def test_with_updates_keeps_df_when_none():
    df = _df()
    state = PipelineState(df=df)
    next_state = state.with_updates(target=_df())
    assert next_state.df is df


def test_with_updates_adds_frames():
    target = _df()
    state = PipelineState(df=_df())
    next_state = state.with_updates(target=target)
    assert next_state.frames["target"] is target


def test_with_updates_merges_existing_frames():
    existing = _df()
    new_frame = _df()
    state = PipelineState(df=_df(), frames={"existing": existing})
    next_state = state.with_updates(new_frame=new_frame)
    assert next_state.frames["existing"] is existing
    assert next_state.frames["new_frame"] is new_frame


def test_with_updates_overwrites_frame():
    old_frame = _df()
    new_frame = _df()
    state = PipelineState(df=_df(), frames={"target": old_frame})
    next_state = state.with_updates(target=new_frame)
    assert next_state.frames["target"] is new_frame


def test_with_updates_replaces_df_and_adds_frame():
    new_df = _df()
    side_car = _df()
    state = PipelineState(df=_df())
    next_state = state.with_updates(df=new_df, candidates=side_car)
    assert next_state.df is new_df
    assert next_state.frames["candidates"] is side_car


def test_with_updates_is_immutable():
    """Original state unchanged after with_updates."""
    df = _df()
    state = PipelineState(df=df)
    state.with_updates(df=_df(), extra=_df())
    assert state.df is df
    assert "extra" not in state.frames


# --- target_exists and with_target (F28) ---------------------------------------


def test_target_exists_defaults_to_none_no_target_step_ran():
    assert PipelineState(df=_df()).target_exists is None


def test_with_target_frame_sets_the_key_and_true():
    frame = _df()
    nxt = PipelineState(df=_df()).with_target(frame)
    assert nxt.frames[KEY] is frame
    assert nxt.target_exists is True


def test_with_target_none_leaves_the_key_absent_and_false():
    nxt = PipelineState(df=_df()).with_target(None)
    assert KEY not in nxt.frames
    assert nxt.target_exists is False


def test_with_target_none_drops_an_existing_target_frame_and_keeps_the_others():
    other = _df()
    state = PipelineState(df=_df(), frames={KEY: _df(), "other": other}, target_exists=True)
    nxt = state.with_target(None)
    assert nxt.frames == {"other": other}
    assert nxt.target_exists is False
    assert KEY in state.frames and state.target_exists is True  # the original is untouched


def test_with_target_frame_after_none_flips_back_to_true():
    frame = _df()
    nxt = PipelineState(df=_df()).with_target(None).with_target(frame)
    assert nxt.frames[KEY] is frame and nxt.target_exists is True


def test_with_target_does_not_touch_df_or_halt():
    df = _df()
    nxt = PipelineState(df=df, halted=True, halt_reason="r").with_target(None)
    assert (nxt.df, nxt.halted, nxt.halt_reason) == (df, True, "r")


_TRANSITIONS = {
    "with_df": lambda s: s.with_df(_df()),
    "with_frame": lambda s: s.with_frame("side", _df()),
    "with_halted": lambda s: s.with_halted("because"),
    "with_updates": lambda s: s.with_updates(df=_df(), side=_df()),
    "with_debug_columns": lambda s: s.with_debug_columns("dbg"),
    "with_hash_separator": lambda s: s.with_hash_separator("|"),
    "with_cached": lambda s: s.with_cached(_df()),
}


def _state_with(target_exists):
    frames = {KEY: _df()} if target_exists is True else {}
    return PipelineState(df=_df(), frames=frames, target_exists=target_exists)


@pytest.mark.parametrize("target_exists", [True, False, None])
@pytest.mark.parametrize("transition", sorted(_TRANSITIONS))
def test_every_with_method_carries_target_exists(transition, target_exists):
    assert _TRANSITIONS[transition](_state_with(target_exists)).target_exists is target_exists


def test_every_with_method_is_covered_by_the_carry_test():
    methods = {n for n in dir(PipelineState) if n.startswith("with_") and n != "with_target"}
    assert methods == set(_TRANSITIONS), "a new with_* method must be added to _TRANSITIONS"


@pytest.mark.parametrize("transition", sorted(_TRANSITIONS))
def test_with_methods_do_not_mutate_the_original(transition):
    state = _state_with(True)
    frames, df = dict(state.frames), state.df
    _TRANSITIONS[transition](state)
    assert state.frames == frames and state.df is df and not state.halted


def test_true_without_the_key_is_refused():
    with pytest.raises(ValueError, match="target_exists=True"):
        PipelineState(df=_df(), target_exists=True)


def test_false_with_the_key_is_refused():
    with pytest.raises(ValueError, match="target_exists=False"):
        PipelineState(df=_df(), frames={KEY: _df()}, target_exists=False)


def test_with_frame_of_a_target_where_the_target_is_said_absent_routes_to_with_target():
    frame = _df()
    nxt = PipelineState(df=_df()).with_target(None).with_frame(KEY, frame)
    assert nxt.frames[KEY] is frame and nxt.target_exists is True


def test_with_frame_of_a_target_from_none_states_the_target_present():
    frame = _df()
    nxt = PipelineState(df=_df()).with_frame(KEY, frame)
    assert nxt.frames[KEY] is frame and nxt.target_exists is True


def test_with_updates_of_a_target_states_the_target_present_even_from_false():
    frame = _df()
    nxt = PipelineState(df=_df()).with_target(None).with_updates(**{KEY: frame})
    assert nxt.frames[KEY] is frame and nxt.target_exists is True


def test_with_updates_of_a_target_from_none_states_the_target_present():
    frame = _df()
    nxt = PipelineState(df=_df()).with_updates(**{KEY: frame})
    assert nxt.target_exists is True


def test_with_updates_target_none_warns_and_states_the_target_absent():
    state = PipelineState(df=_df(), frames={KEY: _df()}, target_exists=True)
    with pytest.warns(DeprecationWarning, match=r"with_target\(None\)") as caught:
        nxt = state.with_updates(**{KEY: None})
    assert nxt.target_exists is False and KEY not in nxt.frames
    assert caught[0].filename == __file__


def test_with_updates_target_none_still_applies_the_other_frames():
    side, df = _df(), _df()
    with pytest.warns(DeprecationWarning):
        nxt = PipelineState(df=_df()).with_updates(df=df, side=side, **{KEY: None})
    assert nxt.df is df and nxt.frames == {"side": side} and nxt.target_exists is False


def test_with_updates_refuses_none_for_any_other_key():
    with pytest.raises(TypeError, match="with_target"):
        PipelineState(df=_df()).with_updates(side=None)


def test_the_constructor_refuses_a_none_frame_and_names_with_target():
    with pytest.raises(TypeError, match="with_target"):
        PipelineState(df=_df(), frames={KEY: None})


def test_with_frame_refuses_none_for_any_other_key():
    with pytest.raises(TypeError, match="with_target"):
        PipelineState(df=_df()).with_frame("side", None)


def test_the_legacy_with_frame_target_none_warns_and_states_the_target_absent():
    state = PipelineState(df=_df(), frames={KEY: _df()}, target_exists=True)
    with pytest.warns(DeprecationWarning, match=r"with_target\(None\)") as caught:
        nxt = state.with_frame(KEY, None)
    assert nxt.target_exists is False and KEY not in nxt.frames
    assert caught[0].filename == __file__  # stacklevel points at the caller


def test_with_frame_of_a_real_frame_does_not_warn():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        PipelineState(df=_df()).with_frame(KEY, _df())


# ── debug columns (F111) ──────────────────────────────────────────────────────

def test_debug_columns_default_empty_and_are_carried_by_every_setter():
    state = PipelineState(df=_df()).with_debug_columns("dbg")
    assert state.debug_columns == frozenset({"dbg"})
    for later in (
        state.with_df(_df()), state.with_frame("x", _df()), state.with_target(_df()),
        state.with_halted("r"), state.with_updates(_df()),
    ):
        assert later.debug_columns == frozenset({"dbg"})
    assert PipelineState(df=_df()).debug_columns == frozenset()


def test_with_debug_columns_adds_to_the_set_and_leaves_the_original_alone():
    first = PipelineState(df=_df()).with_debug_columns("a")
    second = first.with_debug_columns("b")
    assert (first.debug_columns, second.debug_columns) == (frozenset({"a"}), frozenset({"a", "b"}))


# ── cached frames ─────────────────────────────────────────────────────────────

def test_cached_defaults_empty_and_is_carried_by_every_setter():
    frame = _df()
    state = PipelineState(df=_df()).with_cached(frame)
    assert state.cached == (frame,)
    for later in (
        state.with_df(_df()), state.with_frame("x", _df()), state.with_target(_df()),
        state.with_halted("r"), state.with_updates(_df()), state.with_debug_columns("d"),
        state.with_hash_separator("|"),
    ):
        assert later.cached == (frame,)
    assert PipelineState(df=_df()).cached == ()


def test_with_cached_appends_once_per_frame_and_leaves_the_original_alone():
    a, b = _df(), _df()
    first = PipelineState(df=_df()).with_cached(a)
    second = first.with_cached(b).with_cached(a)
    assert first.cached == (a,)
    assert second.cached == (a, b)
