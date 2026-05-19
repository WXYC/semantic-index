"""Tests for the field-corruption helpers in ``scripts/eval/build_wrong_set.py``.

These are pure functions that take a metadata dict (the shape ``_lookup_artist_metadata``
emits) and return a copy with one field deliberately corrupted. The orchestrator wraps
them in a per-pair strategy picker; here we cover the building blocks.
"""

from __future__ import annotations

import random

import pytest

from scripts.eval.build_wrong_set import (
    _inject_outlier_styles,
    _is_refusal,
    _pick_corruption,
    _pick_outlier_styles_for,
    _swap_genre,
    _swap_voice_instrumental,
)

# ---------------------------------------------------------------------------
# _swap_voice_instrumental
# ---------------------------------------------------------------------------


def test_swap_voice_instrumental_flips_instrumental_to_vocal():
    meta = {
        "name": "Stereolab",
        "audio": {"voice_instrumental": "instrumental", "recording_count": 12},
    }
    out = _swap_voice_instrumental(meta)
    assert out is not None
    assert out["audio"]["voice_instrumental"] == "vocal-forward"
    assert out["audio"]["recording_count"] == 12
    # Input untouched (no in-place mutation).
    assert meta["audio"]["voice_instrumental"] == "instrumental"


def test_swap_voice_instrumental_flips_vocal_to_instrumental():
    meta = {"name": "X", "audio": {"voice_instrumental": "vocal-forward"}}
    out = _swap_voice_instrumental(meta)
    assert out is not None
    assert out["audio"]["voice_instrumental"] == "instrumental"


def test_swap_voice_instrumental_returns_none_without_audio():
    assert _swap_voice_instrumental({"name": "X"}) is None


def test_swap_voice_instrumental_returns_none_without_field():
    assert _swap_voice_instrumental({"name": "X", "audio": {"recording_count": 5}}) is None


def test_swap_voice_instrumental_returns_none_for_unknown_value():
    # Production qualitative descriptors only emit the two extremes; anything else
    # would be a schema drift we don't want to silently corrupt.
    meta = {"name": "X", "audio": {"voice_instrumental": "ambiguous"}}
    assert _swap_voice_instrumental(meta) is None


# ---------------------------------------------------------------------------
# _inject_outlier_styles
# ---------------------------------------------------------------------------


def test_inject_outlier_styles_replaces_existing_styles():
    meta = {"name": "X", "styles": ["Indie Rock", "Post-Rock"]}
    out = _inject_outlier_styles(meta, ["Dance-pop", "Euro House", "Makina"])
    assert out is not None
    assert out["styles"] == ["Dance-pop", "Euro House", "Makina"]
    # Input untouched.
    assert meta["styles"] == ["Indie Rock", "Post-Rock"]


def test_inject_outlier_styles_returns_none_when_meta_has_no_styles():
    assert _inject_outlier_styles({"name": "X"}, ["Trance"]) is None


def test_inject_outlier_styles_returns_none_when_outliers_empty():
    assert _inject_outlier_styles({"name": "X", "styles": ["Rock"]}, []) is None


# ---------------------------------------------------------------------------
# _swap_genre
# ---------------------------------------------------------------------------


def test_swap_genre_replaces():
    meta = {"name": "X", "genre": "Rock"}
    out = _swap_genre(meta, "Electronic")
    assert out is not None
    assert out["genre"] == "Electronic"
    assert meta["genre"] == "Rock"


def test_swap_genre_returns_none_when_missing():
    assert _swap_genre({"name": "X"}, "Rock") is None


def test_swap_genre_returns_none_when_same():
    assert _swap_genre({"name": "X", "genre": "Rock"}, "Rock") is None


# ---------------------------------------------------------------------------
# _pick_outlier_styles_for
# ---------------------------------------------------------------------------


def test_pick_outlier_styles_excludes_target_genre():
    pool = {
        "Rock": [["Indie Rock"], ["Post-Rock"]],
        "Electronic": [["Trance", "House"], ["Techno"]],
        "Jazz": [["Bebop"]],
    }
    rng = random.Random(0)
    # Run many times — should never pick a Rock style-list.
    for _ in range(50):
        picked = _pick_outlier_styles_for(pool, "Rock", rng)
        assert picked  # non-empty
        # The Rock entries are the only ones containing "Indie Rock" or "Post-Rock".
        assert "Indie Rock" not in picked
        assert "Post-Rock" not in picked


def test_pick_outlier_styles_returns_empty_when_pool_empty():
    rng = random.Random(0)
    assert _pick_outlier_styles_for({}, "Rock", rng) == []


def test_pick_outlier_styles_returns_empty_when_only_target_genre_in_pool():
    pool = {"Rock": [["Indie Rock"]]}
    rng = random.Random(0)
    assert _pick_outlier_styles_for(pool, "Rock", rng) == []


# ---------------------------------------------------------------------------
# _pick_corruption (orchestrator)
# ---------------------------------------------------------------------------


def test_pick_corruption_skips_pair_with_no_corruptable_fields():
    # Neither side has genre, styles, or audio.voice_instrumental — skip.
    source = {"name": "X", "total_plays": 100}
    target = {"name": "Y", "total_plays": 100}
    outlier_pool: dict[str, list[list[str]]] = {"Rock": [["Indie Rock"]]}
    rng = random.Random(0)
    result = _pick_corruption(source, target, outlier_pool, rng, genre_pool=["Electronic"])
    assert result is None


def test_pick_corruption_returns_strategy_and_modified_meta():
    source = {
        "name": "X",
        "genre": "Rock",
        "styles": ["Indie Rock"],
        "audio": {"voice_instrumental": "vocal-forward"},
    }
    target = {"name": "Y", "genre": "Jazz"}
    outlier_pool = {"Electronic": [["Trance", "Tech House"]]}
    rng = random.Random(7)
    result = _pick_corruption(source, target, outlier_pool, rng, genre_pool=["Electronic", "Jazz"])
    assert result is not None
    new_source, new_target, side, strategy = result
    assert side in ("source", "target")
    assert strategy in ("voice_instrumental", "outlier_styles", "genre_swap")
    # Whichever side was corrupted, the other should equal the input.
    if side == "source":
        assert new_target == target
        assert new_source != source
    else:
        assert new_source == source
        assert new_target != target


def test_pick_corruption_voice_instrumental_only_feasible_when_audio_present():
    # Force the rng to a known seed; if voice_instrumental is the picked strategy,
    # it should only apply to a side that has it.
    source = {"name": "X", "audio": {"voice_instrumental": "instrumental"}}
    target = {"name": "Y"}  # no audio
    outlier_pool: dict[str, list[list[str]]] = {}
    rng = random.Random(42)
    # The only feasible (side, strategy) here is (source, voice_instrumental) since
    # target has no genre/styles/audio.
    result = _pick_corruption(source, target, outlier_pool, rng, genre_pool=[])
    assert result is not None
    new_source, new_target, side, strategy = result
    assert side == "source"
    assert strategy == "voice_instrumental"
    assert new_source["audio"]["voice_instrumental"] == "vocal-forward"


# ---------------------------------------------------------------------------
# _is_refusal
# ---------------------------------------------------------------------------


def test_is_refusal_short_narrative():
    assert _is_refusal("Too short.") is True
    # ~30 words is the minimum bar.
    short = " ".join(["word"] * 10)
    assert _is_refusal(short) is True


def test_is_refusal_recognizes_refusal_phrases():
    long_enough = " ".join(["word"] * 50)
    assert (
        _is_refusal(f"I am unable to characterize this connection meaningfully. {long_enough}")
        is True
    )
    assert _is_refusal(f"I cannot write a narrative from this data. {long_enough}") is True
    assert _is_refusal(f"The input data lacks sufficient signal. {long_enough}") is True


def test_is_refusal_empty():
    assert _is_refusal("") is True
    assert _is_refusal(None) is True  # type: ignore[arg-type]


def test_is_refusal_accepts_substantive_narrative():
    text = (
        "WXYC DJs pair Stereolab with Yo La Tengo across 8 transitions in our flowsheets. "
        "Both share the Indie Rock tag, and Tortoise and Lambchop appear as common "
        "neighbors in DJs' programming, suggesting a shared late-90s post-rock lineage."
    )
    assert _is_refusal(text) is False


@pytest.mark.parametrize("strategy", ["voice_instrumental", "outlier_styles", "genre_swap"])
def test_pick_corruption_strategies_are_reachable(strategy):
    """Every strategy must be picked at least once across many seeds when feasible."""
    source = {
        "name": "X",
        "genre": "Rock",
        "styles": ["Indie Rock"],
        "audio": {"voice_instrumental": "instrumental"},
    }
    target = {
        "name": "Y",
        "genre": "Jazz",
        "styles": ["Bebop"],
        "audio": {"voice_instrumental": "vocal-forward"},
    }
    outlier_pool = {"Electronic": [["Trance"]]}
    genre_pool = ["Electronic", "Jazz", "Rock"]
    seen: set[str] = set()
    for seed in range(200):
        rng = random.Random(seed)
        result = _pick_corruption(source, target, outlier_pool, rng, genre_pool=genre_pool)
        assert result is not None
        seen.add(result[3])
        if strategy in seen:
            return
    raise AssertionError(f"Strategy {strategy} never picked across 200 seeds; seen={seen}")
