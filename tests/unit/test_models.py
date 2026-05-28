"""Structural regression tests for pipeline data models.

`FlowsheetEntry` and `ResolvedEntry` are instantiated ~1M times each during a
nightly sync. They MUST be slotted dataclasses (no per-instance `__dict__`)
to fit the production cgroup memory cap. See WXYC/semantic-index#338.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from semantic_index.models import FlowsheetEntry, ResolvedEntry


def _make_flowsheet_entry(**overrides) -> FlowsheetEntry:
    defaults = {
        "id": 1,
        "artist_name": "Juana Molina",
        "song_title": "la paradoja",
        "release_title": "DOGA",
        "library_release_id": 100,
        "label_name": "Sonamos",
        "show_id": 10,
        "sequence": 1,
        "entry_type_code": 1,
    }
    defaults.update(overrides)
    return FlowsheetEntry(**defaults)


class TestFlowsheetEntrySlots:
    def test_instance_has_no_dict(self):
        # Pydantic v2 BaseModel declares __slots__ that includes '__dict__',
        # so `hasattr(cls, '__slots__')` is not a useful check. The real
        # signal is whether instances allow __dict__ allocation.
        entry = _make_flowsheet_entry()
        assert not hasattr(entry, "__dict__"), (
            "FlowsheetEntry instances must NOT have __dict__ "
            "(per-instance dict allocation defeats slot savings, see #338)"
        )

    def test_slots_excludes_dict(self):
        assert "__dict__" not in FlowsheetEntry.__slots__, (
            "FlowsheetEntry.__slots__ must not list '__dict__' "
            "(would re-enable per-instance dict allocation)"
        )

    def test_is_frozen(self):
        entry = _make_flowsheet_entry()
        with pytest.raises(FrozenInstanceError):
            entry.artist_name = "Stereolab"

    def test_construction_with_defaults(self):
        entry = _make_flowsheet_entry()
        assert entry.request_flag == 0
        assert entry.start_time is None

    def test_construction_with_all_fields(self):
        entry = _make_flowsheet_entry(request_flag=1, start_time=1_700_000_000)
        assert entry.request_flag == 1
        assert entry.start_time == 1_700_000_000

    def test_attribute_access_preserved(self):
        entry = _make_flowsheet_entry()
        assert entry.id == 1
        assert entry.artist_name == "Juana Molina"
        assert entry.song_title == "la paradoja"
        assert entry.release_title == "DOGA"
        assert entry.library_release_id == 100
        assert entry.label_name == "Sonamos"
        assert entry.show_id == 10
        assert entry.sequence == 1
        assert entry.entry_type_code == 1


class TestResolvedEntrySlots:
    def test_instance_has_no_dict(self):
        resolved = ResolvedEntry(
            entry=_make_flowsheet_entry(),
            canonical_name="Juana Molina",
            resolution_method="catalog",
        )
        assert not hasattr(resolved, "__dict__")

    def test_slots_excludes_dict(self):
        assert "__dict__" not in ResolvedEntry.__slots__

    def test_is_frozen(self):
        resolved = ResolvedEntry(
            entry=_make_flowsheet_entry(),
            canonical_name="Juana Molina",
            resolution_method="catalog",
        )
        with pytest.raises(FrozenInstanceError):
            resolved.canonical_name = "Stereolab"

    def test_holds_flowsheet_entry_by_reference(self):
        entry = _make_flowsheet_entry()
        resolved = ResolvedEntry(
            entry=entry, canonical_name="Juana Molina", resolution_method="catalog"
        )
        assert resolved.entry is entry
        assert resolved.entry.artist_name == "Juana Molina"
