"""Structural regression tests for pipeline data models.

`FlowsheetEntry` and `ResolvedEntry` are instantiated ~1M times each during a
nightly sync. `LibraryCode` (~462K) and `LibraryRelease` (~196K) are the next
highest-cardinality types and stay resident through resolver construction and
graph_metrics. All MUST be slotted dataclasses (no per-instance `__dict__`)
to fit the production cgroup memory cap.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from semantic_index.models import FlowsheetEntry, LibraryCode, LibraryRelease, ResolvedEntry
from tests.conftest import (
    make_flowsheet_entry,
    make_library_code,
    make_library_release,
    make_resolved_entry,
)


class TestFlowsheetEntrySlots:
    def test_instance_has_no_dict(self):
        # Pydantic v2 BaseModel declares __slots__ that includes '__dict__',
        # so `hasattr(cls, '__slots__')` is not a useful check. The real
        # signal is whether instances allow __dict__ allocation.
        entry = make_flowsheet_entry()
        assert not hasattr(entry, "__dict__"), (
            "FlowsheetEntry instances must NOT have __dict__ "
            "(per-instance dict allocation defeats slot savings)"
        )

    def test_slots_excludes_dict(self):
        assert "__dict__" not in FlowsheetEntry.__slots__, (
            "FlowsheetEntry.__slots__ must not list '__dict__' "
            "(would re-enable per-instance dict allocation)"
        )

    def test_is_frozen(self):
        entry = make_flowsheet_entry()
        with pytest.raises(FrozenInstanceError):
            entry.artist_name = "Stereolab"

    def test_construction_with_defaults(self):
        entry = make_flowsheet_entry()
        assert entry.request_flag == 0
        assert entry.start_time is None

    def test_construction_with_all_fields(self):
        entry = make_flowsheet_entry(request_flag=1, start_time=1_700_000_000)
        assert entry.request_flag == 1
        assert entry.start_time == 1_700_000_000

    def test_attribute_access_preserved(self):
        entry = make_flowsheet_entry()
        assert entry.id == 1
        assert entry.artist_name == "Autechre"
        assert entry.song_title == "VI Scose Poise"
        assert entry.release_title == "Confield"
        assert entry.library_release_id == 100
        assert entry.label_name == "Warp"
        assert entry.show_id == 1
        assert entry.sequence == 1
        assert entry.entry_type_code == 1


class TestLibraryCodeSlots:
    def test_instance_has_no_dict(self):
        code = make_library_code()
        assert not hasattr(code, "__dict__"), (
            "LibraryCode instances must NOT have __dict__ -- ~462K coexist at "
            "sync peak and stay resident through resolver + graph_metrics"
        )

    def test_slots_excludes_dict(self):
        assert "__dict__" not in LibraryCode.__slots__

    def test_is_frozen(self):
        code = make_library_code()
        with pytest.raises(FrozenInstanceError):
            code.presentation_name = "Stereolab"

    def test_attribute_access_preserved(self):
        code = make_library_code()
        assert code.id == 200
        assert code.genre_id == 15
        assert code.presentation_name == "Autechre"

    def test_equality_and_hashable(self):
        # frozen dataclass: value equality + hashable, relied on by tests/sets.
        a = LibraryCode(id=1, genre_id=2, presentation_name="Autechre")
        b = LibraryCode(id=1, genre_id=2, presentation_name="Autechre")
        assert a == b
        assert {a, b} == {a}


class TestLibraryReleaseSlots:
    def test_instance_has_no_dict(self):
        release = make_library_release()
        assert not hasattr(release, "__dict__"), (
            "LibraryRelease instances must NOT have __dict__ -- ~196K coexist at sync peak"
        )

    def test_slots_excludes_dict(self):
        assert "__dict__" not in LibraryRelease.__slots__

    def test_is_frozen(self):
        release = make_library_release()
        with pytest.raises(FrozenInstanceError):
            release.library_code_id = 999

    def test_attribute_access_and_equality(self):
        release = make_library_release()
        assert release.id == 100
        assert release.library_code_id == 200
        assert release == LibraryRelease(id=100, library_code_id=200)


class TestResolvedEntrySlots:
    def test_instance_has_no_dict(self):
        resolved = make_resolved_entry()
        assert not hasattr(resolved, "__dict__")

    def test_slots_excludes_dict(self):
        assert "__dict__" not in ResolvedEntry.__slots__

    def test_is_frozen(self):
        resolved = make_resolved_entry()
        with pytest.raises(FrozenInstanceError):
            resolved.canonical_name = "Stereolab"

    def test_holds_flowsheet_entry_by_reference(self):
        entry = make_flowsheet_entry()
        resolved = ResolvedEntry(
            entry=entry, canonical_name="Autechre", resolution_method="catalog"
        )
        assert resolved.entry is entry
        assert resolved.entry.artist_name == "Autechre"
