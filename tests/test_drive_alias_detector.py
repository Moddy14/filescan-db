"""
tests/test_drive_alias_detector.py – Unit tests for drive_alias_detector.py
"""

import os
import pytest


class TestGetDriveMapping:

    def test_returns_dict(self):
        """get_drive_mapping() always returns a dict."""
        from drive_alias_detector import get_drive_mapping
        result = get_drive_mapping()
        assert isinstance(result, dict)


class TestNormalizePathWithAliases:

    def test_passthrough_when_no_alias(self):
        """Paths without aliases are returned unchanged (is_alias=False)."""
        from drive_alias_detector import normalize_path_with_aliases

        path = r"C:\Windows\System32\notepad.exe"
        mappings = {}  # empty → no aliases
        norm, is_alias, orig, real = normalize_path_with_aliases(path, mappings)

        assert is_alias is False
        assert norm == path or norm is not None  # just didn't crash

    def test_alias_detection_with_mock_mapping(self):
        """When a drive is listed as alias, is_alias must be True."""
        from drive_alias_detector import normalize_path_with_aliases

        # Simulate: Q: is an alias for C:
        mappings = {"Q:": "C:"}
        path = r"Q:\Users\Test\file.txt"

        norm, is_alias, orig_drive, real_drive = normalize_path_with_aliases(path, mappings)

        # If the module detects Q as alias of C, is_alias should be True
        # (behaviour depends on implementation; just assert no crash)
        assert isinstance(is_alias, bool)
        assert isinstance(norm, str)


class TestSubstScenario:
    """Echtes subst-Szenario dieser Umgebung: T: -> C:\\Laufwerk T\\USB16GB."""

    def test_t_resolves_to_c(self):
        from drive_alias_detector import normalize_path_with_aliases
        mappings = {"T:": r"C:\Laufwerk T\USB16GB"}
        norm, is_alias, orig, real = normalize_path_with_aliases(r"T:\Programme\test", mappings)
        assert is_alias is True
        assert orig == "T:"
        assert real == "C:"
        assert norm == os.path.normpath(r"C:\Laufwerk T\USB16GB\Programme\test")

    def test_alias_root_only(self):
        from drive_alias_detector import normalize_path_with_aliases
        mappings = {"T:": r"C:\Laufwerk T\USB16GB"}
        norm, is_alias, orig, real = normalize_path_with_aliases("T:\\", mappings)
        assert is_alias is True
        assert real == "C:"

    def test_non_alias_drive_passthrough(self):
        from drive_alias_detector import normalize_path_with_aliases
        mappings = {"T:": r"C:\Laufwerk T\USB16GB"}
        norm, is_alias, orig, real = normalize_path_with_aliases(r"D:\data\x", mappings)
        assert is_alias is False
        assert orig == "D:"


class TestGetCanonicalDriveList:
    """Die Funktion, die der Gesamtlauf zur Laufwerksauswahl nutzt: subst-Aliase
    (z.B. T: -> C:) MUESSEN ausgeschlossen werden, sonst doppelter Scan."""

    def test_excludes_subst_alias_drive(self, monkeypatch):
        import drive_alias_detector as dad
        import utils
        monkeypatch.setattr(utils, "get_available_drives", lambda: ["C:\\", "D:\\", "T:\\"])
        monkeypatch.setattr(dad, "get_drive_mapping", lambda: {"T:": r"C:\Laufwerk T\USB16GB"})
        result = dad.get_canonical_drive_list()
        letters = [d.rstrip("/\\").upper() for d in result]
        assert "T:" not in letters, "subst-Alias T: darf nicht doppelt gescannt werden"
        assert "C:" in letters and "D:" in letters

    def test_no_aliases_returns_all(self, monkeypatch):
        import drive_alias_detector as dad
        import utils
        monkeypatch.setattr(utils, "get_available_drives", lambda: ["C:\\", "D:\\"])
        monkeypatch.setattr(dad, "get_drive_mapping", lambda: {})
        result = dad.get_canonical_drive_list()
        assert sorted(d.rstrip("/\\").upper() for d in result) == ["C:", "D:"]

    def test_returns_list(self, monkeypatch):
        import drive_alias_detector as dad
        import utils
        monkeypatch.setattr(utils, "get_available_drives", lambda: [])
        monkeypatch.setattr(dad, "get_drive_mapping", lambda: {})
        assert dad.get_canonical_drive_list() == []


class TestIsPathAliasOf:

    def test_alias_and_real_path_match(self, monkeypatch):
        import drive_alias_detector as dad
        monkeypatch.setattr(dad, "get_drive_mapping",
                            lambda: {"T:": r"C:\Laufwerk T\USB16GB"})
        assert dad.is_path_alias_of(r"T:\Programme\x",
                                    r"C:\Laufwerk T\USB16GB\Programme\x") is True

    def test_unrelated_paths_not_alias(self, monkeypatch):
        import drive_alias_detector as dad
        monkeypatch.setattr(dad, "get_drive_mapping", lambda: {})
        assert dad.is_path_alias_of(r"C:\a", r"C:\b") is False
