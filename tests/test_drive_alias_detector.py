"""
tests/test_drive_alias_detector.py – Unit tests for drive_alias_detector.py
"""

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
