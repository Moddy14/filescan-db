"""
tests/test_utils.py – Unit tests for utils.py
"""

import os
import pytest


# ---------------------------------------------------------------------------
# Test: calculate_hash
# ---------------------------------------------------------------------------

class TestCalculateHash:

    def test_hash_known_content(self, tmp_path):
        """SHA-256 of a known byte string must match the expected digest."""
        import hashlib
        from utils import calculate_hash

        content = b"Hello, filescan-db!"
        expected = hashlib.sha256(content).hexdigest()

        f = tmp_path / "test_file.bin"
        f.write_bytes(content)

        result = calculate_hash(str(f))
        assert result == expected

    def test_hash_empty_file(self, tmp_path):
        """Hashing an empty file returns the known SHA-256 of b''."""
        import hashlib
        from utils import calculate_hash

        f = tmp_path / "empty.bin"
        f.write_bytes(b"")

        expected = hashlib.sha256(b"").hexdigest()
        assert calculate_hash(str(f)) == expected

    def test_hash_missing_file_returns_none(self, tmp_path):
        """calculate_hash must return None (not raise) for a missing file."""
        from utils import calculate_hash
        result = calculate_hash(str(tmp_path / "does_not_exist.txt"))
        assert result is None

    def test_hash_large_file_chunked(self, tmp_path):
        """Hashing works correctly for files larger than one chunk (>8 KB)."""
        import hashlib
        from utils import calculate_hash

        data = os.urandom(32 * 1024)  # 32 KB
        f = tmp_path / "big.bin"
        f.write_bytes(data)

        expected = hashlib.sha256(data).hexdigest()
        assert calculate_hash(str(f)) == expected


# ---------------------------------------------------------------------------
# Test: load_config / save_config
# ---------------------------------------------------------------------------

class TestConfig:

    def test_load_config_returns_dict(self, monkeypatch, tmp_path):
        """load_config always returns a dict with default keys."""
        import json
        from utils import load_config
        import utils as _utils

        cfg_path = tmp_path / "config.json"
        cfg_path.write_text(json.dumps({"log_level": "DEBUG"}), encoding="utf-8")
        monkeypatch.setattr(_utils, "CONFIG_PATH", str(cfg_path))

        cfg = load_config()
        assert isinstance(cfg, dict)
        assert "log_level" in cfg
        assert cfg["log_level"] == "DEBUG"

    def test_load_config_missing_file_gives_defaults(self, monkeypatch, tmp_path):
        """When config.json does not exist, default values are returned."""
        import utils as _utils
        from utils import load_config
        monkeypatch.setattr(_utils, "CONFIG_PATH", str(tmp_path / "nonexistent.json"))

        cfg = load_config()
        assert isinstance(cfg, dict)
        assert "hashing" in cfg
        assert cfg["hashing"] is False

    def test_load_config_invalid_json_gives_defaults(self, monkeypatch, tmp_path):
        """Corrupted JSON file must fall back to default config silently."""
        import utils as _utils
        from utils import load_config

        bad_cfg = tmp_path / "config.json"
        bad_cfg.write_text("{invalid json", encoding="utf-8")
        monkeypatch.setattr(_utils, "CONFIG_PATH", str(bad_cfg))

        cfg = load_config()
        assert isinstance(cfg, dict)

    def test_save_config_round_trip(self, monkeypatch, tmp_path):
        """save_config → load_config round-trip preserves values."""
        import json
        import utils as _utils
        from utils import load_config, save_config

        cfg_path = tmp_path / "config.json"
        monkeypatch.setattr(_utils, "CONFIG_PATH", str(cfg_path))

        data = {"hashing": True, "log_level": "WARNING", "extra_key": 42}
        save_config(data)

        loaded = load_config()
        assert loaded["hashing"] is True
        assert loaded["log_level"] == "WARNING"
        assert loaded["extra_key"] == 42


# ---------------------------------------------------------------------------
# Test: get_available_drives
# ---------------------------------------------------------------------------

class TestGetAvailableDrives:

    def test_returns_list(self):
        """get_available_drives always returns a list."""
        from utils import get_available_drives
        drives = get_available_drives()
        assert isinstance(drives, list)

    def test_c_drive_present_on_windows(self):
        """On a Windows host, C:\\ should be available."""
        import platform
        from utils import get_available_drives

        if platform.system() != "Windows":
            pytest.skip("Skipping Windows-only test")

        drives = get_available_drives()
        assert any(d.upper().startswith("C:") for d in drives)
