# -*- coding: utf-8 -*-
"""
tests/test_scan_orchestration.py – Korrekte Exit-Code-Klassifikation im Gesamtlauf.

scanner_core.py liefert: 0 = Erfolg, 1 = FATALER Scan-Fehler (run_scan() -> False),
3 = konnte Scan-Lock nicht erwerben (anderer Scan laeuft bereits). Der Orchestrator
scan_all_drives.py klassifizierte Exit 1 faelschlich als "skipped" (anderer Scan) und
Exit 3 als "failed" – also genau VERTAUSCHT, sodass ein echter Hard-Fail als harmloser
Skip gemeldet wurde. classify_scan_exit_code() bildet die Codes korrekt ab.
"""
import pytest
from scan_all_drives import classify_scan_exit_code


class TestClassifyScanExitCode:

    def test_zero_is_success(self):
        assert classify_scan_exit_code(0) == "success"

    def test_three_is_skipped_concurrent_scan(self):
        # Exit 3 = scanner_core konnte Lock nicht erwerben (anderer Scan lief)
        assert classify_scan_exit_code(3) == "skipped"

    def test_one_is_failed_not_skipped(self):
        # Exit 1 = run_scan() -> False = FATALER Fehler, NICHT skipped
        assert classify_scan_exit_code(1) == "failed"

    @pytest.mark.parametrize("code", [2, 127, -1, 255])
    def test_other_nonzero_is_failed(self, code):
        assert classify_scan_exit_code(code) == "failed"
