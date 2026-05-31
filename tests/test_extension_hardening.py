# -*- coding: utf-8 -*-
"""
tests/test_extension_hardening.py – Root-Cause-Fix gegen extensions-Bloat.

Die Endung wurde bisher per os.path.splitext OHNE Validierung abgeleitet, sodass
aus Dateinamen mit mehreren Punkten Muell-"Endungen" entstanden ('.0', '.0-STABLE',
67-Zeichen-SHA-Hashes, '.luzia-UBUNTU_2,ST'). split_name_ext() wendet die
R3-Plausibilitaetsregel an: nur '.<alnum/_/-> (1..16, mind. 1 Buchstabe, nie
rein-numerisch)' gilt als echte Endung; sonst bleibt der volle Name im filename
und die Endung ist '[none]'. Die Entscheidung ist case-insensitiv, gespeichert
wird die Originalschreibweise (Konkatenations-Invariante filename+ext == basename).
"""
import pytest
from models import split_name_ext


class TestSplitNameExtKeepsRealExtensions:

    @pytest.mark.parametrize("basename,exp_name,exp_ext", [
        ("logo.png", "logo", ".png"),
        ("report.docx", "report", ".docx"),
        ("x.7z", "x", ".7z"),                       # Ziffer+Buchstabe -> echt
        ("config.properties", "config", ".properties"),
        ("module.kicad_mod", "module", ".kicad_mod"),  # Unterstrich -> echt
        ("archive.tar.gz", "archive.tar", ".gz"),   # nur letzte Endung
        ("PHOTO.JPG", "PHOTO", ".JPG"),             # Originalschreibweise bleibt
    ])
    def test_real_extension(self, basename, exp_name, exp_ext):
        assert split_name_ext(basename) == (exp_name, exp_ext)


class TestSplitNameExtRejectsJunk:

    @pytest.mark.parametrize("basename", [
        "data.0",                       # rein-numerisch
        "file.1776402205",              # rein-numerisch (Timestamp)
        "weird.luzia-UBUNTU_2,ST",      # Komma -> kein Endungs-Zeichen
        "x.0, Culture=neutral",         # Komma/Leerzeichen -> Muell
        "h.0123456789abcdef0123456789abcdef",  # 32 Zeichen -> zu lang
        "noext",                        # gar kein Punkt
    ])
    def test_junk_falls_back_to_none(self, basename):
        name, ext = split_name_ext(basename)
        assert ext == "[none]", f"{basename!r} sollte als [none] gelten"
        # Voller Name bleibt erhalten (Konkatenations-Invariante)
        assert name == basename

    def test_dotfile_has_no_extension(self):
        # os.path.splitext('.gitignore') == ('.gitignore', '') -> keine Endung
        assert split_name_ext(".gitignore") == (".gitignore", "[none]")


class TestConcatInvariant:
    """filename + (ext, falls echt) muss IMMER den Original-Basename ergeben."""

    @pytest.mark.parametrize("basename", [
        "logo.png", "archive.tar.gz", "data.0", "weird.name,with,commas",
        "PHOTO.JPG", "module.kicad_mod", "noext", ".gitignore",
    ])
    def test_roundtrip(self, basename):
        name, ext = split_name_ext(basename)
        reconstructed = name + ("" if ext == "[none]" else ext)
        assert reconstructed == basename
