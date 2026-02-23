#!/usr/bin/env python3
"""
Space Recovery Advisor v3 - Ultimative Systembereinigung
Dual-Pane-Vergleich mit voll-responsivem Layout und verschiebbaren Splittern.
"""

import sys, os, sqlite3, subprocess, time, shutil, logging
from pathlib import Path
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QTreeWidget, QTreeWidgetItem, QLabel, QPushButton,
    QProgressBar, QHeaderView, QMessageBox, QSpinBox, QMenu,
    QAbstractItemView, QToolButton, QSizePolicy, QDialog,
    QDialogButtonBox, QRadioButton, QButtonGroup, QLineEdit,
    QFileDialog, QTextEdit
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from PyQt6.QtGui import QFont, QColor, QBrush

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"space_recovery_{time.strftime('%Y%m%d_%H%M%S')}.log")

log = logging.getLogger("SpaceRecovery")
log.setLevel(logging.DEBUG)
# File handler — ALLES protokollieren
_fh = logging.FileHandler(LOG_FILE, encoding='utf-8')
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s"))
log.addHandler(_fh)
# Console handler — nur Warnungen
_ch = logging.StreamHandler()
_ch.setLevel(logging.WARNING)
_ch.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
log.addHandler(_ch)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Dateien.db")

# Farben
CLR_IDENTICAL   = QColor(220, 60, 60)
CLR_ONLY_LEFT   = QColor(80, 200, 100)
CLR_ONLY_RIGHT  = QColor(255, 165, 50)
CLR_BG_IDENT    = QColor(45, 20, 20)
CLR_BG_ONLY_L   = QColor(20, 45, 20)
CLR_BG_ONLY_R   = QColor(50, 35, 15)
CLR_BG_EMPTY    = QColor(25, 25, 35)


def human_size(b):
    if not b: return "0 B"
    for u in ['B','KB','MB','GB','TB']:
        if abs(b) < 1024: return f"{b:.1f} {u}"
        b /= 1024
    return f"{b:.1f} PB"

def db_path_to_win(path):
    w = path.replace("/", "\\")
    if len(w) > 4 and w[1] == ':' and '\\' in w[2:]:
        r = w[3:]
        if len(r) > 1 and r[1] == ':': w = r
    return w


# ═══════════════════════════════════════════════════════════
#  Background Threads
# ═══════════════════════════════════════════════════════════

class ScanThread(QThread):
    progress = pyqtSignal(str, int)
    folder_pairs = pyqtSignal(list)
    error = pyqtSignal(str)

    def __init__(self, db_path, min_shared_mb, min_file_mb):
        super().__init__()
        self.db_path = db_path
        self.min_shared = min_shared_mb * 1024 * 1024
        self.min_file = min_file_mb * 1024 * 1024

    def run(self):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA cache_size=-200000")
            c = conn.cursor()

            self.progress.emit("Phase 1/3: Duplikat-Gruppen...", 10)
            c.execute("DROP TABLE IF EXISTS temp_dup_sigs")
            c.execute("""CREATE TEMP TABLE temp_dup_sigs AS
                SELECT filename, extension_id, size FROM files
                WHERE size > ? GROUP BY filename, extension_id, size
                HAVING COUNT(*) > 1""", (self.min_file,))
            n = c.execute("SELECT COUNT(*) FROM temp_dup_sigs").fetchone()[0]
            self.progress.emit(f"Phase 1: {n:,} Gruppen", 25)
            c.execute("CREATE INDEX temp.idx_ds ON temp_dup_sigs(filename, extension_id, size)")

            self.progress.emit("Phase 2/3: Ordner-Statistiken...", 30)
            c.execute("SELECT directory_id, COUNT(*), SUM(size) FROM files WHERE size>0 GROUP BY directory_id")
            ds = {r[0]: (r[1], r[2]) for r in c.fetchall()}

            self.progress.emit("Phase 3/3: Ordner-Paare (1-2 Min)...", 40)
            c.execute("""SELECT f1.directory_id, f2.directory_id, COUNT(*), SUM(f1.size)
                FROM files f1
                JOIN temp_dup_sigs d ON f1.filename=d.filename AND f1.extension_id=d.extension_id AND f1.size=d.size
                JOIN files f2 ON f1.filename=f2.filename AND f1.extension_id=f2.extension_id
                    AND f1.size=f2.size AND f1.directory_id < f2.directory_id
                GROUP BY f1.directory_id, f2.directory_id
                HAVING SUM(f1.size) > ? ORDER BY SUM(f1.size) DESC LIMIT 200""",
                (self.min_shared,))
            raw = c.fetchall()

            self.progress.emit(f"Lade Pfade ({len(raw)} Paare)...", 85)
            ids = set()
            for r in raw: ids.add(r[0]); ids.add(r[1])
            dp = {}
            if ids:
                ph = ','.join('?'*len(ids))
                c.execute(f"SELECT d.id, d.full_path, dr.name FROM directories d JOIN drives dr ON d.drive_id=dr.id WHERE d.id IN ({ph})", list(ids))
                for r in c.fetchall(): dp[r[0]] = (r[1], r[2])

            res = []
            for d1, d2, sh, sb in raw:
                p1, v1 = dp.get(d1, ("???","?")); p2, v2 = dp.get(d2, ("???","?"))
                t1, b1 = ds.get(d1, (0,0)); t2, b2 = ds.get(d2, (0,0))
                sm = min(t1, t2) or 1; ov = min(round(sh*100.0/sm), 100)
                tp = "BACKUP" if ov >= 95 else ("HOCH" if ov >= 70 else "TEIL")
                res.append(dict(dir1_id=d1, dir2_id=d2, path1=f"{v1}{p1}", path2=f"{v2}{p2}",
                    shared_files=sh, shared_bytes=sb, total1=t1, total2=t2,
                    bytes1=b1 or 0, bytes2=b2 or 0, overlap_pct=ov, typ=tp))
            conn.close()
            self.progress.emit(f"Fertig! {len(res)} Paare.", 100)
            self.folder_pairs.emit(res)
        except Exception as e:
            import traceback; self.error.emit(f"{e}\n{traceback.format_exc()}")


class CompareThread(QThread):
    result = pyqtSignal(dict)
    def __init__(self, db_path, d1, d2):
        super().__init__(); self.db_path=db_path; self.d1=d1; self.d2=d2
    def run(self):
        try:
            conn = sqlite3.connect(self.db_path); c = conn.cursor()
            def gf(did):
                c.execute("""SELECT f.id, f.filename,
                    CASE WHEN e.name IS NULL OR e.name='[none]' THEN '' ELSE e.name END,
                    f.size, f.modified_date
                    FROM files f LEFT JOIN extensions e ON f.extension_id=e.id
                    WHERE f.directory_id=? ORDER BY f.size DESC""", (did,))
                return c.fetchall()
            f1, f2 = gf(self.d1), gf(self.d2)
            m2 = {}
            for fid, n, e, s, d in f2: m2[(n,e,s)] = (fid,n,e,s,d)
            matched, only1, rem = [], [], dict(m2)
            for fid, n, e, s, d in f1:
                k = (n,e,s)
                if k in m2:
                    v = m2[k]
                    matched.append(dict(name=f"{n}{e}", size=s, date1=d or '', date2=v[4] or ''))
                    rem.pop(k, None)
                else:
                    only1.append(dict(name=f"{n}{e}", size=s, date=d or ''))
            only2 = [dict(name=f"{v[1]}{v[2]}", size=v[3], date=v[4] or '') for v in rem.values()]
            conn.close()
            self.result.emit(dict(
                matched=sorted(matched, key=lambda x: x['size'], reverse=True),
                only1=sorted(only1, key=lambda x: x['size'], reverse=True),
                only2=sorted(only2, key=lambda x: x['size'], reverse=True),
                total_matched=sum(m['size'] for m in matched),
                total_only1=sum(f['size'] for f in only1),
                total_only2=sum(f['size'] for f in only2)))
        except Exception as e:
            self.result.emit(dict(error=str(e)))


def check_disk_space(target_path, needed_bytes):
    """Prüft ob genug Platz am Ziel ist. Returns (ok, free_bytes, message)."""
    try:
        drive = os.path.splitdrive(target_path)[0]
        if not drive:
            drive = target_path[:3]  # z.B. "N:\"
        usage = shutil.disk_usage(drive + '\\')
        free = usage.free
        margin = needed_bytes * 1.05  # 5% Sicherheitsmarge
        if free >= margin:
            return True, free, f"Platz OK: {human_size(free)} frei, {human_size(needed_bytes)} benoetigt"
        else:
            return False, free, (
                f"NICHT GENUG PLATZ!\n\n"
                f"Ziel: {drive}\n"
                f"Frei: {human_size(free)}\n"
                f"Benoetigt: {human_size(needed_bytes)} (+5% Marge)\n\n"
                f"Bitte Platz schaffen oder anderen Ordner waehlen."
            )
    except Exception as e:
        return True, 0, f"Speicherplatz-Check fehlgeschlagen: {e} (fahre trotzdem fort)"


class DeleteThread(QThread):
    """Sicherer Lösch-Thread mit Live-Logging."""
    progress = pyqtSignal(str, int, int)
    log_line = pyqtSignal(str)            # Live-Log fürs GUI-Panel
    finished_ok = pyqtSignal(dict)
    error = pyqtSignal(str)

    def __init__(self, db_path, dir_id, dir_path, mode='folder', matched_files=None, backup_root=None):
        super().__init__()
        self.db_path = db_path
        self.dir_id = dir_id
        self.dir_path = dir_path  # DB-format path (e.g. "D:/D:/Daten/...")
        self.mode = mode
        self.matched_files = matched_files or []
        self.backup_root = backup_root  # z.B. "N:\BACKUP" → None = kein Backup

    def _log(self, msg):
        log.info(f"[DELETE] {msg}")
        self.log_line.emit(msg)

    def run(self):
        try:
            win_base = db_path_to_win(self.dir_path)
            self._log(f"START: {win_base} (mode={self.mode})")
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=10000")
            c = conn.cursor()

            if self.mode == 'duplicates':
                targets = self.matched_files
            else:
                c.execute("""
                    SELECT f.id, f.filename,
                           CASE WHEN e.name IS NULL OR e.name='[none]' THEN '' ELSE e.name END,
                           f.size
                    FROM files f LEFT JOIN extensions e ON f.extension_id = e.id
                    WHERE f.directory_id = ?
                """, (self.dir_id,))
                targets = [{'name': f"{r[1]}{r[2]}", 'size': r[3], 'file_id': r[0]}
                           for r in c.fetchall()]

            total = len(targets)
            self._log(f"Ziel: {total} Dateien")
            if total == 0:
                self.finished_ok.emit({'deleted': 0, 'failed': 0, 'db_cleaned': 0,
                                       'bytes_freed': 0, 'errors': []})
                conn.close()
                return

            deleted = 0
            failed = 0
            db_cleaned = 0
            bytes_freed = 0
            backed_up = 0
            errors = []

            # Backup-Pfad berechnen: D:\FOTO\2025 → N:\BACKUP\FOTO\2025
            backup_dir = None
            if self.backup_root:
                # KRITISCH: Prüfe ob Backup-Laufwerk überhaupt erreichbar ist!
                backup_drive = os.path.splitdrive(self.backup_root)[0]
                if backup_drive and not os.path.exists(backup_drive + '\\'):
                    self._log(f"ABBRUCH: Backup-Laufwerk {backup_drive} nicht erreichbar!")
                    self.error.emit(
                        f"Backup-Laufwerk {backup_drive} ist nicht erreichbar!\n\n"
                        f"Bitte Laufwerk anschliessen und erneut versuchen.\n"
                        f"Ohne Backup wird NICHT geloescht."
                    )
                    conn.close()
                    return

                # win_base = "D:\Daten\Fotos\2025"
                # → Laufwerk entfernen: "Daten\Fotos\2025"
                # → Backup: "N:\BACKUP\Daten\Fotos\2025"
                drive_part = os.path.splitdrive(win_base)  # ("D:", "\Daten\Fotos\2025")
                if drive_part[0]:
                    # Laufwerk-Buchstabe als Ordner einbauen für Eindeutigkeit
                    drive_letter = drive_part[0][0]  # "D"
                    rel_path = drive_part[1].lstrip("\\")  # "Daten\Fotos\2025"
                    backup_dir = os.path.join(self.backup_root, drive_letter, rel_path)
                else:
                    backup_dir = os.path.join(self.backup_root, win_base.lstrip("\\"))

                try:
                    os.makedirs(backup_dir, exist_ok=True)
                    self._log(f"Backup-Ordner: {backup_dir}")
                except Exception as e:
                    self._log(f"ABBRUCH: Backup-Ordner kann nicht erstellt werden: {backup_dir}: {e}")
                    self.error.emit(
                        f"Backup-Ordner kann nicht erstellt werden!\n\n"
                        f"Pfad: {backup_dir}\n"
                        f"Fehler: {e}\n\n"
                        f"Ohne Backup wird NICHT geloescht."
                    )
                    conn.close()
                    return

            # 2) Datei für Datei: [backup →] löschen → prüfen → DB updaten
            for i, f in enumerate(targets):
                fname = f['name']
                fsize = f.get('size', 0)
                full_path = os.path.join(win_base, fname)

                self.progress.emit(f"Loesche: {fname}", i + 1, total)

                # --- 2a) BACKUP (vor dem Löschen!) ---
                if backup_dir and os.path.exists(full_path):
                    try:
                        bkp_path = os.path.join(backup_dir, fname)
                        if not os.path.exists(bkp_path):
                            shutil.copy2(full_path, bkp_path)
                            if os.path.exists(bkp_path) and os.path.getsize(bkp_path) == fsize:
                                backed_up += 1
                                self._log(f"Backup OK: {fname}")
                            else:
                                errors.append(f"Backup-Verify fehl: {fname}")
                                self._log(f"WARN Backup verify: {fname}")
                        else:
                            backed_up += 1
                            self._log(f"Backup existiert: {fname}")
                    except Exception as e:
                        errors.append(f"Backup-Fehler: {fname}: {e}")
                        self._log(f"FEHLER Backup: {fname}: {e}")

                # --- 2b) Datei löschen (robust, crash-sicher) ---
                file_gone = False
                try:
                    if os.path.exists(full_path):
                        delete_ok = False
                        # Stufe 1: os.remove (schnell, zuverlässig, kein COM)
                        # Wenn Backup vorhanden → os.remove ist sicher
                        # Ohne Backup → Papierkorb bevorzugt
                        if backup_dir and backed_up > 0:
                            try:
                                os.remove(full_path)
                                delete_ok = True
                                self._log(f"Geloescht (Backup vorhanden): {fname}")
                            except Exception:
                                pass  # Fallback zu Papierkorb

                        # Stufe 2: PowerShell RecycleBin (kein COM-Leak!)
                        if not delete_ok:
                            try:
                                ps_path = full_path.replace("'", "''")
                                ps_cmd = (
                                    "Add-Type -AssemblyName Microsoft.VisualBasic; "
                                    "[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile("
                                    f"'{ps_path}',"
                                    "[Microsoft.VisualBasic.FileIO.UIOption]::OnlyErrorDialogs,"
                                    "[Microsoft.VisualBasic.FileIO.RecycleOption]::SendToRecycleBin)"
                                )
                                r = subprocess.run(
                                    ['powershell', '-NoProfile', '-Command', ps_cmd],
                                    capture_output=True, timeout=30,
                                    encoding='utf-8', errors='replace'
                                )
                                if r.returncode == 0:
                                    delete_ok = True
                                    self._log(f"Papierkorb (PS): {fname}")
                                else:
                                    self._log(f"PS fehl: {fname}: {r.stderr[:100]}")
                            except subprocess.TimeoutExpired:
                                self._log(f"PS timeout: {fname}")
                            except Exception as e2:
                                self._log(f"PS fehler: {fname}: {str(e2)[:100]}")

                        # Stufe 3: send2trash (COM — nur als letzter Ausweg)
                        if not delete_ok:
                            try:
                                from send2trash import send2trash as s2t
                                s2t(full_path)
                                delete_ok = True
                                self._log(f"Papierkorb (COM): {fname}")
                            except Exception as e3:
                                self._log(f"send2trash fehl: {fname}: {str(e3)[:100]}")

                        # Stufe 4: os.remove (absolut letzter Ausweg)
                        if not delete_ok:
                            try:
                                os.remove(full_path)
                                delete_ok = True
                                self._log(f"os.remove (letzte Stufe): {fname}")
                            except Exception as e4:
                                self._log(f"ALLE METHODEN FEHLGESCHLAGEN: {fname}")

                    # --- 2c) Verifizieren ---
                    time.sleep(0.05)
                    if not os.path.exists(full_path):
                        file_gone = True
                        deleted += 1
                        bytes_freed += fsize
                    else:
                        time.sleep(0.3)
                        if not os.path.exists(full_path):
                            file_gone = True
                            deleted += 1
                            bytes_freed += fsize
                        else:
                            failed += 1
                            errors.append(f"Existiert noch: {fname}")
                            self._log(f"EXISTIERT NOCH: {fname}")
                except PermissionError as e:
                    failed += 1
                    errors.append(f"Zugriff verweigert: {fname}")
                    self._log(f"ZUGRIFF VERWEIGERT: {fname}")
                except Exception as e:
                    failed += 1
                    errors.append(f"{fname}: {e}")
                    self._log(f"FEHLER: {fname}: {e}")

                # --- 2c) DB updaten (nur wenn Datei wirklich weg!) ---
                if file_gone:
                    try:
                        # Exakte Datei per directory_id + filename + extension finden und löschen
                        # Splitten: "photo.jpg" → filename="photo", ext=".jpg"
                        base, ext = os.path.splitext(fname)
                        if not ext:
                            ext = '[none]'

                        c.execute("""
                            DELETE FROM files
                            WHERE directory_id = ?
                              AND filename = ?
                              AND extension_id = (
                                  SELECT id FROM extensions WHERE name = ?
                              )
                        """, (self.dir_id, base, ext))

                        if c.rowcount > 0:
                            db_cleaned += c.rowcount
                            self._log(f"DB: {c.rowcount} Eintraege entfernt fuer {fname}")
                        else:
                            # Fallback: Vielleicht multi-dot Problem
                            # Suche mit LIKE
                            c.execute("""
                                DELETE FROM files
                                WHERE directory_id = ?
                                  AND (filename || (SELECT name FROM extensions WHERE id=extension_id)) = ?
                            """, (self.dir_id, fname))
                            if c.rowcount > 0:
                                db_cleaned += c.rowcount
                                self._log(f"DB (fallback): {c.rowcount} entfernt fuer {fname}")
                            else:
                                self._log(f"DB: Kein Eintrag gefunden fuer {fname} in dir {self.dir_id}")
                    except Exception as e:
                        self._log(f"DB-Update Fehler fuer {fname}: {e}")
                        errors.append(f"DB-Fehler: {fname}: {e}")

                # Commit alle 50 Dateien (Performance vs Sicherheit)
                if file_gone and (i % 50 == 0):
                    conn.commit()

            # 3) Finaler Commit
            conn.commit()

            # 4) Prüfe ob der Ordner jetzt leer ist → ggf. Ordner auch löschen
            if self.mode == 'folder':
                try:
                    if os.path.isdir(win_base) and not os.listdir(win_base):
                        os.rmdir(win_base)
                        self._log(f"Leerer Ordner entfernt: {win_base}")
                        # Directory-Eintrag aus DB löschen
                        c.execute("DELETE FROM directories WHERE id = ?", (self.dir_id,))
                        conn.commit()
                        self._log(f"DB: Directory {self.dir_id} entfernt")
                except Exception as e:
                    self._log(f"Ordner-Cleanup: {e}")

            conn.close()

            self.finished_ok.emit({
                'deleted': deleted,
                'failed': failed,
                'db_cleaned': db_cleaned,
                'bytes_freed': bytes_freed,
                'backed_up': backed_up,
                'backup_dir': backup_dir or '',
                'errors': errors[:20],
                'total': total
            })

        except Exception as e:
            import traceback
            self.error.emit(f"{e}\n{traceback.format_exc()}")


# ═══════════════════════════════════════════════════════════
#  Delete Confirmation Dialog
# ═══════════════════════════════════════════════════════════

class DeleteDialog(QDialog):
    """Sicherheits-Dialog mit Modus-Auswahl: Nur Duplikate vs. ganzer Ordner."""

    def __init__(self, parent, path, overlap, matched_count, unique_count, matched_size, unique_size, backup_root=None):
        super().__init__(parent)
        self.setWindowTitle("Sicher Loeschen")
        self.setMinimumWidth(550)
        self.setStyleSheet("""
            QDialog { background-color: #1e1e2e; }
            QLabel { color: #cdd6f4; }
            QRadioButton { color: #cdd6f4; font-size: 10pt; padding: 6px; }
            QRadioButton:checked { color: #f38ba8; }
            QTextEdit { background-color: #181825; color: #cdd6f4; border: 1px solid #45475a;
                        font-family: Consolas; font-size: 9pt; }
            QPushButton { background-color: #313244; color: #cdd6f4; border: 1px solid #45475a;
                          border-radius: 3px; padding: 6px 16px; font-size: 9pt; }
            QPushButton:hover { background-color: #45475a; }
        """)

        layout = QVBoxLayout(self)

        # Info
        info = QLabel(f"Ordner: {path}\nOverlap: {overlap}%")
        info.setStyleSheet("font-weight: bold; font-size: 10pt; color: #fab387;")
        info.setWordWrap(True)
        layout.addWidget(info)

        # Modus-Auswahl
        self.mode_group = QButtonGroup(self)

        self.rb_dupes = QRadioButton(
            f"Nur Duplikate loeschen ({matched_count} Dateien, {human_size(matched_size)})\n"
            f"   Einzigartige Dateien bleiben erhalten!"
        )
        self.rb_dupes.setChecked(True)
        self.mode_group.addButton(self.rb_dupes)
        layout.addWidget(self.rb_dupes)

        self.rb_folder = QRadioButton(
            f"Ganzen Ordner loeschen ({matched_count + unique_count} Dateien)\n"
            f"   WARNUNG: {unique_count} einzigartige Dateien ({human_size(unique_size)}) gehen verloren!"
        )
        if unique_count > 0:
            self.rb_folder.setStyleSheet("QRadioButton { color: #f38ba8; }")
        self.mode_group.addButton(self.rb_folder)
        layout.addWidget(self.rb_folder)

        # Backup-Info
        if backup_root:
            bkp = QLabel(f"Sicherheitsnetz: Backup nach {backup_root}\\...")
            bkp.setStyleSheet("color: #89b4fa; font-weight: bold; font-size: 9pt; padding: 4px;")
            bkp.setWordWrap(True)
            layout.addWidget(bkp)

        # Bestätigungs-Info
        steps = "Ablauf:\n"
        if backup_root:
            steps += "1. Backup jeder Datei (Ordnerstruktur beibehalten)\n"
            steps += "2. Backup verifizieren (Groesse pruefen)\n"
            steps += "3. Datei in Papierkorb verschieben\n"
            steps += "4. Pruefen ob Datei wirklich weg ist\n"
            steps += "5. Nur verifizierte Loeschungen aus DB entfernen\n"
            steps += "6. Zusammenfassung anzeigen"
        else:
            steps += "1. Datei in Papierkorb verschieben\n"
            steps += "2. Pruefen ob Datei wirklich weg ist\n"
            steps += "3. Nur verifizierte Loeschungen aus DB entfernen\n"
            steps += "4. Zusammenfassung anzeigen\n"
            steps += "\nHINWEIS: Kein Backup konfiguriert! Nur Papierkorb."
        warn = QLabel(steps)
        warn.setStyleSheet("color: #a6adc8; font-size: 9pt; padding: 8px;")
        layout.addWidget(warn)

        # Buttons
        btns = QDialogButtonBox()
        self.btn_ok = btns.addButton("LOESCHEN", QDialogButtonBox.ButtonRole.AcceptRole)
        self.btn_ok.setStyleSheet("QPushButton { background-color: #d20f39; color: white; font-weight: bold; }")
        btns.addButton("Abbrechen", QDialogButtonBox.ButtonRole.RejectRole)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    @property
    def mode(self):
        return 'duplicates' if self.rb_dupes.isChecked() else 'folder'


# ═══════════════════════════════════════════════════════════
#  Merge Thread — Einzigartige Dateien rüberkopieren
# ═══════════════════════════════════════════════════════════

class MergeThread(QThread):
    """Kopiert einzigartige Dateien von source_dir nach target_dir."""
    progress = pyqtSignal(str, int, int)
    log_line = pyqtSignal(str)
    finished_ok = pyqtSignal(dict)
    error = pyqtSignal(str)

    def __init__(self, db_path, source_dir_id, source_path, target_dir_id, target_path, files_to_copy):
        super().__init__()
        self.db_path = db_path
        self.src_dir_id = source_dir_id
        self.src_path = source_path
        self.tgt_dir_id = target_dir_id
        self.tgt_path = target_path
        self.files = files_to_copy  # [{'name': 'photo.jpg', 'size': 12345}, ...]

    def _log(self, msg):
        log.info(f"[MERGE] {msg}")
        self.log_line.emit(msg)

    def run(self):
        try:
            src_win = db_path_to_win(self.src_path)
            tgt_win = db_path_to_win(self.tgt_path)
            self._log(f"START: {src_win} -> {tgt_win} ({len(self.files)} Dateien)")

            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=10000")
            c = conn.cursor()

            total = len(self.files)
            copied = 0
            skipped = 0
            failed = 0
            db_added = 0
            bytes_copied = 0
            errors = []

            for i, f in enumerate(self.files):
                fname = f['name']
                fsize = f.get('size', 0)
                src_full = os.path.join(src_win, fname)
                tgt_full = os.path.join(tgt_win, fname)

                self.progress.emit(f"Kopiere: {fname}", i + 1, total)

                # Schon vorhanden? (Sollte nicht sein, aber sicher ist sicher)
                if os.path.exists(tgt_full):
                    existing_size = os.path.getsize(tgt_full)
                    if existing_size == fsize:
                        skipped += 1
                        self._log(f"Uebersprungen (existiert): {fname}")
                        continue
                    else:
                        # Andere Größe = anderer Inhalt → umbenennen
                        base, ext = os.path.splitext(fname)
                        tgt_full = os.path.join(tgt_win, f"{base}_merged{ext}")
                        fname = f"{base}_merged{ext}"
                        self._log(f"Namenskonflikt: {f['name']} → {fname}")

                # Kopieren
                try:
                    if not os.path.exists(src_full):
                        failed += 1
                        errors.append(f"Quelle fehlt: {fname}")
                        continue

                    shutil.copy2(src_full, tgt_full)

                    # Verifizieren
                    time.sleep(0.02)
                    if os.path.exists(tgt_full):
                        tgt_size = os.path.getsize(tgt_full)
                        if tgt_size == fsize:
                            copied += 1
                            bytes_copied += fsize
                        else:
                            # Größe stimmt nicht → Warnung aber als Erfolg zählen
                            copied += 1
                            bytes_copied += tgt_size
                            self._log(f"Groessen-Diff: {fname} src={fsize} tgt={tgt_size}")
                    else:
                        failed += 1
                        errors.append(f"Kopie nicht verifiziert: {fname}")
                        continue

                    # DB-Eintrag anlegen
                    try:
                        base, ext = os.path.splitext(fname)
                        if not ext:
                            ext = '[none]'

                        # Extension-ID holen
                        c.execute("SELECT id FROM extensions WHERE name = ?", (ext,))
                        row = c.fetchone()
                        if row:
                            ext_id = row[0]
                        else:
                            c.execute("INSERT INTO extensions (name) VALUES (?)", (ext,))
                            ext_id = c.lastrowid

                        # Modified date von der kopierten Datei
                        try:
                            mdate = os.path.getmtime(tgt_full)
                            mdate_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mdate))
                        except:
                            mdate_str = None

                        # INSERT (bei Conflict → ignorieren, UNIQUE constraint)
                        c.execute("""
                            INSERT OR IGNORE INTO files
                                (directory_id, filename, extension_id, size, modified_date)
                            VALUES (?, ?, ?, ?, ?)
                        """, (self.tgt_dir_id, base, ext_id, fsize, mdate_str))

                        if c.rowcount > 0:
                            db_added += 1
                            self._log(f"DB: Eintrag angelegt fuer {fname} in dir {self.tgt_dir_id}")
                        else:
                            self._log(f"DB: INSERT IGNORE fuer {fname} (existiert bereits?)")

                    except Exception as e:
                        self._log(f"DB-Insert Fehler fuer {fname}: {e}")
                        errors.append(f"DB-Fehler: {fname}: {e}")

                except PermissionError as e:
                    failed += 1
                    errors.append(f"Zugriff verweigert: {fname}")
                except Exception as e:
                    failed += 1
                    errors.append(f"{fname}: {e}")

                # Commit alle 50 Dateien
                if (i % 50 == 0):
                    conn.commit()

            conn.commit()
            conn.close()

            self.finished_ok.emit({
                'copied': copied,
                'skipped': skipped,
                'failed': failed,
                'db_added': db_added,
                'bytes_copied': bytes_copied,
                'errors': errors[:20],
                'total': total
            })

        except Exception as e:
            import traceback
            self.error.emit(f"{e}\n{traceback.format_exc()}")


# ═══════════════════════════════════════════════════════════
#  Main Window
# ═══════════════════════════════════════════════════════════

STYLE = """
QMainWindow { background-color: #1e1e2e; }
QLabel { color: #cdd6f4; }
QTreeWidget {
    background-color: #181825; color: #cdd6f4; border: 1px solid #45475a;
    font-family: 'Consolas', monospace; font-size: 9pt;
}
QTreeWidget::item { padding: 2px 0; }
QTreeWidget::item:selected { background-color: #313244; }
QTreeWidget::item:hover { background-color: #262637; }
QHeaderView::section {
    background-color: #313244; color: #cdd6f4; padding: 4px 6px;
    border: none; border-right: 1px solid #45475a; border-bottom: 1px solid #45475a;
    font-weight: bold; font-size: 8pt;
}
QPushButton {
    background-color: #313244; color: #cdd6f4; border: 1px solid #45475a;
    border-radius: 3px; padding: 5px 12px; font-size: 9pt;
}
QPushButton:hover { background-color: #45475a; }
QPushButton:disabled { background-color: #1e1e2e; color: #585b70; }
QProgressBar {
    border: 1px solid #45475a; border-radius: 3px; text-align: center;
    color: white; background-color: #181825; max-height: 16px;
}
QProgressBar::chunk { background-color: #f38ba8; border-radius: 2px; }
QSpinBox {
    background-color: #181825; color: #cdd6f4; border: 1px solid #45475a;
    border-radius: 3px; padding: 2px 4px;
}
QToolButton {
    background-color: #313244; color: #585b70; border: 1px solid #45475a;
    border-radius: 3px; padding: 3px 8px; font-size: 9pt;
}
QToolButton:checked { color: #cdd6f4; border-color: #89b4fa; background-color: #45475a; }
QToolButton:hover { background-color: #45475a; }
QStatusBar { background-color: #181825; color: #a6adc8; font-size: 8pt; }

/* === SPLITTER HANDLES — dick & sichtbar === */
QSplitter::handle {
    background-color: #585b70;
}
QSplitter::handle:vertical {
    height: 6px;
    image: none;
}
QSplitter::handle:horizontal {
    width: 6px;
    image: none;
}
QSplitter::handle:hover {
    background-color: #89b4fa;
}
QSplitter::handle:pressed {
    background-color: #f38ba8;
}
"""


class SpaceRecoveryAdvisor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.db_path = DB_PATH
        self.pairs_data = []
        self.current_pair = None
        self.compare_data = None
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("Space Recovery Advisor")
        self.setMinimumSize(1200, 700)
        self.setStyleSheet(STYLE)

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setSpacing(4)
        root.setContentsMargins(6, 6, 6, 4)

        # ── Header row (fixed height) ───────────────────────
        hdr = QHBoxLayout()
        hdr.setSpacing(8)
        t = QLabel("SPACE RECOVERY ADVISOR")
        t.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        t.setStyleSheet("color: #f38ba8;")
        hdr.addWidget(t)

        hdr.addWidget(QLabel("Min. gemeinsam:"))
        self.spin_shared = QSpinBox(); self.spin_shared.setRange(10,10000)
        self.spin_shared.setValue(50); self.spin_shared.setSuffix(" MB")
        self.spin_shared.setFixedWidth(90)
        hdr.addWidget(self.spin_shared)

        hdr.addWidget(QLabel("Min. Datei:"))
        self.spin_file = QSpinBox(); self.spin_file.setRange(0,100)
        self.spin_file.setValue(1); self.spin_file.setSuffix(" MB")
        self.spin_file.setFixedWidth(70)
        hdr.addWidget(self.spin_file)

        hdr.addStretch()
        self.total_lbl = QLabel("")
        self.total_lbl.setFont(QFont("Segoe UI", 10))
        self.total_lbl.setStyleSheet("color: #a6e3a1;")
        hdr.addWidget(self.total_lbl)

        self.scan_btn = QPushButton("SYSTEM SCANNEN")
        self.scan_btn.setStyleSheet("""
            QPushButton { background-color: #f38ba8; color: #1e1e2e; font-weight: bold;
                          font-size: 10pt; padding: 6px 22px; border: none; }
            QPushButton:hover { background-color: #f5a0b8; }
        """)
        self.scan_btn.clicked.connect(self.start_scan)
        hdr.addWidget(self.scan_btn)
        root.addLayout(hdr)

        # ── Backup-Pfad Zeile ──────────────────────────────
        bkp_row = QHBoxLayout(); bkp_row.setSpacing(6)
        bkp_lbl = QLabel("Backup-Ordner:")
        bkp_lbl.setStyleSheet("color: #89b4fa; font-size: 9pt;")
        bkp_row.addWidget(bkp_lbl)

        self.backup_edit = QLineEdit()
        self.backup_edit.setPlaceholderText("z.B. N:\\BACKUP  (leer = kein Backup, nur Papierkorb)")
        self.backup_edit.setStyleSheet("""
            QLineEdit { background-color: #181825; color: #cdd6f4; border: 1px solid #45475a;
                        border-radius: 3px; padding: 3px 6px; font-size: 9pt; }
            QLineEdit:focus { border-color: #89b4fa; }
        """)
        # Gespeicherten Wert laden
        self._bkp_cfg = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.backup_path.txt')
        if os.path.exists(self._bkp_cfg):
            try:
                self.backup_edit.setText(open(self._bkp_cfg, 'r').read().strip())
            except: pass
        bkp_row.addWidget(self.backup_edit, 1)

        btn_browse = QPushButton("...")
        btn_browse.setFixedWidth(32)
        btn_browse.setStyleSheet("QPushButton{padding:3px}")
        btn_browse.clicked.connect(self._browse_backup)
        bkp_row.addWidget(btn_browse)

        bkp_info = QLabel("Struktur: BACKUP\\Laufwerk\\Pfad\\Datei")
        bkp_info.setStyleSheet("color: #585b70; font-size: 8pt;")
        bkp_row.addWidget(bkp_info)
        root.addLayout(bkp_row)

        # Progress (hidden by default)
        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximum(100)
        self.progress_bar.setVisible(False)
        self.progress_bar.setFixedHeight(16)
        root.addWidget(self.progress_bar)

        self.progress_lbl = QLabel("")
        self.progress_lbl.setStyleSheet("color: #a6adc8; font-size: 8pt;")
        self.progress_lbl.setFixedHeight(16)
        root.addWidget(self.progress_lbl)

        # ══ MAIN VERTICAL SPLITTER: Pairs | Compare ════════
        self.v_splitter = QSplitter(Qt.Orientation.Vertical)
        self.v_splitter.setHandleWidth(6)
        self.v_splitter.setChildrenCollapsible(False)

        # ── TOP: Pairs tree ──────────────────────────────────
        self.pairs_tree = QTreeWidget()
        self.pairs_tree.setHeaderLabels([
            "Typ", "Ordner 1", "Ordner 2", "Gemeinsam", "GB", "Overlap%", "Dat.1", "Dat.2"
        ])
        self.pairs_tree.setAlternatingRowColors(True)
        self.pairs_tree.setRootIsDecorated(False)
        self.pairs_tree.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.pairs_tree.itemSelectionChanged.connect(self.on_pair_selected)
        self.pairs_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.pairs_tree.customContextMenuRequested.connect(self.show_ctx)
        h = self.pairs_tree.header()
        h.setStretchLastSection(False)
        for i in [0,3,4,5,6,7]:
            h.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        h.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        h.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.v_splitter.addWidget(self.pairs_tree)

        # ── BOTTOM: Compare area ────────────────────────────
        cmp_w = QWidget()
        cmp_l = QVBoxLayout(cmp_w)
        cmp_l.setContentsMargins(0, 0, 0, 0)
        cmp_l.setSpacing(0)  # KEIN Abstand zwischen Filter-Bar und Dual-Pane!

        # Filter bar (compact, fixed height)
        filter_w = QWidget()
        filter_w.setFixedHeight(28)
        fb = QHBoxLayout(filter_w)
        fb.setContentsMargins(4, 2, 4, 2)
        fb.setSpacing(4)
        fb.addWidget(QLabel("Anzeigen:"))

        self.btn_left = QToolButton(); self.btn_left.setText("Nur Links")
        self.btn_left.setCheckable(True); self.btn_left.setChecked(True)
        self.btn_left.setStyleSheet("QToolButton:checked { color: #a6e3a1; border-color: #a6e3a1; }")
        self.btn_left.toggled.connect(self.repopulate)
        fb.addWidget(self.btn_left)

        self.btn_ident = QToolButton(); self.btn_ident.setText("Identisch")
        self.btn_ident.setCheckable(True); self.btn_ident.setChecked(True)
        self.btn_ident.setStyleSheet("QToolButton:checked { color: #f38ba8; border-color: #f38ba8; }")
        self.btn_ident.toggled.connect(self.repopulate)
        fb.addWidget(self.btn_ident)

        self.btn_right = QToolButton(); self.btn_right.setText("Nur Rechts")
        self.btn_right.setCheckable(True); self.btn_right.setChecked(True)
        self.btn_right.setStyleSheet("QToolButton:checked { color: #fab387; border-color: #fab387; }")
        self.btn_right.toggled.connect(self.repopulate)
        fb.addWidget(self.btn_right)

        fb.addStretch()
        self.stats_lbl = QLabel("")
        self.stats_lbl.setStyleSheet("color: #a6adc8; font-size: 8pt;")
        fb.addWidget(self.stats_lbl)
        cmp_l.addWidget(filter_w)  # Fixed-height filter bar

        # ── HORIZONTAL SPLITTER: Left pane | Right pane ─────
        self.h_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.h_splitter.setHandleWidth(6)
        self.h_splitter.setChildrenCollapsible(False)

        # LEFT pane
        lw = QWidget()
        ll = QVBoxLayout(lw); ll.setContentsMargins(0,0,0,0); ll.setSpacing(2)

        lh = QHBoxLayout(); lh.setSpacing(4)
        self.lbl_p1 = QLabel("Ordner 1")
        self.lbl_p1.setStyleSheet("color: #a6e3a1; font-weight: bold; font-size: 8pt;")
        self.lbl_p1.setWordWrap(True)
        self.lbl_p1.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        lh.addWidget(self.lbl_p1)
        self.btn_e1 = QPushButton("Explorer")
        self.btn_e1.setFixedWidth(70)
        self.btn_e1.setStyleSheet("QPushButton{background:#40a02b;color:white;padding:3px 8px} QPushButton:hover{background:#4cb036}")
        self.btn_e1.clicked.connect(lambda: self.open_explorer(1))
        self.btn_e1.setEnabled(False)
        lh.addWidget(self.btn_e1)
        ll.addLayout(lh)

        self.tree_l = QTreeWidget()
        self.tree_l.setHeaderLabels(["", "Dateiname", "Groesse", "Datum"])
        self.tree_l.setRootIsDecorated(False)
        self.tree_l.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.tree_l.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        hl = self.tree_l.header()
        hl.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed); hl.resizeSection(0, 36)
        hl.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        hl.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        hl.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        ll.addWidget(self.tree_l)

        btn_row1 = QHBoxLayout(); btn_row1.setSpacing(4)
        self.btn_m1 = QPushButton("<<< Hierher mergen")
        self.btn_m1.setFixedHeight(28)
        self.btn_m1.setStyleSheet("QPushButton{background:#1e66f5;color:white;font-weight:bold} QPushButton:hover{background:#3b82f6}")
        self.btn_m1.setToolTip("Einzigartige Dateien von Ordner 2 hierher kopieren")
        self.btn_m1.clicked.connect(lambda: self.merge_to(1))
        self.btn_m1.setEnabled(False)
        btn_row1.addWidget(self.btn_m1)
        self.btn_d1 = QPushButton("LOESCHEN")
        self.btn_d1.setFixedHeight(28)
        self.btn_d1.setStyleSheet("QPushButton{background:#d20f39;color:white;font-weight:bold} QPushButton:hover{background:#e64553}")
        self.btn_d1.clicked.connect(lambda: self.delete_folder(1))
        self.btn_d1.setEnabled(False)
        btn_row1.addWidget(self.btn_d1)
        ll.addLayout(btn_row1)
        self.h_splitter.addWidget(lw)

        # RIGHT pane
        rw = QWidget()
        rl = QVBoxLayout(rw); rl.setContentsMargins(0,0,0,0); rl.setSpacing(2)

        rh = QHBoxLayout(); rh.setSpacing(4)
        self.lbl_p2 = QLabel("Ordner 2")
        self.lbl_p2.setStyleSheet("color: #fab387; font-weight: bold; font-size: 8pt;")
        self.lbl_p2.setWordWrap(True)
        self.lbl_p2.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        rh.addWidget(self.lbl_p2)
        self.btn_e2 = QPushButton("Explorer")
        self.btn_e2.setFixedWidth(70)
        self.btn_e2.setStyleSheet("QPushButton{background:#df8e1d;color:white;padding:3px 8px} QPushButton:hover{background:#e9a030}")
        self.btn_e2.clicked.connect(lambda: self.open_explorer(2))
        self.btn_e2.setEnabled(False)
        rh.addWidget(self.btn_e2)
        rl.addLayout(rh)

        self.tree_r = QTreeWidget()
        self.tree_r.setHeaderLabels(["", "Dateiname", "Groesse", "Datum"])
        self.tree_r.setRootIsDecorated(False)
        self.tree_r.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.tree_r.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        hr = self.tree_r.header()
        hr.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed); hr.resizeSection(0, 36)
        hr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        hr.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        hr.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        rl.addWidget(self.tree_r)

        btn_row2 = QHBoxLayout(); btn_row2.setSpacing(4)
        self.btn_m2 = QPushButton("Hierher mergen >>>")
        self.btn_m2.setFixedHeight(28)
        self.btn_m2.setStyleSheet("QPushButton{background:#1e66f5;color:white;font-weight:bold} QPushButton:hover{background:#3b82f6}")
        self.btn_m2.setToolTip("Einzigartige Dateien von Ordner 1 hierher kopieren")
        self.btn_m2.clicked.connect(lambda: self.merge_to(2))
        self.btn_m2.setEnabled(False)
        btn_row2.addWidget(self.btn_m2)
        self.btn_d2 = QPushButton("LOESCHEN")
        self.btn_d2.setFixedHeight(28)
        self.btn_d2.setStyleSheet("QPushButton{background:#d20f39;color:white;font-weight:bold} QPushButton:hover{background:#e64553}")
        self.btn_d2.clicked.connect(lambda: self.delete_folder(2))
        self.btn_d2.setEnabled(False)
        btn_row2.addWidget(self.btn_d2)
        rl.addLayout(btn_row2)
        self.h_splitter.addWidget(rw)

        # Sync scroll
        self.tree_l.verticalScrollBar().valueChanged.connect(self.tree_r.verticalScrollBar().setValue)
        self.tree_r.verticalScrollBar().valueChanged.connect(self.tree_l.verticalScrollBar().setValue)

        # Vertikal-Splitter innerhalb Compare: Dual-Pane | Log-Panel
        cmp_split = QSplitter(Qt.Orientation.Vertical)
        cmp_split.setHandleWidth(5)
        cmp_split.setChildrenCollapsible(True)
        cmp_split.addWidget(self.h_splitter)

        # Live-Log Panel
        log_w = QWidget()
        log_l = QVBoxLayout(log_w); log_l.setContentsMargins(0,0,0,0); log_l.setSpacing(0)
        log_hdr = QHBoxLayout()
        log_lbl = QLabel("LIVE-LOG")
        log_lbl.setStyleSheet("color: #89b4fa; font-weight: bold; font-size: 8pt; padding: 2px 4px;")
        log_hdr.addWidget(log_lbl)
        log_hdr.addStretch()
        self.log_file_lbl = QLabel(f"Log: {LOG_FILE}")
        self.log_file_lbl.setStyleSheet("color: #585b70; font-size: 7pt;")
        log_hdr.addWidget(self.log_file_lbl)
        btn_clear = QPushButton("Clear")
        btn_clear.setFixedSize(50, 20)
        btn_clear.setStyleSheet("QPushButton{padding:1px;font-size:7pt}")
        btn_clear.clicked.connect(lambda: self.log_text.clear())
        log_hdr.addWidget(btn_clear)
        log_l.addLayout(log_hdr)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setStyleSheet("""
            QTextEdit { background-color: #11111b; color: #a6adc8; border: 1px solid #45475a;
                        font-family: Consolas, monospace; font-size: 8pt; }
        """)
        log_l.addWidget(self.log_text)
        cmp_split.addWidget(log_w)
        cmp_split.setSizes([500, 120])  # Dual-Pane groesser, Log kleiner

        cmp_l.addWidget(cmp_split, 1)
        self.v_splitter.addWidget(cmp_w)

        # Stretch-Faktoren: Paare=2, Vergleich=3 (unten mehr Platz)
        self.v_splitter.setStretchFactor(0, 2)
        self.v_splitter.setStretchFactor(1, 3)

        root.addWidget(self.v_splitter, 1)

        self.statusBar().showMessage(f"Bereit. Log: {LOG_FILE}")

    def _append_log(self, msg):
        """Fügt eine Zeile zum Live-Log-Panel hinzu."""
        ts = time.strftime('%H:%M:%S')
        color = '#a6adc8'
        if 'FEHLER' in msg or 'FAIL' in msg or 'VERWEIGERT' in msg:
            color = '#f38ba8'
        elif 'OK' in msg or 'Papierkorb' in msg or 'Kopiert' in msg:
            color = '#a6e3a1'
        elif 'WARN' in msg or 'fehl' in msg.lower():
            color = '#fab387'
        self.log_text.append(f'<span style="color:#585b70">{ts}</span> <span style="color:{color}">{msg}</span>')
        # Auto-scroll to bottom
        self.log_text.verticalScrollBar().setValue(self.log_text.verticalScrollBar().maximum())

    def _browse_backup(self):
        d = QFileDialog.getExistingDirectory(self, "Backup-Ordner waehlen", self.backup_edit.text())
        if d:
            self.backup_edit.setText(d.replace('/', '\\'))

    def _get_backup_root(self):
        """Gibt den Backup-Pfad zurück oder None. Speichert den Wert."""
        txt = self.backup_edit.text().strip()
        if txt:
            # Pfad speichern für nächsten Start
            try:
                with open(self._bkp_cfg, 'w') as f: f.write(txt)
            except: pass
            return txt
        return None

    # ─── Scan ───────────────────────────────────────────────
    def start_scan(self):
        self.scan_btn.setEnabled(False)
        self.progress_bar.setVisible(True); self.progress_bar.setValue(0)
        self.pairs_tree.clear(); self.clear_cmp()
        self._st = ScanThread(self.db_path, self.spin_shared.value(), self.spin_file.value())
        self._st.progress.connect(self._prog)
        self._st.folder_pairs.connect(self._scan_ok)
        self._st.error.connect(self._scan_err)
        self._st.start()

    def _prog(self, m, p): self.progress_lbl.setText(m); self.progress_bar.setValue(p)

    def _scan_err(self, m):
        self.progress_lbl.setText(f"FEHLER"); self.scan_btn.setEnabled(True)
        QMessageBox.critical(self, "Fehler", m)

    def _scan_ok(self, pairs):
        self.pairs_data = pairs; self.scan_btn.setEnabled(True)
        self.progress_bar.setVisible(False); self.progress_lbl.setText("")
        total = sum(p['shared_bytes'] for p in pairs)
        self.total_lbl.setText(f"~{human_size(total)} wiedergewinnbar ({len(pairs)} Paare)")

        tc = {'BACKUP': QColor(243,139,168), 'HOCH': QColor(250,179,135), 'TEIL': QColor(137,180,250)}
        for p in pairs:
            it = QTreeWidgetItem([
                p['typ'], p['path1'], p['path2'], str(p['shared_files']),
                f"{p['shared_bytes']/1024**3:.1f}", f"{p['overlap_pct']}%",
                str(p['total1']), str(p['total2'])])
            it.setForeground(0, QBrush(tc.get(p['typ'], QColor(200,200,200))))
            it.setFont(0, QFont("Segoe UI", 9, QFont.Weight.Bold))
            if p['shared_bytes'] > 50*1024**3:
                for c in range(8): it.setBackground(c, QBrush(QColor(60,20,20)))
            it.setData(0, Qt.ItemDataRole.UserRole, p)
            self.pairs_tree.addTopLevelItem(it)
        self.statusBar().showMessage(f"{len(pairs)} Paare, ~{human_size(total)} wiedergewinnbar")

    # ─── Pair → Compare ────────────────────────────────────
    def on_pair_selected(self):
        sel = self.pairs_tree.selectedItems()
        if not sel: return
        p = sel[0].data(0, Qt.ItemDataRole.UserRole)
        self.current_pair = p
        self.lbl_p1.setText(p['path1']); self.lbl_p2.setText(p['path2'])
        self.btn_e1.setEnabled(True); self.btn_e2.setEnabled(True)
        self.btn_d1.setEnabled(True); self.btn_d2.setEnabled(True)
        self.btn_m1.setEnabled(True); self.btn_m2.setEnabled(True)
        self.stats_lbl.setText("Lade...")
        self.tree_l.clear(); self.tree_r.clear()
        self._ct = CompareThread(self.db_path, p['dir1_id'], p['dir2_id'])
        self._ct.result.connect(self._cmp_ok)
        self._ct.start()

    def _cmp_ok(self, d):
        if 'error' in d: self.stats_lbl.setText(f"Fehler: {d['error']}"); return
        self.compare_data = d; self.repopulate()

    def repopulate(self):
        if not self.compare_data: return
        d = self.compare_data
        self.tree_l.clear(); self.tree_r.clear()

        rows_l, rows_r = [], []

        if self.btn_ident.isChecked():
            for m in d['matched']:
                rows_l.append(('==', m['name'], m['size'], m['date1'], 'ident'))
                rows_r.append(('==', m['name'], m['size'], m['date2'], 'ident'))

        if self.btn_left.isChecked():
            for f in d['only1']:
                rows_l.append(('<<<', f['name'], f['size'], f['date'], 'left'))
                rows_r.append(('', '', 0, '', 'empty'))

        if self.btn_right.isChecked():
            for f in d['only2']:
                rows_l.append(('', '', 0, '', 'empty'))
                rows_r.append(('>>>', f['name'], f['size'], f['date'], 'right'))

        def fill(tree, rows):
            for st, nm, sz, dt, kind in rows:
                it = QTreeWidgetItem([st, nm, human_size(sz) if sz else '', str(dt)])
                if kind == 'ident':
                    it.setForeground(0, QBrush(CLR_IDENTICAL))
                    it.setForeground(1, QBrush(CLR_IDENTICAL))
                    for c in range(4): it.setBackground(c, QBrush(CLR_BG_IDENT))
                elif kind == 'left':
                    it.setForeground(0, QBrush(CLR_ONLY_LEFT))
                    it.setForeground(1, QBrush(CLR_ONLY_LEFT))
                    for c in range(4): it.setBackground(c, QBrush(CLR_BG_ONLY_L))
                elif kind == 'right':
                    it.setForeground(0, QBrush(CLR_ONLY_RIGHT))
                    it.setForeground(1, QBrush(CLR_ONLY_RIGHT))
                    for c in range(4): it.setBackground(c, QBrush(CLR_BG_ONLY_R))
                elif kind == 'empty':
                    for c in range(4): it.setBackground(c, QBrush(CLR_BG_EMPTY))
                tree.addTopLevelItem(it)

        fill(self.tree_l, rows_l)
        fill(self.tree_r, rows_r)

        self.stats_lbl.setText(
            f"Identisch: {len(d['matched'])} ({human_size(d['total_matched'])}) | "
            f"Nur links: {len(d['only1'])} ({human_size(d['total_only1'])}) | "
            f"Nur rechts: {len(d['only2'])} ({human_size(d['total_only2'])})")

    # ─── Actions ────────────────────────────────────────────
    def clear_cmp(self):
        self.tree_l.clear(); self.tree_r.clear()
        self.lbl_p1.setText("Ordner 1"); self.lbl_p2.setText("Ordner 2")
        self.stats_lbl.setText("")
        self.btn_e1.setEnabled(False); self.btn_e2.setEnabled(False)
        self.btn_d1.setEnabled(False); self.btn_d2.setEnabled(False)
        self.btn_m1.setEnabled(False); self.btn_m2.setEnabled(False)
        self.current_pair = None; self.compare_data = None

    def open_explorer(self, w):
        if not self.current_pair: return
        p = self.current_pair['path1'] if w==1 else self.current_pair['path2']
        try: subprocess.Popen(['explorer', db_path_to_win(p)])
        except Exception as e: QMessageBox.warning(self, "Fehler", str(e))

    def delete_folder(self, w):
        if not self.current_pair or not self.compare_data:
            return

        p = self.current_pair['path1'] if w == 1 else self.current_pair['path2']
        dir_id = self.current_pair['dir1_id'] if w == 1 else self.current_pair['dir2_id']
        ov = self.current_pair['overlap_pct']
        wp = db_path_to_win(p)

        matched = self.compare_data['matched']
        unique = self.compare_data['only1'] if w == 1 else self.compare_data['only2']

        # Sicherheits-Dialog mit Modus-Auswahl
        dlg = DeleteDialog(
            self, wp, ov,
            matched_count=len(matched),
            unique_count=len(unique),
            matched_size=self.compare_data['total_matched'],
            unique_size=self.compare_data['total_only1'] if w == 1 else self.compare_data['total_only2'],
            backup_root=self._get_backup_root()
        )

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        mode = dlg.mode

        # Matched files für den Thread vorbereiten
        matched_for_thread = [{'name': m['name'], 'size': m['size']} for m in matched]

        # Backup-Laufwerk Erreichbarkeits-Check (KRITISCH!)
        bkp_root = self._get_backup_root()
        if bkp_root:
            bkp_drive = os.path.splitdrive(bkp_root)[0]
            if bkp_drive and not os.path.exists(bkp_drive + '\\'):
                QMessageBox.critical(self, "Backup-Laufwerk nicht erreichbar!",
                    f"Das Backup-Laufwerk {bkp_drive} ist nicht erreichbar!\n\n"
                    f"Bitte Laufwerk anschliessen und erneut versuchen.\n"
                    f"Ohne funktionierendes Backup wird NICHT geloescht.")
                return

            # Speicherplatz-Check
            needed = self.compare_data['total_matched'] if mode == 'duplicates' else (
                self.compare_data['total_matched'] + (self.compare_data['total_only1'] if w == 1 else self.compare_data['total_only2']))
            ok, free, msg = check_disk_space(bkp_root, needed)
            if not ok:
                QMessageBox.warning(self, "Nicht genug Platz", msg)
                return
            self._append_log(f"Platz-Check Backup: {msg}")

        # Progress-Bar aktivieren
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(len(matched) if mode == 'duplicates' else len(matched) + len(unique))
        self.progress_bar.setValue(0)
        self.progress_lbl.setText("Starte Loeschvorgang...")
        self.btn_d1.setEnabled(False); self.btn_d2.setEnabled(False)
        self.btn_m1.setEnabled(False); self.btn_m2.setEnabled(False)

        # Thread starten (mit Backup wenn konfiguriert)
        self._del_thread = DeleteThread(
            self.db_path, dir_id, p, mode=mode,
            matched_files=matched_for_thread,
            backup_root=self._get_backup_root()
        )
        self._del_thread.progress.connect(self._del_progress)
        self._del_thread.log_line.connect(self._append_log)
        self._del_thread.finished_ok.connect(self._del_done)
        self._del_thread.error.connect(self._del_error)
        self._del_thread.start()

    def _del_progress(self, msg, current, total):
        self.progress_lbl.setText(msg)
        self.progress_bar.setValue(current)

    def _del_error(self, msg):
        self.progress_bar.setVisible(False)
        self.progress_lbl.setText("")
        self.btn_d1.setEnabled(True); self.btn_d2.setEnabled(True)
        self.btn_m1.setEnabled(True); self.btn_m2.setEnabled(True)
        QMessageBox.critical(self, "Fehler", msg)

    def _del_done(self, summary):
        self.progress_bar.setVisible(False)
        self.progress_lbl.setText("")
        self.btn_d1.setEnabled(True); self.btn_d2.setEnabled(True)
        self.btn_m1.setEnabled(True); self.btn_m2.setEnabled(True)

        # Zusammenfassung
        d = summary['deleted']
        f = summary['failed']
        db = summary['db_cleaned']
        freed = summary['bytes_freed']
        bkp = summary.get('backed_up', 0)
        bkp_dir = summary.get('backup_dir', '')
        errs = summary['errors']
        total = summary['total']

        msg = f"ERGEBNIS:\n\n"

        if bkp > 0:
            msg += (
                f"Backup erstellt:     {bkp} Dateien\n"
                f"Backup-Ordner:       {bkp_dir}\n\n"
            )

        msg += (
            f"Dateien geloescht:      {d} / {total}\n"
            f"DB-Eintraege bereinigt: {db}\n"
            f"Speicher freigegeben:   {human_size(freed)}\n"
        )

        if f > 0:
            msg += f"\nFehlgeschlagen: {f}\n"

        if db != d:
            msg += f"\nHINWEIS: {d} Dateien geloescht, aber nur {db} DB-Eintraege bereinigt.\n"
            msg += "Moegliche Multi-Dot-Dateinamen oder DB-Inkonsistenz.\n"

        if errs:
            msg += "\nFehler-Details:\n"
            for e in errs[:10]:
                msg += f"  - {e}\n"
            if len(errs) > 10:
                msg += f"  ... und {len(errs)-10} weitere\n"

        # Integrität: Warnung wenn Diskrepanz
        if d > 0 and db == 0:
            msg += (
                "\nKRITISCH: Dateien wurden geloescht, aber KEINE DB-Eintraege!\n"
                "DB ist jetzt inkonsistent. Bitte Scanner neu laufen lassen.\n"
            )

        icon = QMessageBox.Icon.Information if f == 0 else QMessageBox.Icon.Warning
        mb = QMessageBox(icon, "Loeschvorgang abgeschlossen", msg, QMessageBox.StandardButton.Ok, self)
        mb.setStyleSheet("QMessageBox { background-color: #1e1e2e; } QLabel { color: #cdd6f4; }")
        mb.exec()

        # UI aktualisieren: Vergleich neu laden
        if d > 0 and self.current_pair:
            self.on_pair_selected()

        self.statusBar().showMessage(
            f"Geloescht: {d}/{total} Dateien, {human_size(freed)} frei, {db} DB-Eintraege bereinigt"
        )

    # ─── Merge ──────────────────────────────────────────────
    def merge_to(self, target_side):
        """Merge: Einzigartige Dateien von der ANDEREN Seite zur target_side kopieren."""
        if not self.current_pair or not self.compare_data:
            return

        # target_side=1 → kopiere only2 (einzigartig rechts) nach Ordner 1
        # target_side=2 → kopiere only1 (einzigartig links) nach Ordner 2
        if target_side == 1:
            files_to_copy = self.compare_data['only2']
            src_dir_id = self.current_pair['dir2_id']
            src_path = self.current_pair['path2']
            tgt_dir_id = self.current_pair['dir1_id']
            tgt_path = self.current_pair['path1']
            total_size = self.compare_data['total_only2']
            direction = "Ordner 2 → Ordner 1"
        else:
            files_to_copy = self.compare_data['only1']
            src_dir_id = self.current_pair['dir1_id']
            src_path = self.current_pair['path1']
            tgt_dir_id = self.current_pair['dir2_id']
            tgt_path = self.current_pair['path2']
            total_size = self.compare_data['total_only1']
            direction = "Ordner 1 → Ordner 2"

        if not files_to_copy:
            QMessageBox.information(self, "Nichts zu mergen",
                "Keine einzigartigen Dateien auf der anderen Seite!")
            return

        # Speicherplatz-Check am Ziel
        tgt_win = db_path_to_win(tgt_path)
        ok, free, space_msg = check_disk_space(tgt_win, total_size)
        if not ok:
            QMessageBox.warning(self, "Nicht genug Platz", space_msg)
            return
        self._append_log(f"Platz-Check Merge: {space_msg}")

        # Bestätigungsdialog
        msg = (
            f"MERGE: {direction}\n\n"
            f"{len(files_to_copy)} Dateien ({human_size(total_size)}) kopieren nach:\n"
            f"{tgt_win}\n\n"
            f"Ablauf:\n"
            f"1. Jede Datei einzeln kopieren (shutil.copy2)\n"
            f"2. Verifizieren (Ziel existiert + Groesse stimmt)\n"
            f"3. DB-Eintrag im Ziel-Ordner anlegen\n\n"
            f"Danach sind beide Ordner identisch und einer kann sicher geloescht werden."
        )

        if QMessageBox.question(self, "Merge bestaetigen", msg,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No) != QMessageBox.StandardButton.Yes:
            return

        # Progress-Bar
        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(len(files_to_copy))
        self.progress_bar.setValue(0)
        self.progress_lbl.setText("Starte Merge...")
        self.btn_d1.setEnabled(False); self.btn_d2.setEnabled(False)
        self.btn_m1.setEnabled(False); self.btn_m2.setEnabled(False)

        # Thread starten
        self._merge_thread = MergeThread(
            self.db_path, src_dir_id, src_path, tgt_dir_id, tgt_path,
            [{'name': f['name'], 'size': f['size']} for f in files_to_copy]
        )
        self._merge_thread.progress.connect(self._merge_progress)
        self._merge_thread.log_line.connect(self._append_log)
        self._merge_thread.finished_ok.connect(self._merge_done)
        self._merge_thread.error.connect(self._merge_error)
        self._merge_thread.start()

    def _merge_progress(self, msg, current, total):
        self.progress_lbl.setText(msg)
        self.progress_bar.setValue(current)

    def _merge_error(self, msg):
        self.progress_bar.setVisible(False); self.progress_lbl.setText("")
        self.btn_d1.setEnabled(True); self.btn_d2.setEnabled(True)
        self.btn_m1.setEnabled(True); self.btn_m2.setEnabled(True)
        QMessageBox.critical(self, "Merge-Fehler", msg)

    def _merge_done(self, summary):
        self.progress_bar.setVisible(False); self.progress_lbl.setText("")
        self.btn_d1.setEnabled(True); self.btn_d2.setEnabled(True)
        self.btn_m1.setEnabled(True); self.btn_m2.setEnabled(True)

        c = summary['copied']
        s = summary['skipped']
        f = summary['failed']
        db = summary['db_added']
        byt = summary['bytes_copied']
        errs = summary['errors']
        total = summary['total']

        msg = (
            f"MERGE ERGEBNIS:\n\n"
            f"Kopiert:        {c} / {total} Dateien\n"
            f"Uebersprungen:  {s} (existierten bereits)\n"
            f"DB-Eintraege:   {db} angelegt\n"
            f"Daten kopiert:  {human_size(byt)}\n"
        )

        if f > 0:
            msg += f"\nFehlgeschlagen: {f}\n"

        if c > 0 and db < c:
            msg += (
                f"\nHINWEIS: {c} kopiert, aber nur {db} DB-Eintraege.\n"
                f"Einige existierten moeglicherweise schon in der DB.\n"
            )

        if errs:
            msg += "\nFehler:\n"
            for e in errs[:10]:
                msg += f"  - {e}\n"

        if f == 0 and c > 0:
            msg += "\nBeide Ordner sollten jetzt identisch sein.\nDer Quell-Ordner kann sicher geloescht werden!"

        icon = QMessageBox.Icon.Information if f == 0 else QMessageBox.Icon.Warning
        mb = QMessageBox(icon, "Merge abgeschlossen", msg, QMessageBox.StandardButton.Ok, self)
        mb.setStyleSheet("QMessageBox { background-color: #1e1e2e; } QLabel { color: #cdd6f4; }")
        mb.exec()

        # UI refreshen
        if c > 0 and self.current_pair:
            self.on_pair_selected()

        self.statusBar().showMessage(
            f"Merge: {c}/{total} kopiert, {human_size(byt)}, {db} DB-Eintraege")

    def show_ctx(self, pos):
        it = self.pairs_tree.itemAt(pos)
        if not it: return
        p = it.data(0, Qt.ItemDataRole.UserRole); m = QMenu(self)
        a1=m.addAction("Ordner 1 Explorer"); a2=m.addAction("Ordner 2 Explorer")
        m.addSeparator()
        a3=m.addAction("Pfad 1 kopieren"); a4=m.addAction("Pfad 2 kopieren")
        a = m.exec(self.pairs_tree.viewport().mapToGlobal(pos))
        if a==a1: self.current_pair=p; self.open_explorer(1)
        elif a==a2: self.current_pair=p; self.open_explorer(2)
        elif a==a3: QApplication.clipboard().setText(p['path1'])
        elif a==a4: QApplication.clipboard().setText(p['path2'])


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    w = SpaceRecoveryAdvisor()
    w.showMaximized()
    sys.exit(app.exec())
