#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import sqlite3
import os
import random
import threading
import urllib.parse
import webbrowser
import re
from functools import partial

from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtMultimedia import QMediaPlayer, QMediaContent
from PyQt5.QtCore import QUrl, Qt, QSettings, pyqtSignal, QThread
from PyQt5.QtGui import QPixmap, QColor, QFont, QIcon, QKeySequence
from PyQt5.QtWidgets import (
    QSplitter, QStatusBar, QShortcut, QHeaderView, QAbstractItemView,
    QMessageBox, QInputDialog, QFileDialog, QMenu, QAction
)

import mutagen
from mutagen.id3 import ID3, APIC
from mutagen.flac import FLAC
import musicbrainzngs

# MusicBrainz API konfigurieren
musicbrainzngs.set_useragent("SexyMusicManager", "2.0", "https://github.com/example")
musicbrainzngs.set_rate_limit(1, 1)  # Max 1 Request pro Sekunde

AUDIO_EXTENSIONS = ('.mp3', '.flac', '.ogg', '.m4a', '.wma', '.opus', '.aac')
PLAYLIST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'playlists')
LAST_PLAYLIST_PATH = os.path.join(PLAYLIST_DIR, 'last_playlist.m3u')
SETTINGS_ORG = "DateiDB"
SETTINGS_APP = "MusicManager"

# --- Repeat Modes ---
REPEAT_OFF = 0
REPEAT_ALL = 1
REPEAT_ONE = 2
REPEAT_LABELS = {REPEAT_OFF: "🔁 Aus", REPEAT_ALL: "🔁 Alle", REPEAT_ONE: "🔂 Eins"}


def normalize_text(text):
    text = text.lower()
    text = re.sub(r'[^a-z0-9 ]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def fetch_songs(db_path, search_fields):
    """Schnelle DB-Abfrage ohne Dateisystem-Zugriff. Gibt Pfade zurück."""
    songs = []
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        conn.execute("PRAGMA busy_timeout = 30000")
        cursor = conn.cursor()

        ext_placeholders = ','.join(['?' for _ in AUDIO_EXTENSIONS])
        query = f"""
            SELECT
                d.full_path || '/' || f.filename ||
                CASE WHEN e.name IS NULL OR e.name = '[none]' THEN '' ELSE e.name END
                as file_path,
                f.filename,
                d.full_path
            FROM files f
            JOIN directories d ON f.directory_id = d.id
            LEFT JOIN extensions e ON f.extension_id = e.id
            WHERE e.name IN ({ext_placeholders})
        """
        params = list(AUDIO_EXTENSIONS)

        additional_where = []
        if search_fields.get('main'):
            for t in search_fields['main'].split():
                additional_where.append("(f.filename LIKE ? OR d.full_path LIKE ?)")
                params.extend([f"%{t}%", f"%{t}%"])
        if search_fields.get('title'):
            additional_where.append("f.filename LIKE ?")
            params.append(f"%{search_fields['title']}%")
        if search_fields.get('artist'):
            additional_where.append("(f.filename LIKE ? OR d.full_path LIKE ?)")
            params.extend([f"%{search_fields['artist']}%", f"%{search_fields['artist']}%"])
        if search_fields.get('album'):
            additional_where.append("d.full_path LIKE ?")
            params.append(f"%{search_fields['album']}%")
        if search_fields.get('year'):
            additional_where.append("(f.filename LIKE ? OR d.full_path LIKE ?)")
            params.extend([f"%{search_fields['year']}%", f"%{search_fields['year']}%"])
        if search_fields.get('genre'):
            additional_where.append("(f.filename LIKE ? OR d.full_path LIKE ?)")
            params.extend([f"%{search_fields['genre']}%", f"%{search_fields['genre']}%"])

        if additional_where:
            query += " AND " + " AND ".join(additional_where)

        query += " ORDER BY d.full_path, f.filename"

        cursor.execute(query, params)
        for row in cursor.fetchall():
            file_path = os.path.normpath(row[0].replace('/', os.sep))
            songs.append({
                'file_path': file_path,
                'filename': row[1],
                'directory': row[2],
                'title': '',
                'artist': '',
                'album': '',
                'year': '',
                'genre': '',
                'duration': '',
                'duration_sec': 0,
                'bitrate': 0,
                'tags_loaded': False,
            })
        conn.close()
    except sqlite3.Error as e:
        print(f"DB Fehler: {e}", file=sys.stderr)
    return songs


def read_tags(file_path):
    """Liest Audio-Tags mit mutagen. Gibt dict mit Metadaten zurück."""
    meta = {
        'title': '', 'artist': '', 'album': '',
        'year': '', 'genre': '', 'duration': '',
        'duration_sec': 0, 'bitrate': 0,
    }
    try:
        audio = mutagen.File(file_path, easy=True)
        if audio is None:
            return meta
        meta['title'] = (audio.get('title') or [''])[0]
        meta['artist'] = (audio.get('artist') or [''])[0]
        meta['album'] = (audio.get('album') or [''])[0]
        meta['year'] = (audio.get('date') or audio.get('year') or [''])[0]
        meta['genre'] = (audio.get('genre') or [''])[0]
        if hasattr(audio, 'info') and audio.info:
            length = getattr(audio.info, 'length', 0) or 0
            meta['duration_sec'] = int(round(length))
            meta['duration'] = f"{int(length)//60:02}:{int(length)%60:02}"
            meta['bitrate'] = getattr(audio.info, 'bitrate', 0) or 0
    except Exception:
        pass
    return meta


def extract_cover(file_path):
    """Extrahiert Cover-Art als bytes. Gibt None zurück wenn kein Cover."""
    try:
        audio = mutagen.File(file_path)
        if audio is None:
            return None
        # MP3 (ID3)
        if hasattr(audio, 'tags') and audio.tags:
            apic_list = audio.tags.getall('APIC') if hasattr(audio.tags, 'getall') else []
            if apic_list:
                return apic_list[0].data
        # FLAC
        if isinstance(audio, FLAC) and audio.pictures:
            return audio.pictures[0].data
        # OGG/Opus mit eingebettetem Cover
        if hasattr(audio, 'tags') and audio.tags:
            for key in ('metadata_block_picture', 'METADATA_BLOCK_PICTURE'):
                pics = audio.tags.get(key)
                if pics:
                    import base64
                    from mutagen.flac import Picture
                    pic = Picture(base64.b64decode(pics[0]))
                    return pic.data
    except Exception:
        pass
    return None


class TagLoaderWorker(QThread):
    """Hintergrund-Thread zum Laden von Audio-Tags."""
    tags_loaded = pyqtSignal(int, dict)  # (row_index, meta_dict)
    progress = pyqtSignal(int, int)  # (current, total)
    finished_loading = pyqtSignal()

    def __init__(self, songs, parent=None):
        super().__init__(parent)
        self.songs = songs
        self._cancel = False

    def cancel(self):
        self._cancel = True

    def run(self):
        total = len(self.songs)
        for i, song in enumerate(self.songs):
            if self._cancel:
                return
            if not os.path.exists(song['file_path']):
                continue
            meta = read_tags(song['file_path'])
            meta['tags_loaded'] = True
            self.tags_loaded.emit(i, meta)
            if (i + 1) % 50 == 0 or i == total - 1:
                self.progress.emit(i + 1, total)
        self.finished_loading.emit()


def mb_search_recording(artist, title, duration_sec=0):
    """Sucht einen Song auf MusicBrainz. Gibt Liste von Treffern zurück."""
    results = []
    try:
        # Verwende spezifische Felder für bessere Ergebnisse
        if artist and title:
            query = f'recording:"{title}" AND artist:"{artist}"'
        elif title:
            query = f'recording:"{title}"'
        else:
            query = artist
        search_args = {'query': query, 'limit': 8}
        response = musicbrainzngs.search_recordings(**search_args)
        for rec in response.get('recording-list', []):
            r = {
                'mb_title': rec.get('title', ''),
                'mb_artist': '',
                'mb_album': '',
                'mb_year': '',
                'mb_genre': '',
                'mb_duration_sec': 0,
                'mb_score': int(rec.get('ext:score', 0)),
            }
            # Artist
            artists = rec.get('artist-credit', [])
            if artists:
                r['mb_artist'] = artists[0].get('artist', {}).get('name', '')
            # Album (erstes Release)
            releases = rec.get('release-list', [])
            if releases:
                r['mb_album'] = releases[0].get('title', '')
                r['mb_year'] = releases[0].get('date', '')[:4] if releases[0].get('date') else ''
            # Dauer
            if rec.get('length'):
                r['mb_duration_sec'] = int(rec['length']) // 1000
            # Genre/Tags
            tag_list = rec.get('tag-list', [])
            if tag_list:
                # Sortiere nach Count, nimm den häufigsten
                tag_list.sort(key=lambda t: int(t.get('count', 0)), reverse=True)
                r['mb_genre'] = tag_list[0].get('name', '').title()
            results.append(r)
    except Exception as e:
        print(f"MusicBrainz Fehler: {e}", file=sys.stderr)
    return results


class MusicBrainzWorker(QThread):
    """Einzelner MusicBrainz-Lookup für einen Song."""
    results_ready = pyqtSignal(list)  # Liste von Treffern
    error = pyqtSignal(str)

    def __init__(self, artist, title, duration_sec=0, parent=None):
        super().__init__(parent)
        self.artist = artist
        self.title = title
        self.duration_sec = duration_sec

    def run(self):
        try:
            results = mb_search_recording(self.artist, self.title, self.duration_sec)
            self.results_ready.emit(results)
        except Exception as e:
            self.error.emit(str(e))


class MusicBrainzBatchWorker(QThread):
    """Batch-Lookup für mehrere Songs mit fehlenden Tags."""
    song_updated = pyqtSignal(int, dict)  # (row_index, best_match)
    progress = pyqtSignal(int, int, str)  # (current, total, status_text)
    finished_batch = pyqtSignal(int, int)  # (updated, skipped)

    def __init__(self, songs, parent=None):
        super().__init__(parent)
        self.songs = songs  # Liste von (row_index, song_dict)
        self._cancel = False

    def cancel(self):
        self._cancel = True

    def run(self):
        updated = 0
        skipped = 0
        total = len(self.songs)
        for i, (row_idx, song) in enumerate(self.songs):
            if self._cancel:
                break
            # Suchbegriff: Dateiname parsen wenn kein Title/Artist
            artist = song.get('artist', '')
            title = song.get('title', '')
            if not title:
                fname = os.path.splitext(os.path.basename(song.get('file_path', '')))[0]
                if ' - ' in fname:
                    artist, title = fname.split(' - ', 1)
                    artist = artist.strip()
                    title = title.strip()
                else:
                    title = fname.strip()

            self.progress.emit(i + 1, total, f"{artist} - {title}" if artist else title)

            results = mb_search_recording(artist, title, song.get('duration_sec', 0))
            if results:
                # Besten Treffer nehmen (höchster Score)
                best = results[0]
                # Nur übernehmen wenn Score >= 80
                if best['mb_score'] >= 80:
                    self.song_updated.emit(row_idx, best)
                    updated += 1
                else:
                    skipped += 1
            else:
                skipped += 1

            import time
            time.sleep(1.1)  # MusicBrainz Rate-Limit einhalten

        self.finished_batch.emit(updated, skipped)


class MusicBrainzResultDialog(QtWidgets.QDialog):
    """Dialog zur Auswahl eines MusicBrainz-Ergebnisses."""
    def __init__(self, results, song, parent=None):
        super().__init__(parent)
        self.setWindowTitle("MusicBrainz Ergebnisse")
        self.setMinimumSize(650, 400)
        self.selected_result = None
        self.setStyleSheet("""
            QDialog { background: #1a1a2e; color: #e0e0e0; }
            QTableWidget { background: #16213e; color: #e0e0e0; border: 1px solid #333;
                          alternate-background-color: #1a2744;
                          selection-background-color: #0f3460; }
            QHeaderView::section { background: #0f3460; color: #e0e0ff; font-weight: bold;
                                  border: 1px solid #1a1a3e; padding: 4px; }
            QPushButton { background: #e94560; color: white; border: none;
                         padding: 8px 16px; border-radius: 4px; font-weight: bold; }
            QPushButton:hover { background: #ff6b6b; }
            QLabel { color: #aab; }
        """)

        layout = QtWidgets.QVBoxLayout(self)

        # Info
        fname = os.path.basename(song.get('file_path', ''))
        info = QtWidgets.QLabel(f"Suche für: {fname}")
        info.setStyleSheet("font-weight: bold; color: #e0e0ff; font-size: 13px; padding: 4px;")
        layout.addWidget(info)

        # Ergebnis-Tabelle
        self.table = QtWidgets.QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["Score", "Titel", "Interpret", "Album", "Jahr", "Genre"])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setAlternatingRowColors(True)
        self.table.setRowCount(len(results))

        for row, r in enumerate(results):
            score_item = QtWidgets.QTableWidgetItem(f"{r['mb_score']}%")
            score_item.setTextAlignment(Qt.AlignCenter)
            if r['mb_score'] >= 90:
                score_item.setForeground(QColor("#4caf50"))
            elif r['mb_score'] >= 70:
                score_item.setForeground(QColor("#ffb74d"))
            else:
                score_item.setForeground(QColor("#ef5350"))
            self.table.setItem(row, 0, score_item)
            self.table.setItem(row, 1, QtWidgets.QTableWidgetItem(r['mb_title']))
            self.table.setItem(row, 2, QtWidgets.QTableWidgetItem(r['mb_artist']))
            self.table.setItem(row, 3, QtWidgets.QTableWidgetItem(r['mb_album']))
            self.table.setItem(row, 4, QtWidgets.QTableWidgetItem(r['mb_year']))
            self.table.setItem(row, 5, QtWidgets.QTableWidgetItem(r['mb_genre']))

        self.table.resizeColumnsToContents()
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        # Buttons
        btn_layout = QtWidgets.QHBoxLayout()
        self.applyBtn = QtWidgets.QPushButton("Übernehmen + Speichern")
        self.applyBtn.clicked.connect(self._apply)
        cancelBtn = QtWidgets.QPushButton("Abbrechen")
        cancelBtn.clicked.connect(self.reject)
        cancelBtn.setStyleSheet("background: #444;")
        btn_layout.addStretch()
        btn_layout.addWidget(cancelBtn)
        btn_layout.addWidget(self.applyBtn)
        layout.addLayout(btn_layout)

        # Erste Zeile vorauswählen
        if results:
            self.table.selectRow(0)

    def _apply(self):
        selected = self.table.selectionModel().selectedRows()
        if selected:
            self.selected_result = selected[0].row()
        elif self.table.rowCount() > 0:
            # Fallback: erste Zeile
            self.selected_result = 0
        else:
            return
        self.accept()

    def get_selected_index(self):
        return self.selected_result


class SongPlayer(QtWidgets.QMainWindow):
    def __init__(self, db_path):
        super().__init__()
        self.setWindowTitle("Wow! Sexy Music Manager")
        _icon = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'icons', 'music.ico')
        if os.path.exists(_icon):
            self.setWindowIcon(QIcon(_icon))

        self.db_path = db_path
        self.songs = []
        self.current_playlist = []
        self.current_index = -1
        self.repeat_mode = REPEAT_OFF
        self.tag_worker = None
        self._mb_batch_worker = None
        self.settings = QSettings(SETTINGS_ORG, SETTINGS_APP)

        # Fensterposition/-größe wiederherstellen
        geo = self.settings.value("geometry")
        if geo:
            self.restoreGeometry(geo)
        else:
            screen = QtWidgets.QApplication.desktop().availableGeometry(self)
            width = screen.width() // 3
            self.setGeometry(screen.x() + screen.width() - width, screen.y(), width, screen.height())

        self._build_ui()
        self._setup_shortcuts()
        self._apply_style()

        self.load_last_playlist()

        # Splitter-Sizes wiederherstellen
        v_sizes = self.settings.value("v_splitter")
        if v_sizes:
            self.vSplitter.setSizes([int(s) for s in v_sizes])
        h_sizes = self.settings.value("h_splitter")
        if h_sizes:
            self.hSplitter.setSizes([int(s) for s in h_sizes])

    def _build_ui(self):
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        mainLayout = QtWidgets.QVBoxLayout(central)
        mainLayout.setContentsMargins(6, 6, 6, 0)
        mainLayout.setSpacing(4)

        # === Suchbereich ===
        searchRow1 = QtWidgets.QHBoxLayout()
        self.titleSearch = QtWidgets.QLineEdit()
        self.titleSearch.setPlaceholderText("Titel")
        self.artistSearch = QtWidgets.QLineEdit()
        self.artistSearch.setPlaceholderText("Interpret")
        self.albumSearch = QtWidgets.QLineEdit()
        self.albumSearch.setPlaceholderText("Album")
        self.yearSearch = QtWidgets.QLineEdit()
        self.yearSearch.setPlaceholderText("Jahr")
        self.yearSearch.setMaximumWidth(60)
        self.genreSearch = QtWidgets.QLineEdit()
        self.genreSearch.setPlaceholderText("Genre")
        for w in (self.titleSearch, self.artistSearch, self.albumSearch, self.yearSearch, self.genreSearch):
            w.returnPressed.connect(self.search_songs)
            searchRow1.addWidget(w)

        searchRow2 = QtWidgets.QHBoxLayout()
        self.searchEdit = QtWidgets.QLineEdit()
        self.searchEdit.setPlaceholderText("Suchbegriff (Dateiname/Pfad)...")
        self.searchEdit.returnPressed.connect(self.search_songs)
        self.searchButton = QtWidgets.QPushButton("Suchen")
        self.searchButton.clicked.connect(self.search_songs)
        self.filterEdit = QtWidgets.QLineEdit()
        self.filterEdit.setPlaceholderText("Filter in Ergebnissen...")
        self.filterEdit.textChanged.connect(self.filter_table)
        self.autoTagButton = QtWidgets.QPushButton("Auto-Tag (MB)")
        self.autoTagButton.setToolTip("Fehlende Tags automatisch via MusicBrainz suchen")
        self.autoTagButton.clicked.connect(self._mb_batch_lookup)
        self.autoTagButton.setCursor(Qt.PointingHandCursor)
        searchRow2.addWidget(self.searchEdit, 3)
        searchRow2.addWidget(self.searchButton)
        searchRow2.addWidget(self.autoTagButton)
        searchRow2.addWidget(self.filterEdit, 2)

        mainLayout.addLayout(searchRow1)
        mainLayout.addLayout(searchRow2)

        # === Vertikaler Splitter: Oben (Songs+Cover) | Unten (Playlist) ===
        self.vSplitter = QSplitter(Qt.Vertical)

        # --- Oberer Bereich: Horizontaler Splitter (Table | Cover) ---
        self.hSplitter = QSplitter(Qt.Horizontal)

        # Song Table
        self.songTable = QtWidgets.QTableWidget()
        self.songTable.setColumnCount(7)
        self.songTable.setHorizontalHeaderLabels(["Pfad", "Titel", "Interpret", "Album", "Jahr", "Genre", "Dauer"])
        self.songTable.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.songTable.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.songTable.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.songTable.horizontalHeader().setStretchLastSection(False)
        self.songTable.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)   # Titel
        self.songTable.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)   # Interpret
        self.songTable.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)   # Album
        self.songTable.setSortingEnabled(True)
        self.songTable.setDragEnabled(True)
        self.songTable.setDragDropMode(QAbstractItemView.DragOnly)
        self.songTable.setContextMenuPolicy(Qt.CustomContextMenu)
        self.songTable.customContextMenuRequested.connect(self.show_song_context_menu)
        self.songTable.doubleClicked.connect(self._song_table_double_clicked)
        self.songTable.setColumnHidden(0, True)
        self.songTable.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)
        self.songTable.selectionModel().selectionChanged.connect(self._update_selection_status)

        # Cover + Now Playing Panel
        coverPanel = QtWidgets.QWidget()
        coverLayout = QtWidgets.QVBoxLayout(coverPanel)
        coverLayout.setContentsMargins(4, 4, 4, 4)

        self.coverLabel = QtWidgets.QLabel()
        self.coverLabel.setFixedSize(220, 220)
        self.coverLabel.setAlignment(Qt.AlignCenter)
        self.coverLabel.setStyleSheet("background: #1a1a2e; border: 2px solid #444; border-radius: 12px;")
        self._set_cover_placeholder()

        self.nowTitle = QtWidgets.QLabel("")
        self.nowTitle.setWordWrap(True)
        self.nowTitle.setAlignment(Qt.AlignCenter)
        self.nowTitle.setStyleSheet("font-size: 15px; font-weight: bold; color: #e0e0ff; padding: 4px;")
        self.nowArtist = QtWidgets.QLabel("")
        self.nowArtist.setWordWrap(True)
        self.nowArtist.setAlignment(Qt.AlignCenter)
        self.nowArtist.setStyleSheet("font-size: 13px; color: #aab; padding: 2px;")
        self.nowAlbum = QtWidgets.QLabel("")
        self.nowAlbum.setWordWrap(True)
        self.nowAlbum.setAlignment(Qt.AlignCenter)
        self.nowAlbum.setStyleSheet("font-size: 12px; color: #889; padding: 2px;")

        coverLayout.addStretch()
        coverLayout.addWidget(self.coverLabel, 0, Qt.AlignCenter)
        coverLayout.addWidget(self.nowTitle)
        coverLayout.addWidget(self.nowArtist)
        coverLayout.addWidget(self.nowAlbum)
        coverLayout.addStretch()
        coverPanel.setMinimumWidth(230)
        coverPanel.setMaximumWidth(300)

        self.hSplitter.addWidget(self.songTable)
        self.hSplitter.addWidget(coverPanel)
        self.hSplitter.setStretchFactor(0, 4)
        self.hSplitter.setStretchFactor(1, 1)

        # --- Player Controls ---
        controlWidget = QtWidgets.QWidget()
        controlLayout = QtWidgets.QHBoxLayout(controlWidget)
        controlLayout.setContentsMargins(4, 2, 4, 2)

        self.prevButton = QtWidgets.QPushButton("⏮")
        self.playButton = QtWidgets.QPushButton("▶")
        self.pauseButton = QtWidgets.QPushButton("⏸")
        self.stopButton = QtWidgets.QPushButton("⏹")
        self.nextButton = QtWidgets.QPushButton("⏭")
        self.repeatButton = QtWidgets.QPushButton(REPEAT_LABELS[REPEAT_OFF])
        self.shuffleButton = QtWidgets.QPushButton("🔀")

        for btn in (self.prevButton, self.playButton, self.pauseButton, self.stopButton,
                    self.nextButton, self.repeatButton, self.shuffleButton):
            btn.setFixedHeight(36)
            btn.setCursor(Qt.PointingHandCursor)

        self.prevButton.clicked.connect(self.play_previous)
        self.playButton.clicked.connect(self.play_selected)
        self.pauseButton.clicked.connect(self.pause_song)
        self.stopButton.clicked.connect(self.stop_song)
        self.nextButton.clicked.connect(self.play_next)
        self.repeatButton.clicked.connect(self.cycle_repeat)
        self.shuffleButton.clicked.connect(self.shuffle_playlist)

        self.slider = QtWidgets.QSlider(Qt.Horizontal)
        self.slider.setRange(0, 0)
        self.slider.sliderMoved.connect(self.set_position)

        self.timeLabel = QtWidgets.QLabel("00:00 / 00:00")
        self.timeLabel.setMinimumWidth(100)

        self.volumeLabel = QtWidgets.QLabel("🔊")
        self.volumeSlider = QtWidgets.QSlider(Qt.Horizontal)
        self.volumeSlider.setRange(0, 100)
        self.volumeSlider.setValue(int(self.settings.value("volume", 50)))
        self.volumeSlider.setFixedWidth(100)
        self.volumeSlider.valueChanged.connect(self.set_volume)

        controlLayout.addWidget(self.prevButton)
        controlLayout.addWidget(self.playButton)
        controlLayout.addWidget(self.pauseButton)
        controlLayout.addWidget(self.stopButton)
        controlLayout.addWidget(self.nextButton)
        controlLayout.addWidget(self.repeatButton)
        controlLayout.addWidget(self.shuffleButton)
        controlLayout.addWidget(self.slider, 1)
        controlLayout.addWidget(self.timeLabel)
        controlLayout.addWidget(self.volumeLabel)
        controlLayout.addWidget(self.volumeSlider)

        # --- Unterer Bereich: Playlist ---
        playlistPanel = QtWidgets.QWidget()
        playlistOuterLayout = QtWidgets.QVBoxLayout(playlistPanel)
        playlistOuterLayout.setContentsMargins(0, 0, 0, 0)
        playlistOuterLayout.setSpacing(2)

        playlistButtonLayout = QtWidgets.QHBoxLayout()
        self.addToPlaylistButton = QtWidgets.QPushButton("+ Playlist")
        self.removeFromPlaylistButton = QtWidgets.QPushButton("- Playlist")
        self.exportPlaylistButton = QtWidgets.QPushButton("Export M3U")
        self.importPlaylistButton = QtWidgets.QPushButton("Import M3U")
        self.playlistDropdown = QtWidgets.QComboBox()
        self.playlistDropdown.setMinimumWidth(180)

        self.addToPlaylistButton.clicked.connect(self.add_to_playlist)
        self.removeFromPlaylistButton.clicked.connect(self.remove_from_playlist)
        self.exportPlaylistButton.clicked.connect(self.export_playlist)
        self.importPlaylistButton.clicked.connect(self.import_playlist)

        self.playlistDropdown.addItem("<Letzte Playlist>")
        self.update_playlist_dropdown()
        self.playlistDropdown.currentIndexChanged.connect(self.dropdown_playlist_selected)

        for btn in (self.addToPlaylistButton, self.removeFromPlaylistButton,
                    self.exportPlaylistButton, self.importPlaylistButton):
            btn.setCursor(Qt.PointingHandCursor)
            playlistButtonLayout.addWidget(btn)
        playlistButtonLayout.addWidget(self.playlistDropdown)

        self.playlistTable = QtWidgets.QTableWidget()
        self.playlistTable.setColumnCount(9)
        self.playlistTable.setHorizontalHeaderLabels(
            ["Pfad", "Titel", "Interpret", "Album", "Dauer", "Genre", "YT", "SP", "AM"])
        self.playlistTable.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.playlistTable.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.playlistTable.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.playlistTable.horizontalHeader().setStretchLastSection(False)
        self.playlistTable.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)  # Titel
        self.playlistTable.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)  # Interpret
        self.playlistTable.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)  # Album
        self.playlistTable.setSortingEnabled(False)  # Playlist: manuelle Reihenfolge
        self.playlistTable.setColumnHidden(0, True)
        self.playlistTable.doubleClicked.connect(self.play_from_playlist)
        self.playlistTable.setAcceptDrops(True)
        self.playlistTable.setDropIndicatorShown(True)
        self.playlistTable.setDragDropMode(QAbstractItemView.DropOnly)
        self.playlistTable.setContextMenuPolicy(Qt.CustomContextMenu)
        self.playlistTable.customContextMenuRequested.connect(self.show_playlist_context_menu)

        # Drag & Drop
        self.playlistTable.dragEnterEvent = self._playlist_drag_enter
        self.playlistTable.dragMoveEvent = self._playlist_drag_move
        self.playlistTable.dropEvent = self._playlist_drop

        playlistOuterLayout.addLayout(playlistButtonLayout)
        playlistOuterLayout.addWidget(self.playlistTable)

        # Zusammensetzen
        topArea = QtWidgets.QWidget()
        topLayout = QtWidgets.QVBoxLayout(topArea)
        topLayout.setContentsMargins(0, 0, 0, 0)
        topLayout.setSpacing(2)
        topLayout.addWidget(self.hSplitter, 1)   # Stretch: Song-Tabelle füllt allen Platz
        topLayout.addWidget(controlWidget, 0)     # Controls: feste Höhe

        self.vSplitter.addWidget(topArea)
        self.vSplitter.addWidget(playlistPanel)
        self.vSplitter.setStretchFactor(0, 3)
        self.vSplitter.setStretchFactor(1, 1)

        mainLayout.addWidget(self.vSplitter)

        # === Statusbar ===
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusLabel = QtWidgets.QLabel("Bereit")
        self.selectionLabel = QtWidgets.QLabel("")
        self.tagProgressLabel = QtWidgets.QLabel("")
        self.statusBar.addWidget(self.statusLabel, 1)
        self.statusBar.addPermanentWidget(self.tagProgressLabel)
        self.statusBar.addPermanentWidget(self.selectionLabel)

        # === Media Player ===
        self.player = QMediaPlayer()
        self.player.positionChanged.connect(self._update_position)
        self.player.durationChanged.connect(self._update_duration)
        self.player.stateChanged.connect(self._update_button_states)
        self.player.setVolume(self.volumeSlider.value())
        self.player.mediaStatusChanged.connect(self._handle_media_status)

    def _setup_shortcuts(self):
        QShortcut(QKeySequence(Qt.Key_Space), self, self._toggle_play_pause)
        QShortcut(QKeySequence(Qt.Key_N), self, self.play_next)
        QShortcut(QKeySequence(Qt.Key_P), self, self.play_previous)
        QShortcut(QKeySequence(Qt.Key_Left), self, lambda: self._seek_relative(-5000))
        QShortcut(QKeySequence(Qt.Key_Right), self, lambda: self._seek_relative(5000))
        QShortcut(QKeySequence(Qt.Key_Up), self, lambda: self.volumeSlider.setValue(min(100, self.volumeSlider.value() + 5)))
        QShortcut(QKeySequence(Qt.Key_Down), self, lambda: self.volumeSlider.setValue(max(0, self.volumeSlider.value() - 5)))
        QShortcut(QKeySequence("Ctrl+F"), self, lambda: self.searchEdit.setFocus())
        QShortcut(QKeySequence(Qt.Key_Delete), self, self.remove_from_playlist)
        QShortcut(QKeySequence("Ctrl+A"), self, lambda: self.songTable.selectAll())
        QShortcut(QKeySequence(Qt.Key_Return), self, self.play_selected)
        QShortcut(QKeySequence("Ctrl+S"), self, self.shuffle_playlist)

    def _apply_style(self):
        self.setStyleSheet("""
            QMainWindow, QWidget {
                background-color: #1a1a2e;
                color: #e0e0e0;
                font-family: 'Segoe UI', 'Arial', sans-serif;
                font-size: 13px;
            }
            QTableWidget {
                background-color: #16213e;
                alternate-background-color: #1a2744;
                border: 1px solid #333;
                gridline-color: #2a2a4a;
                selection-background-color: #0f3460;
                selection-color: #fff;
                font-size: 12px;
            }
            QTableWidget::item {
                padding: 3px 6px;
            }
            QHeaderView::section {
                background-color: #0f3460;
                color: #e0e0ff;
                font-weight: bold;
                border: 1px solid #1a1a3e;
                padding: 4px 8px;
                font-size: 12px;
            }
            QPushButton {
                background-color: #e94560;
                color: white;
                border: none;
                padding: 6px 14px;
                border-radius: 6px;
                font-weight: bold;
                font-size: 13px;
            }
            QPushButton:hover {
                background-color: #ff6b6b;
            }
            QPushButton:pressed {
                background-color: #c23152;
            }
            QPushButton:disabled {
                background-color: #444;
                color: #888;
            }
            QLineEdit {
                background: #16213e;
                color: #e0e0e0;
                border: 1px solid #333;
                border-radius: 6px;
                padding: 5px 10px;
                font-size: 13px;
            }
            QLineEdit:focus {
                border: 1px solid #e94560;
            }
            QComboBox {
                background: #16213e;
                color: #e0e0e0;
                border: 1px solid #333;
                border-radius: 6px;
                padding: 4px 8px;
            }
            QComboBox::drop-down {
                border: none;
            }
            QComboBox QAbstractItemView {
                background: #16213e;
                color: #e0e0e0;
                selection-background-color: #0f3460;
            }
            QSlider::groove:horizontal {
                background: #333;
                height: 6px;
                border-radius: 3px;
            }
            QSlider::handle:horizontal {
                background: #e94560;
                width: 14px;
                margin: -4px 0;
                border-radius: 7px;
            }
            QSlider::sub-page:horizontal {
                background: #e94560;
                border-radius: 3px;
            }
            QStatusBar {
                background: #0f0f23;
                color: #888;
                font-size: 11px;
                border-top: 1px solid #333;
            }
            QSplitter::handle {
                background: #333;
            }
            QSplitter::handle:vertical {
                height: 4px;
            }
            QSplitter::handle:horizontal {
                width: 4px;
            }
        """)
        self.songTable.setAlternatingRowColors(True)
        self.playlistTable.setAlternatingRowColors(True)

    # ==================== Lookup-Helpers ====================

    def _song_from_row(self, visual_row):
        """Gibt den Song-Dict für eine visuelle Tabellenzeile zurück (sortier-sicher)."""
        item = self.songTable.item(visual_row, 0)
        if not item:
            return None
        path = item.text()
        for song in self.songs:
            if song['file_path'] == path:
                return song
        return None

    def _songs_from_selection(self):
        """Gibt alle selektierten Songs zurück (sortier-sicher)."""
        selected = self.songTable.selectionModel().selectedRows()
        result = []
        for idx in selected:
            song = self._song_from_row(idx.row())
            if song:
                result.append(song)
        return result

    def _find_visual_row(self, file_path):
        """Findet die visuelle Zeile für einen Dateipfad. Gibt -1 zurück wenn nicht gefunden."""
        for row in range(self.songTable.rowCount()):
            item = self.songTable.item(row, 0)
            if item and item.text() == file_path:
                return row
        return -1

    def _song_index(self, file_path):
        """Findet den Index in self.songs für einen Dateipfad."""
        for i, s in enumerate(self.songs):
            if s['file_path'] == file_path:
                return i
        return -1

    # ==================== Suche ====================

    def search_songs(self):
        search_fields = {
            'main': self.searchEdit.text().strip(),
            'title': self.titleSearch.text().strip(),
            'artist': self.artistSearch.text().strip(),
            'album': self.albumSearch.text().strip(),
            'year': self.yearSearch.text().strip(),
            'genre': self.genreSearch.text().strip(),
        }
        # Laufenden Worker abbrechen
        self._cancel_tag_worker()

        self.songs = fetch_songs(self.db_path, search_fields)
        self._populate_song_table()
        self.filterEdit.clear()
        self.statusLabel.setText(f"{len(self.songs)} Songs gefunden")

        # Tags im Hintergrund laden
        if self.songs:
            self.tag_worker = TagLoaderWorker(self.songs)
            self.tag_worker.tags_loaded.connect(self._on_tag_loaded)
            self.tag_worker.progress.connect(self._on_tag_progress)
            self.tag_worker.finished_loading.connect(self._on_tags_finished)
            self.tag_worker.start()

    def _cancel_tag_worker(self):
        if self.tag_worker and self.tag_worker.isRunning():
            self.tag_worker.cancel()
            self.tag_worker.wait(2000)
            self.tag_worker = None

    def _on_tag_loaded(self, songs_idx, meta):
        if songs_idx >= len(self.songs):
            return
        self.songs[songs_idx].update(meta)
        # Finde die visuelle Zeile (kann durch Sortierung anders sein)
        visual_row = self._find_visual_row(self.songs[songs_idx]['file_path'])
        if visual_row < 0:
            return
        self.songTable.setSortingEnabled(False)
        self.songTable.setItem(visual_row, 1, QtWidgets.QTableWidgetItem(meta.get('title', '')))
        self.songTable.setItem(visual_row, 2, QtWidgets.QTableWidgetItem(meta.get('artist', '')))
        self.songTable.setItem(visual_row, 3, QtWidgets.QTableWidgetItem(meta.get('album', '')))
        self.songTable.setItem(visual_row, 4, QtWidgets.QTableWidgetItem(meta.get('year', '')))
        self.songTable.setItem(visual_row, 5, QtWidgets.QTableWidgetItem(meta.get('genre', '')))
        self.songTable.setItem(visual_row, 6, QtWidgets.QTableWidgetItem(meta.get('duration', '')))
        self.songTable.setSortingEnabled(True)

    def _on_tag_progress(self, current, total):
        self.tagProgressLabel.setText(f"Tags: {current}/{total}")
        self.songTable.resizeColumnsToContents()
        self.songTable.setColumnHidden(0, True)

    def _on_tags_finished(self):
        self.tagProgressLabel.setText("")
        # Duplikate entfernen basierend auf geladenen Tags
        self._deduplicate_songs()
        self.songTable.resizeColumnsToContents()
        self.songTable.setColumnHidden(0, True)
        total_sec = sum(s.get('duration_sec', 0) for s in self.songs)
        h, m, s = total_sec // 3600, (total_sec % 3600) // 60, total_sec % 60
        self.statusLabel.setText(f"{len(self.songs)} Songs | {h:02}:{m:02}:{s:02} Gesamtdauer")

    def _deduplicate_songs(self):
        """Entfernt Duplikate basierend auf Titel+Artist+Dauer, behält höchste Bitrate."""
        seen = {}
        unique = []
        for song in self.songs:
            if not song.get('tags_loaded'):
                unique.append(song)
                continue
            key = (normalize_text(song.get('title', '')),
                   normalize_text(song.get('artist', '')),
                   song.get('duration_sec', 0))
            # Prüfe ob ähnlicher Key existiert (±1 Sekunde)
            matched = False
            for (t, a, d) in list(seen.keys()):
                if t == key[0] and a == key[1] and abs(d - key[2]) <= 1:
                    if song.get('bitrate', 0) > seen[(t, a, d)].get('bitrate', 0):
                        # Ersetze mit besserer Qualität
                        idx = unique.index(seen[(t, a, d)])
                        unique[idx] = song
                        seen[(t, a, d)] = song
                    matched = True
                    break
            if not matched:
                seen[key] = song
                unique.append(song)

        if len(unique) < len(self.songs):
            removed = len(self.songs) - len(unique)
            self.songs = unique
            self._populate_song_table()
            self.statusLabel.setText(self.statusLabel.text() + f" | {removed} Duplikate entfernt")

    def _populate_song_table(self):
        self.songTable.setSortingEnabled(False)
        self.songTable.setRowCount(len(self.songs))
        for row, song in enumerate(self.songs):
            self.songTable.setItem(row, 0, QtWidgets.QTableWidgetItem(song['file_path']))
            self.songTable.setItem(row, 1, QtWidgets.QTableWidgetItem(song.get('title', '')))
            self.songTable.setItem(row, 2, QtWidgets.QTableWidgetItem(song.get('artist', '')))
            self.songTable.setItem(row, 3, QtWidgets.QTableWidgetItem(song.get('album', '')))
            self.songTable.setItem(row, 4, QtWidgets.QTableWidgetItem(song.get('year', '')))
            self.songTable.setItem(row, 5, QtWidgets.QTableWidgetItem(song.get('genre', '')))
            self.songTable.setItem(row, 6, QtWidgets.QTableWidgetItem(song.get('duration', '')))
        self.songTable.setSortingEnabled(True)
        self.songTable.resizeColumnsToContents()
        # Pfad-Spalte bleibt hidden
        self.songTable.setColumnHidden(0, True)

    # ==================== Player ====================

    def _toggle_play_pause(self):
        if self.player.state() == QMediaPlayer.PlayingState:
            self.pause_song()
        elif self.player.state() == QMediaPlayer.PausedState:
            self.player.play()
        else:
            self.play_selected()

    def play_selected(self):
        selected_songs = self._songs_from_selection()
        if not selected_songs:
            # Wenn Playlist existiert und pausiert, fortsetzen
            if self.player.state() == QMediaPlayer.PausedState:
                self.player.play()
                return
            if self.current_playlist:
                if 0 <= self.current_index < len(self.current_playlist):
                    self._play_song(self.current_playlist[self.current_index]['file_path'])
                return
            QMessageBox.warning(self, "Keine Auswahl", "Bitte wähle mindestens einen Song aus.")
            return
        self.current_playlist = selected_songs
        self.current_index = 0
        self.save_last_playlist()
        self._populate_playlist_table()
        self._play_song(self.current_playlist[self.current_index]['file_path'])

    def _song_table_double_clicked(self, index):
        row = index.row()
        song = self._song_from_row(row)
        if song:
            # Zur Playlist hinzufügen wenn nicht vorhanden, dann abspielen
            if song not in self.current_playlist:
                self.current_playlist.append(song)
            self.current_index = self.current_playlist.index(song)
            self.save_last_playlist()
            self._populate_playlist_table()
            self._play_song(song['file_path'])

    def play_from_playlist(self, index):
        row = index.row()
        if 0 <= row < len(self.current_playlist):
            self.current_index = row
            self._play_song(self.current_playlist[self.current_index]['file_path'])

    def _play_song(self, path):
        if not os.path.exists(path):
            QMessageBox.critical(self, "Fehler", f"Datei nicht gefunden:\n{path}")
            return
        self.player.setMedia(QMediaContent(QUrl.fromLocalFile(path)))
        self.player.play()
        self._update_now_playing(path)
        # Highlight in Playlist-Table
        if 0 <= self.current_index < self.playlistTable.rowCount():
            self.playlistTable.selectRow(self.current_index)

    def _update_now_playing(self, path):
        """Aktualisiert Cover und Now-Playing-Info."""
        meta = read_tags(path)
        title = meta.get('title') or os.path.splitext(os.path.basename(path))[0]
        artist = meta.get('artist', '')
        album = meta.get('album', '')

        self.nowTitle.setText(title)
        self.nowArtist.setText(artist)
        self.nowAlbum.setText(album)
        self.setWindowTitle(f"{'♪ ' + artist + ' - ' if artist else ''}{title} — Music Manager")

        # Cover laden
        cover_data = extract_cover(path)
        if cover_data:
            pixmap = QPixmap()
            pixmap.loadFromData(QtCore.QByteArray(cover_data))
            self.coverLabel.setPixmap(pixmap.scaled(
                220, 220, Qt.KeepAspectRatio, Qt.SmoothTransformation))
        else:
            self._set_cover_placeholder()

    def play_next(self):
        if not self.current_playlist:
            return
        if self.repeat_mode == REPEAT_ONE:
            self._play_song(self.current_playlist[self.current_index]['file_path'])
            return
        if self.current_index + 1 < len(self.current_playlist):
            self.current_index += 1
            self._play_song(self.current_playlist[self.current_index]['file_path'])
        elif self.repeat_mode == REPEAT_ALL:
            self.current_index = 0
            self._play_song(self.current_playlist[self.current_index]['file_path'])

    def play_previous(self):
        if not self.current_playlist:
            return
        # Wenn mehr als 3 Sekunden gespielt: Song neustarten
        if self.player.position() > 3000:
            self.player.setPosition(0)
            return
        if self.current_index > 0:
            self.current_index -= 1
            self._play_song(self.current_playlist[self.current_index]['file_path'])
        elif self.repeat_mode == REPEAT_ALL:
            self.current_index = len(self.current_playlist) - 1
            self._play_song(self.current_playlist[self.current_index]['file_path'])

    def pause_song(self):
        self.player.pause()

    def stop_song(self):
        self.player.stop()
        self.slider.setValue(0)
        self.timeLabel.setText("00:00 / 00:00")

    def cycle_repeat(self):
        self.repeat_mode = (self.repeat_mode + 1) % 3
        self.repeatButton.setText(REPEAT_LABELS[self.repeat_mode])

    def shuffle_playlist(self):
        if not self.current_playlist:
            return
        # Aktuellen Song merken
        current_song = None
        if 0 <= self.current_index < len(self.current_playlist):
            current_song = self.current_playlist[self.current_index]
        random.shuffle(self.current_playlist)
        # Aktuellen Song an Position 0 setzen
        if current_song and current_song in self.current_playlist:
            self.current_playlist.remove(current_song)
            self.current_playlist.insert(0, current_song)
            self.current_index = 0
        else:
            self.current_index = 0
        self.save_last_playlist()
        self._populate_playlist_table()

    def _seek_relative(self, ms):
        pos = self.player.position() + ms
        pos = max(0, min(pos, self.player.duration()))
        self.player.setPosition(pos)

    def set_position(self, position):
        self.player.setPosition(position)

    def set_volume(self, value):
        self.player.setVolume(value)
        if value == 0:
            self.volumeLabel.setText("🔇")
        elif value < 30:
            self.volumeLabel.setText("🔈")
        elif value < 70:
            self.volumeLabel.setText("🔉")
        else:
            self.volumeLabel.setText("🔊")

    def _update_position(self, position):
        self.slider.blockSignals(True)
        self.slider.setValue(position)
        self.slider.blockSignals(False)
        self._update_time_label()

    def _update_duration(self, duration):
        self.slider.setRange(0, duration)
        self._update_time_label()

    def _update_time_label(self):
        pos = self.player.position() // 1000
        dur = self.player.duration() // 1000
        self.timeLabel.setText(f"{pos//60:02}:{pos%60:02} / {dur//60:02}:{dur%60:02}")

    def _update_button_states(self, state):
        playing = state == QMediaPlayer.PlayingState
        stopped = state == QMediaPlayer.StoppedState
        self.playButton.setEnabled(not playing)
        self.pauseButton.setEnabled(playing)
        self.stopButton.setEnabled(not stopped)

    def _handle_media_status(self, status):
        if status == QMediaPlayer.EndOfMedia:
            if self.repeat_mode == REPEAT_ONE:
                self._play_song(self.current_playlist[self.current_index]['file_path'])
            elif self.current_playlist and self.current_index + 1 < len(self.current_playlist):
                self.current_index += 1
                self._play_song(self.current_playlist[self.current_index]['file_path'])
            elif self.repeat_mode == REPEAT_ALL and self.current_playlist:
                self.current_index = 0
                self._play_song(self.current_playlist[self.current_index]['file_path'])

    def _update_selection_status(self):
        selected_songs = self._songs_from_selection()
        if selected_songs:
            total_sec = sum(s.get('duration_sec', 0) for s in selected_songs)
            m, s = total_sec // 60, total_sec % 60
            self.selectionLabel.setText(f"{len(selected_songs)} ausgewählt | {m:02}:{s:02}")
        else:
            self.selectionLabel.setText("")

    # ==================== Cover ====================

    def _set_cover_placeholder(self):
        pixmap = QPixmap(220, 220)
        pixmap.fill(QColor("#1a1a2e"))
        painter = QtGui.QPainter(pixmap)
        painter.setPen(QColor("#444"))
        painter.setFont(QFont("Segoe UI", 72))
        painter.drawText(pixmap.rect(), Qt.AlignCenter, "♫")
        painter.end()
        self.coverLabel.setPixmap(pixmap)

    # ==================== Playlist ====================

    def add_to_playlist(self):
        selected_songs = self._songs_from_selection()
        if not selected_songs:
            QMessageBox.warning(self, "Keine Auswahl", "Bitte wähle Songs aus.")
            return
        added = 0
        for song in selected_songs:
            if not any(s['file_path'] == song['file_path'] for s in self.current_playlist):
                self.current_playlist.append(song)
                added += 1
        if added:
            self.save_last_playlist()
            self._populate_playlist_table()
            self.statusBar.showMessage(f"{added} Song(s) zur Playlist hinzugefügt", 3000)

    def remove_from_playlist(self):
        selected = self.playlistTable.selectionModel().selectedRows()
        if not selected:
            # Versuche Song-Table Selection
            song_sel = self._songs_from_selection()
            if not song_sel:
                return
            paths_to_remove = {s['file_path'] for s in song_sel}
            self.current_playlist = [s for s in self.current_playlist
                                    if s['file_path'] not in paths_to_remove]
        else:
            rows_to_remove = sorted([idx.row() for idx in selected], reverse=True)
            for row in rows_to_remove:
                if 0 <= row < len(self.current_playlist):
                    del self.current_playlist[row]

        # Index anpassen
        if self.current_index >= len(self.current_playlist):
            self.current_index = len(self.current_playlist) - 1

        self.save_last_playlist()
        self._populate_playlist_table()

    def _populate_playlist_table(self):
        self.playlistTable.setRowCount(len(self.current_playlist))
        for row, song in enumerate(self.current_playlist):
            self.playlistTable.setItem(row, 0, QtWidgets.QTableWidgetItem(song['file_path']))
            self.playlistTable.setItem(row, 1, QtWidgets.QTableWidgetItem(song.get('title', '') or os.path.splitext(os.path.basename(song['file_path']))[0]))
            self.playlistTable.setItem(row, 2, QtWidgets.QTableWidgetItem(song.get('artist', '')))
            self.playlistTable.setItem(row, 3, QtWidgets.QTableWidgetItem(song.get('album', '')))
            self.playlistTable.setItem(row, 4, QtWidgets.QTableWidgetItem(song.get('duration', '')))
            self.playlistTable.setItem(row, 5, QtWidgets.QTableWidgetItem(song.get('genre', '')))
            # YouTube Button
            yt_btn = QtWidgets.QPushButton("YT")
            yt_btn.setFixedWidth(36)
            yt_btn.setCursor(Qt.PointingHandCursor)
            yt_btn.clicked.connect(partial(self._open_youtube, song))
            self.playlistTable.setCellWidget(row, 6, yt_btn)
            # Spotify Button
            sp_btn = QtWidgets.QPushButton("SP")
            sp_btn.setFixedWidth(36)
            sp_btn.setCursor(Qt.PointingHandCursor)
            sp_btn.clicked.connect(partial(self._open_spotify, song))
            self.playlistTable.setCellWidget(row, 7, sp_btn)
            # Amazon Music Button
            am_btn = QtWidgets.QPushButton("AM")
            am_btn.setFixedWidth(36)
            am_btn.setCursor(Qt.PointingHandCursor)
            am_btn.clicked.connect(partial(self._open_amazon_music, song))
            self.playlistTable.setCellWidget(row, 8, am_btn)

            # Aktuell spielenden Song hervorheben
            if row == self.current_index:
                for col in range(self.playlistTable.columnCount()):
                    item = self.playlistTable.item(row, col)
                    if item:
                        item.setBackground(QColor("#0f3460"))

        self.playlistTable.resizeColumnsToContents()
        self.playlistTable.setColumnHidden(0, True)

    def export_playlist(self):
        if not self.current_playlist:
            QMessageBox.warning(self, "Keine Playlist", "Die Playlist ist leer.")
            return
        name, ok = QInputDialog.getText(self, "Playlistname", "Name für die Playlist:")
        if not ok or not name.strip():
            return
        name = name.strip()

        # Frage ob Songs kopiert werden sollen
        copy_files = QMessageBox.question(
            self, "Songs kopieren?",
            "Sollen die Audio-Dateien in den Export-Ordner kopiert werden?\n\n"
            "Ja = Songs + M3U + Titelliste\n"
            "Nein = Nur M3U + Titelliste (Referenzen)",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        ) == QMessageBox.Yes

        export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{name}_export")
        try:
            if not os.path.exists(export_dir):
                os.makedirs(export_dir)

            if copy_files:
                import shutil
                for song in self.current_playlist:
                    src = song['file_path']
                    if os.path.exists(src):
                        dst = os.path.join(export_dir, os.path.basename(src))
                        if not os.path.exists(dst):
                            shutil.copy2(src, dst)

            # M3U
            m3u_path = os.path.join(export_dir, f"{name}.m3u")
            with open(m3u_path, 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                for song in self.current_playlist:
                    artist = song.get('artist', '')
                    title = song.get('title', '')
                    dur = song.get('duration_sec', -1)
                    f.write(f"#EXTINF:{dur},{artist} - {title}\n")
                    if copy_files:
                        f.write(os.path.basename(song['file_path']) + "\n")
                    else:
                        f.write(song['file_path'] + "\n")

            # Auch in PLAYLIST_DIR speichern
            if not os.path.exists(PLAYLIST_DIR):
                os.makedirs(PLAYLIST_DIR)
            playlist_m3u = os.path.join(PLAYLIST_DIR, f"{name}.m3u")
            with open(playlist_m3u, 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                for song in self.current_playlist:
                    artist = song.get('artist', '')
                    title = song.get('title', '')
                    dur = song.get('duration_sec', -1)
                    f.write(f"#EXTINF:{dur},{artist} - {title}\n{song['file_path']}\n")

            # Titelliste für Streaming-Dienste (TuneMyMusic, Soundiiz, etc.)
            txt_path = os.path.join(export_dir, f"{name}_titles.txt")
            with open(txt_path, 'w', encoding='utf-8') as f:
                for song in self.current_playlist:
                    line = f"{song.get('artist', '')} - {song.get('title', '')}"
                    if line.strip() != '-':
                        f.write(line + "\n")

            # CSV für Playlist-Import-Services (TuneMyMusic, Soundiiz, FreeYourMusic)
            csv_path = os.path.join(export_dir, f"{name}_import.csv")
            with open(csv_path, 'w', encoding='utf-8') as f:
                f.write("Title,Artist,Album,Duration\n")
                for song in self.current_playlist:
                    title = song.get('title', '').replace('"', '""')
                    artist = song.get('artist', '').replace('"', '""')
                    album = song.get('album', '').replace('"', '""')
                    duration = song.get('duration', '')
                    f.write(f'"{title}","{artist}","{album}","{duration}"\n')

            QMessageBox.information(self, "Export",
                f"Playlist exportiert nach:\n{export_dir}\n\n"
                f"Dateien:\n"
                f"  {name}.m3u — Standard-Playlist\n"
                f"  {name}_titles.txt — Titelliste (Artist - Title)\n"
                f"  {name}_import.csv — CSV für TuneMyMusic/Soundiiz\n\n"
                f"Für Amazon Music / Spotify / YouTube Music:\n"
                f"1. Öffne tunemymusic.com oder soundiiz.com\n"
                f"2. Wähle 'Import from File'\n"
                f"3. Lade die CSV oder TXT-Datei hoch\n"
                f"4. Wähle Amazon Music als Ziel")
            self.update_playlist_dropdown()
        except Exception as e:
            QMessageBox.critical(self, "Fehler", f"Export-Fehler: {e}")

    def import_playlist(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Playlist öffnen", PLAYLIST_DIR, "Playlists (*.m3u *.m3u8)")
        if not path:
            return
        self._import_playlist_from_path(path)

    def _import_playlist_from_path(self, path):
        playlist = []
        try:
            with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        file_path = os.path.normpath(line)
                        if os.path.exists(file_path):
                            meta = {'file_path': file_path}
                            tags = read_tags(file_path)
                            meta.update(tags)
                            meta['tags_loaded'] = True
                            playlist.append(meta)
            if playlist:
                self.current_playlist = playlist
                self.current_index = 0
                self.save_last_playlist()
                self._populate_playlist_table()
                self._play_song(self.current_playlist[0]['file_path'])
                self.statusBar.showMessage(f"{len(playlist)} Songs geladen", 3000)
            else:
                QMessageBox.warning(self, "Leere Playlist", "Keine gültigen Songs in der Playlist gefunden.")
        except Exception as e:
            QMessageBox.critical(self, "Fehler", f"Import-Fehler: {e}")

    def update_playlist_dropdown(self):
        self.playlistDropdown.blockSignals(True)
        self.playlistDropdown.clear()
        self.playlistDropdown.addItem("<Letzte Playlist>")
        if os.path.exists(PLAYLIST_DIR):
            for fname in sorted(os.listdir(PLAYLIST_DIR)):
                if fname.lower().endswith(('.m3u', '.m3u8')):
                    self.playlistDropdown.addItem(fname)
        self.playlistDropdown.blockSignals(False)

    def dropdown_playlist_selected(self, idx):
        if idx == 0:
            self.load_last_playlist()
        else:
            fname = self.playlistDropdown.currentText()
            path = os.path.join(PLAYLIST_DIR, fname)
            if os.path.exists(path):
                self._import_playlist_from_path(path)

    def save_last_playlist(self):
        if not os.path.exists(PLAYLIST_DIR):
            os.makedirs(PLAYLIST_DIR)
        try:
            with open(LAST_PLAYLIST_PATH, 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                for song in self.current_playlist:
                    artist = song.get('artist', '')
                    title = song.get('title', '')
                    dur = song.get('duration_sec', -1)
                    f.write(f"#EXTINF:{dur},{artist} - {title}\n{song['file_path']}\n")
        except Exception as e:
            print(f"Playlist-Speicherfehler: {e}", file=sys.stderr)

    def load_last_playlist(self):
        if not os.path.exists(LAST_PLAYLIST_PATH):
            self.current_playlist = []
            self.current_index = -1
            self._populate_playlist_table()
            return
        playlist = []
        try:
            with open(LAST_PLAYLIST_PATH, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        file_path = os.path.normpath(line)
                        meta = {'file_path': file_path, 'title': '', 'artist': '',
                                'album': '', 'duration': '', 'genre': '',
                                'year': '', 'duration_sec': 0, 'bitrate': 0,
                                'tags_loaded': False}
                        if os.path.exists(file_path):
                            tags = read_tags(file_path)
                            meta.update(tags)
                            meta['tags_loaded'] = True
                        playlist.append(meta)
            self.current_playlist = playlist
            self.current_index = 0 if playlist else -1
            self._populate_playlist_table()
        except Exception as e:
            print(f"Playlist-Ladefehler: {e}", file=sys.stderr)
            self.current_playlist = []
            self.current_index = -1
            self._populate_playlist_table()

    # ==================== Drag & Drop ====================

    def _playlist_drag_enter(self, event):
        if event.mimeData().hasFormat('application/x-qabstractitemmodeldatalist'):
            event.acceptProposedAction()
        else:
            event.ignore()

    def _playlist_drag_move(self, event):
        if event.mimeData().hasFormat('application/x-qabstractitemmodeldatalist'):
            event.acceptProposedAction()
        else:
            event.ignore()

    def _playlist_drop(self, event):
        if event.mimeData().hasFormat('application/x-qabstractitemmodeldatalist'):
            selected_songs = self._songs_from_selection()
            added = 0
            for song in selected_songs:
                if not any(s['file_path'] == song['file_path'] for s in self.current_playlist):
                    self.current_playlist.append(song)
                    added += 1
            if added:
                self.save_last_playlist()
                self._populate_playlist_table()
                self.statusBar.showMessage(f"{added} Song(s) per Drag & Drop hinzugefügt", 3000)
            event.acceptProposedAction()
        else:
            event.ignore()

    # ==================== Kontextmenü ====================

    def show_song_context_menu(self, pos):
        index = self.songTable.indexAt(pos)
        if not index.isValid():
            return
        row = index.row()
        song = self._song_from_row(row)
        if not song:
            return

        menu = QMenu(self)
        menu.setStyleSheet("""
            QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #333; }
            QMenu::item:selected { background: #0f3460; }
        """)
        action_play = menu.addAction("▶ Abspielen")
        action_add = menu.addAction("+ Zur Playlist")
        menu.addSeparator()
        action_explorer = menu.addAction("📁 Im Explorer anzeigen")
        action_path = menu.addAction("📋 Pfad kopieren")
        menu.addSeparator()
        action_tags = menu.addAction("🏷 MP3-Tags bearbeiten")
        action_mb = menu.addAction("🌐 Tags online suchen (MusicBrainz)")
        action_yt = menu.addAction("🔍 YouTube suchen")
        action_sp = menu.addAction("🔍 Spotify suchen")
        action_am = menu.addAction("🔍 Amazon Music suchen")

        action = menu.exec_(self.songTable.viewport().mapToGlobal(pos))
        if action == action_play:
            self.current_playlist = [song]
            self.current_index = 0
            self.save_last_playlist()
            self._populate_playlist_table()
            self._play_song(song['file_path'])
        elif action == action_add:
            if not any(s['file_path'] == song['file_path'] for s in self.current_playlist):
                self.current_playlist.append(song)
                self.save_last_playlist()
                self._populate_playlist_table()
        elif action == action_explorer:
            self._open_in_explorer(song['file_path'])
        elif action == action_path:
            QtWidgets.QApplication.clipboard().setText(song['file_path'])
            self.statusBar.showMessage("Pfad kopiert", 2000)
        elif action == action_tags:
            self._edit_tags(song)
        elif action == action_mb:
            self._mb_lookup_single(song)
        elif action == action_yt:
            self._open_youtube(song)
        elif action == action_sp:
            self._open_spotify(song)
        elif action == action_am:
            self._open_amazon_music(song)

    def show_playlist_context_menu(self, pos):
        index = self.playlistTable.indexAt(pos)
        if not index.isValid():
            return
        row = index.row()
        if row >= len(self.current_playlist):
            return
        song = self.current_playlist[row]

        menu = QMenu(self)
        menu.setStyleSheet("""
            QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #333; }
            QMenu::item:selected { background: #0f3460; }
        """)
        action_play = menu.addAction("▶ Abspielen")
        action_remove = menu.addAction("✕ Aus Playlist entfernen")
        menu.addSeparator()
        action_explorer = menu.addAction("📁 Im Explorer anzeigen")
        action_path = menu.addAction("📋 Pfad kopieren")
        menu.addSeparator()
        action_yt = menu.addAction("🔍 YouTube suchen")
        action_sp = menu.addAction("🔍 Spotify suchen")
        action_am = menu.addAction("🔍 Amazon Music suchen")

        action = menu.exec_(self.playlistTable.viewport().mapToGlobal(pos))
        if action == action_play:
            self.current_index = row
            self._play_song(song['file_path'])
        elif action == action_remove:
            del self.current_playlist[row]
            if self.current_index >= len(self.current_playlist):
                self.current_index = len(self.current_playlist) - 1
            self.save_last_playlist()
            self._populate_playlist_table()
        elif action == action_explorer:
            self._open_in_explorer(song['file_path'])
        elif action == action_path:
            QtWidgets.QApplication.clipboard().setText(song['file_path'])
            self.statusBar.showMessage("Pfad kopiert", 2000)
        elif action == action_yt:
            self._open_youtube(song)
        elif action == action_sp:
            self._open_spotify(song)
        elif action == action_am:
            self._open_amazon_music(song)

    def _open_in_explorer(self, path):
        if os.path.exists(path):
            if sys.platform.startswith('win'):
                import subprocess
                subprocess.Popen(['explorer', '/select,', os.path.normpath(path)])
            else:
                folder = os.path.dirname(path)
                QtGui.QDesktopServices.openUrl(QUrl.fromLocalFile(folder))
        else:
            QMessageBox.warning(self, "Fehler", f"Datei nicht gefunden:\n{path}")

    def _edit_tags(self, song):
        try:
            # Stoppe Wiedergabe falls dieser Song läuft
            current_url = self.player.media().canonicalUrl().toLocalFile() if self.player.media() else None
            if current_url and os.path.abspath(current_url) == os.path.abspath(song['file_path']):
                self.player.stop()

            # Tags lesen
            tags = read_tags(song['file_path'])
            song_copy = dict(song)
            song_copy.update(tags)

            dlg = TagEditorDialog(song_copy, self)
            if dlg.exec_() == QtWidgets.QDialog.Accepted:
                new_tags = dlg.get_tags()
                try:
                    audio = mutagen.File(song['file_path'], easy=True)
                    if audio is not None:
                        audio['title'] = new_tags['title']
                        audio['artist'] = new_tags['artist']
                        audio['album'] = new_tags['album']
                        if new_tags['year']:
                            audio['date'] = new_tags['year']
                        if new_tags['genre']:
                            audio['genre'] = new_tags['genre']
                        audio.save()

                        # Neu einlesen und in-memory aktualisieren
                        updated = read_tags(song['file_path'])
                        updated['tags_loaded'] = True
                        song.update(updated)

                        self._populate_song_table()
                        self._populate_playlist_table()
                        self.statusBar.showMessage("Tags gespeichert", 3000)
                except Exception as e:
                    QMessageBox.critical(self, "Fehler", f"Fehler beim Speichern: {e}")
        except Exception as e:
            QMessageBox.critical(self, "Fehler", f"Fehler beim Tag-Editor: {e}")

    # ==================== MusicBrainz ====================

    def _mb_lookup_single(self, song):
        """MusicBrainz-Lookup für einen einzelnen Song mit Ergebnis-Dialog."""
        artist = song.get('artist', '')
        title = song.get('title', '')
        if not title:
            fname = os.path.splitext(os.path.basename(song.get('file_path', '')))[0]
            if ' - ' in fname:
                artist, title = fname.split(' - ', 1)
                artist = artist.strip()
                title = title.strip()
            else:
                title = fname.strip()

        self.statusBar.showMessage(f"MusicBrainz: Suche '{artist} - {title}'...", 5000)
        QtWidgets.QApplication.processEvents()

        results = mb_search_recording(artist, title, song.get('duration_sec', 0))
        if not results:
            QMessageBox.information(self, "MusicBrainz", "Keine Ergebnisse gefunden.")
            return

        dlg = MusicBrainzResultDialog(results, song, self)
        if dlg.exec_() == QtWidgets.QDialog.Accepted:
            idx = dlg.get_selected_index()
            if idx is not None and idx < len(results):
                match = results[idx]
                self._apply_mb_tags(song, match)
                QMessageBox.information(self, "Tags gespeichert",
                    f"Tags wurden in die Datei geschrieben:\n\n"
                    f"Titel: {match.get('mb_title', '')}\n"
                    f"Interpret: {match.get('mb_artist', '')}\n"
                    f"Album: {match.get('mb_album', '')}\n"
                    f"Jahr: {match.get('mb_year', '')}\n"
                    f"Genre: {match.get('mb_genre', '')}")

    def _mb_batch_lookup(self):
        """Batch-Lookup für alle Songs mit fehlenden Tags."""
        if not self.songs:
            QMessageBox.warning(self, "Keine Songs", "Bitte zuerst eine Suche durchführen.")
            return

        # Finde Songs mit fehlenden Tags
        candidates = []
        for i, song in enumerate(self.songs):
            if not song.get('tags_loaded'):
                continue
            # Song braucht Update wenn Titel ODER Artist leer
            title = song.get('title', '').strip()
            artist = song.get('artist', '').strip()
            if not title or not artist:
                candidates.append((i, song))

        if not candidates:
            # Frage ob trotzdem alle aktualisiert werden sollen
            reply = QMessageBox.question(
                self, "Alle Tags vorhanden",
                f"Alle {len(self.songs)} Songs haben bereits Titel und Interpret.\n\n"
                "Trotzdem alle online nachschlagen und aktualisieren?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if reply != QMessageBox.Yes:
                return
            candidates = [(i, s) for i, s in enumerate(self.songs) if s.get('tags_loaded')]

        # Bestätigung
        reply = QMessageBox.question(
            self, "MusicBrainz Batch-Lookup",
            f"{len(candidates)} Songs werden online nachgeschlagen.\n"
            f"(ca. {len(candidates)} Sekunden, Rate-Limit: 1/Sek)\n\n"
            "Tags mit Score >= 80% werden automatisch übernommen und gespeichert.\n\n"
            "Fortfahren?",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply != QMessageBox.Yes:
            return

        self.autoTagButton.setEnabled(False)
        self.autoTagButton.setText("Suche läuft...")

        self._mb_batch_worker = MusicBrainzBatchWorker(candidates)
        self._mb_batch_worker.song_updated.connect(self._on_mb_batch_match)
        self._mb_batch_worker.progress.connect(self._on_mb_batch_progress)
        self._mb_batch_worker.finished_batch.connect(self._on_mb_batch_finished)
        self._mb_batch_worker.start()

    def _on_mb_batch_match(self, row_idx, match):
        """Ein Song wurde via Batch-Lookup gefunden — Tags anwenden."""
        if row_idx < len(self.songs):
            self._apply_mb_tags(self.songs[row_idx], match)

    def _on_mb_batch_progress(self, current, total, status):
        self.tagProgressLabel.setText(f"MB: {current}/{total}")
        self.statusBar.showMessage(f"MusicBrainz: {status}", 2000)

    def _on_mb_batch_finished(self, updated, skipped):
        self.autoTagButton.setEnabled(True)
        self.autoTagButton.setText("Auto-Tag (MB)")
        self.tagProgressLabel.setText("")
        QMessageBox.information(
            self, "MusicBrainz Batch",
            f"Fertig!\n\n"
            f"Aktualisiert: {updated} Songs\n"
            f"Übersprungen: {skipped} Songs (Score < 80% oder nicht gefunden)")

    def _apply_mb_tags(self, song, match):
        """Wendet MusicBrainz-Tags auf einen Song an und speichert in die Datei."""
        file_path = song.get('file_path', '')
        if not os.path.exists(file_path):
            return

        # Stoppe Wiedergabe falls dieser Song läuft
        current_url = self.player.media().canonicalUrl().toLocalFile() if self.player.media() else None
        if current_url and os.path.abspath(current_url) == os.path.abspath(file_path):
            self.player.stop()

        try:
            audio = mutagen.File(file_path, easy=True)
            if audio is None:
                return

            if match.get('mb_title'):
                audio['title'] = match['mb_title']
            if match.get('mb_artist'):
                audio['artist'] = match['mb_artist']
            if match.get('mb_album'):
                audio['album'] = match['mb_album']
            if match.get('mb_year'):
                audio['date'] = match['mb_year']
            if match.get('mb_genre'):
                audio['genre'] = match['mb_genre']
            audio.save()

            # In-Memory-Daten aktualisieren
            updated = read_tags(file_path)
            updated['tags_loaded'] = True
            song.update(updated)

            # Tabelle aktualisieren (visuelle Zeile finden)
            visual_row = self._find_visual_row(file_path)
            if visual_row >= 0:
                self.songTable.setSortingEnabled(False)
                self.songTable.setItem(visual_row, 1, QtWidgets.QTableWidgetItem(song.get('title', '')))
                self.songTable.setItem(visual_row, 2, QtWidgets.QTableWidgetItem(song.get('artist', '')))
                self.songTable.setItem(visual_row, 3, QtWidgets.QTableWidgetItem(song.get('album', '')))
                self.songTable.setItem(visual_row, 4, QtWidgets.QTableWidgetItem(song.get('year', '')))
                self.songTable.setItem(visual_row, 5, QtWidgets.QTableWidgetItem(song.get('genre', '')))
                self.songTable.setItem(visual_row, 6, QtWidgets.QTableWidgetItem(song.get('duration', '')))
                self.songTable.setSortingEnabled(True)

            self.statusBar.showMessage(
                f"Tags gespeichert: {match.get('mb_artist', '')} - {match.get('mb_title', '')}", 5000)
        except Exception as e:
            QMessageBox.critical(self, "Tag-Fehler",
                f"Fehler beim Speichern der Tags:\n{e}\n\nDatei: {file_path}")

    # ==================== Filter ====================

    def filter_table(self):
        text = self.filterEdit.text().strip().lower()
        for row in range(self.songTable.rowCount()):
            if not text:
                self.songTable.setRowHidden(row, False)
                continue
            row_text = " ".join(
                self.songTable.item(row, col).text().lower()
                if self.songTable.item(row, col) else ''
                for col in range(self.songTable.columnCount())
            )
            self.songTable.setRowHidden(row, text not in row_text)

    # ==================== External Links ====================

    def _open_youtube(self, song):
        query = f"{song.get('artist', '')} {song.get('title', '')}".strip()
        if not query:
            query = os.path.splitext(os.path.basename(song['file_path']))[0]
        url = f"https://www.youtube.com/results?search_query={urllib.parse.quote(query)}"
        threading.Thread(target=webbrowser.open_new_tab, args=(url,), daemon=True).start()

    def _open_spotify(self, song):
        query = f"{song.get('artist', '')} {song.get('title', '')}".strip()
        if not query:
            query = os.path.splitext(os.path.basename(song['file_path']))[0]
        url = f"https://open.spotify.com/search/{urllib.parse.quote(query)}"
        threading.Thread(target=webbrowser.open_new_tab, args=(url,), daemon=True).start()

    def _open_amazon_music(self, song):
        query = f"{song.get('artist', '')} {song.get('title', '')}".strip()
        if not query:
            query = os.path.splitext(os.path.basename(song['file_path']))[0]
        url = f"https://music.amazon.de/search/{urllib.parse.quote(query)}"
        threading.Thread(target=webbrowser.open_new_tab, args=(url,), daemon=True).start()

    # ==================== Fenster schließen ====================

    def closeEvent(self, event):
        self._cancel_tag_worker()
        if self._mb_batch_worker and self._mb_batch_worker.isRunning():
            self._mb_batch_worker.cancel()
            self._mb_batch_worker.wait(2000)
        self.settings.setValue("geometry", self.saveGeometry())
        self.settings.setValue("v_splitter", self.vSplitter.sizes())
        self.settings.setValue("h_splitter", self.hSplitter.sizes())
        self.settings.setValue("volume", self.volumeSlider.value())
        self.player.stop()
        event.accept()


class TagEditorDialog(QtWidgets.QDialog):
    def __init__(self, song, parent=None):
        super().__init__(parent)
        self.setWindowTitle("MP3-Tags bearbeiten")
        self.setMinimumWidth(450)
        self.song = song
        self.setStyleSheet("""
            QDialog { background: #1a1a2e; color: #e0e0e0; }
            QLabel { color: #aab; font-size: 12px; }
            QLineEdit { background: #16213e; color: #e0e0e0; border: 1px solid #333;
                        border-radius: 4px; padding: 4px 8px; font-size: 13px; }
            QLineEdit:focus { border: 1px solid #e94560; }
            QPushButton { background: #e94560; color: white; border: none;
                         padding: 6px 12px; border-radius: 4px; font-weight: bold; }
            QPushButton:hover { background: #ff6b6b; }
        """)

        layout = QtWidgets.QFormLayout(self)

        file_path = song.get('file_path', '')
        file_name = os.path.basename(file_path)
        folder_name = os.path.basename(os.path.dirname(file_path))
        self.fileLabel = QtWidgets.QLabel(file_name)
        self.fileLabel.setStyleSheet("font-weight: bold; color: #e0e0ff;")
        self.folderLabel = QtWidgets.QLabel(folder_name)
        layout.addRow("Datei:", self.fileLabel)
        layout.addRow("Ordner:", self.folderLabel)

        self.titleEdit = QtWidgets.QLineEdit(song.get('title', ''))
        self.artistEdit = QtWidgets.QLineEdit(song.get('artist', ''))
        self.albumEdit = QtWidgets.QLineEdit(song.get('album', ''))
        self.yearEdit = QtWidgets.QLineEdit(song.get('year', ''))
        self.genreEdit = QtWidgets.QLineEdit(song.get('genre', ''))

        self.fromFilenameButton = QtWidgets.QPushButton("Tags aus Dateiname übernehmen")
        self.fromFilenameButton.clicked.connect(self._fill_from_filename)
        self.mbSearchButton = QtWidgets.QPushButton("Tags online suchen (MusicBrainz)")
        self.mbSearchButton.clicked.connect(self._search_musicbrainz)
        layout.addRow(self.fromFilenameButton)
        layout.addRow(self.mbSearchButton)
        layout.addRow("Titel:", self.titleEdit)
        layout.addRow("Interpret:", self.artistEdit)
        layout.addRow("Album:", self.albumEdit)
        layout.addRow("Jahr:", self.yearEdit)
        layout.addRow("Genre:", self.genreEdit)

        btnLayout = QtWidgets.QHBoxLayout()
        btnLayout.addStretch()
        cancelBtn = QtWidgets.QPushButton("Abbrechen")
        cancelBtn.setStyleSheet("background: #444;")
        cancelBtn.clicked.connect(self.reject)
        saveBtn = QtWidgets.QPushButton("Speichern")
        saveBtn.clicked.connect(self.accept)
        saveBtn.setDefault(True)
        btnLayout.addWidget(cancelBtn)
        btnLayout.addWidget(saveBtn)
        layout.addRow(btnLayout)

    def _fill_from_filename(self):
        file_path = self.song.get('file_path', '')
        name = os.path.splitext(os.path.basename(file_path))[0]
        folder = os.path.basename(os.path.dirname(file_path))
        if ' - ' in name:
            artist, title = name.split(' - ', 1)
            self.artistEdit.setText(artist.strip())
            self.titleEdit.setText(title.strip())
        else:
            self.titleEdit.setText(name.strip())
        self.albumEdit.setText(folder)

    def _search_musicbrainz(self):
        """MusicBrainz-Suche aus dem Tag-Editor heraus."""
        artist = self.artistEdit.text().strip()
        title = self.titleEdit.text().strip()
        if not title:
            # Versuche aus Dateiname
            fname = os.path.splitext(os.path.basename(self.song.get('file_path', '')))[0]
            if ' - ' in fname:
                artist, title = fname.split(' - ', 1)
            else:
                title = fname

        self.mbSearchButton.setEnabled(False)
        self.mbSearchButton.setText("Suche...")
        QtWidgets.QApplication.processEvents()

        results = mb_search_recording(artist.strip(), title.strip())

        self.mbSearchButton.setEnabled(True)
        self.mbSearchButton.setText("Tags online suchen (MusicBrainz)")

        if not results:
            QMessageBox.information(self, "MusicBrainz", "Keine Ergebnisse gefunden.")
            return

        dlg = MusicBrainzResultDialog(results, self.song, self)
        if dlg.exec_() == QtWidgets.QDialog.Accepted:
            idx = dlg.get_selected_index()
            if idx is not None and idx < len(results):
                match = results[idx]
                if match.get('mb_title'):
                    self.titleEdit.setText(match['mb_title'])
                if match.get('mb_artist'):
                    self.artistEdit.setText(match['mb_artist'])
                if match.get('mb_album'):
                    self.albumEdit.setText(match['mb_album'])
                if match.get('mb_year'):
                    self.yearEdit.setText(match['mb_year'])
                if match.get('mb_genre'):
                    self.genreEdit.setText(match['mb_genre'])

    def get_tags(self):
        return {
            'title': self.titleEdit.text(),
            'artist': self.artistEdit.text(),
            'album': self.albumEdit.text(),
            'year': self.yearEdit.text(),
            'genre': self.genreEdit.text(),
        }


def main():
    if len(sys.argv) < 2:
        print("Usage: {} <db_path>".format(sys.argv[0]), file=sys.stderr)
        sys.exit(1)
    db_path = sys.argv[1]

    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('Fusion')
    player = SongPlayer(db_path)
    player.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
