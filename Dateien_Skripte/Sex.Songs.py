#!/usr/bin/env python3
import sys
import sqlite3
import os
import tempfile
import webbrowser
import random
import threading
import urllib.parse
from functools import partial
from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtMultimedia import QMediaPlayer, QMediaContent
from PyQt5.QtCore import QUrl
from mutagen.mp3 import MP3
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3, APIC
from PyQt5.QtGui import QPixmap
from PyQt5.QtCore import QByteArray
import re

PLAYLIST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'playlists')
LAST_PLAYLIST_PATH = os.path.join(PLAYLIST_DIR, 'last_playlist.m3u')

class SongPlayer(QtWidgets.QWidget):
    def __init__(self, db_path):
        super().__init__()
        self.setWindowTitle("Wow! Sexy MP3 Player")
        _icon = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'icons', 'music.ico')
        if os.path.exists(_icon):
            self.setWindowIcon(QtGui.QIcon(_icon))
        screen = QtWidgets.QApplication.desktop().availableGeometry(self)
        width = screen.width() // 3
        height = screen.height()
        x = screen.x() + screen.width() - width
        y = screen.y()
        self.move(x, y)
        self.resize(width, height)
        self.db_path = db_path
        self.songs = []  # Liste von Dicts mit Metadaten
        self.current_playlist = []
        self.current_index = -1

        # Erweiterte Suchfelder
        self.titleSearch = QtWidgets.QLineEdit()
        self.titleSearch.setPlaceholderText("Titel")
        self.artistSearch = QtWidgets.QLineEdit()
        self.artistSearch.setPlaceholderText("Interpret")
        self.albumSearch = QtWidgets.QLineEdit()
        self.albumSearch.setPlaceholderText("Album")
        self.yearSearch = QtWidgets.QLineEdit()
        self.yearSearch.setPlaceholderText("Jahr")
        self.genreSearch = QtWidgets.QLineEdit()
        self.genreSearch.setPlaceholderText("Genre")

        # Suchfeld
        self.searchEdit = QtWidgets.QLineEdit()
        self.searchEdit.setPlaceholderText("Suchbegriff für MP3-Dateien...")
        self.searchButton = QtWidgets.QPushButton("Suchen")
        self.searchButton.clicked.connect(self.search_songs)

        # Filterfeld für die angezeigten Ergebnisse
        self.filterEdit = QtWidgets.QLineEdit()
        self.filterEdit.setPlaceholderText("Filter in Ergebnissen...")
        self.filterEdit.textChanged.connect(self.filter_table)

        # Layout für erweiterte Suche
        advancedSearchLayout = QtWidgets.QHBoxLayout()
        advancedSearchLayout.addWidget(self.titleSearch)
        advancedSearchLayout.addWidget(self.artistSearch)
        advancedSearchLayout.addWidget(self.albumSearch)
        advancedSearchLayout.addWidget(self.yearSearch)
        advancedSearchLayout.addWidget(self.genreSearch)

        # Media Player
        self.player = QMediaPlayer()
        self.player.positionChanged.connect(self.update_position)
        self.player.durationChanged.connect(self.update_duration)
        self.player.stateChanged.connect(self.update_buttons)
        self.player.setVolume(50)
        self.player.mediaStatusChanged.connect(self.handle_media_status)

        # Song Table
        self.songTable = QtWidgets.QTableWidget()
        self.songTable.setColumnCount(5)
        self.songTable.setHorizontalHeaderLabels(["Datei", "Titel", "Interpret", "Album", "Dauer"])
        self.songTable.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.songTable.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)
        self.songTable.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.songTable.horizontalHeader().setStretchLastSection(True)
        self.songTable.setRowCount(0)  # Start: leer
        self.songTable.setSortingEnabled(True)  # Sortierbar
        self.songTable.setDragEnabled(True)
        self.songTable.setDragDropMode(QtWidgets.QAbstractItemView.DragOnly)
        self.songTable.horizontalHeader().setStyleSheet("QHeaderView::section { background-color: #223366; color: white; font-weight: bold; }")
        self.songTable.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.songTable.customContextMenuRequested.connect(self.show_song_context_menu)

        # Cover-Anzeige
        self.coverLabel = QtWidgets.QLabel()
        self.coverLabel.setFixedSize(180, 180)
        self.coverLabel.setAlignment(QtCore.Qt.AlignCenter)
        self.coverLabel.setStyleSheet("background: #223366; border: 2px solid #fff; border-radius: 10px;")
        self.set_cover_placeholder()

        # Player Controls
        self.playButton = QtWidgets.QPushButton("▶")
        self.pauseButton = QtWidgets.QPushButton("⏸")
        self.stopButton = QtWidgets.QPushButton("⏹")
        self.nextButton = QtWidgets.QPushButton("⏭")
        self.shuffleButton = QtWidgets.QPushButton("Shuffle")
        self.playButton.clicked.connect(self.play_selected)
        self.pauseButton.clicked.connect(self.pause_song)
        self.stopButton.clicked.connect(self.stop_song)
        self.nextButton.clicked.connect(self.play_next)
        self.shuffleButton.clicked.connect(self.shuffle_playlist)

        # Fortschrittsanzeige
        self.slider = QtWidgets.QSlider(QtCore.Qt.Horizontal)
        self.slider.setRange(0, 0)
        self.slider.sliderMoved.connect(self.set_position)
        self.timeLabel = QtWidgets.QLabel("00:00 / 00:00")

        # Lautstärke
        self.volumeSlider = QtWidgets.QSlider(QtCore.Qt.Horizontal)
        self.volumeSlider.setRange(0, 100)
        self.volumeSlider.setValue(50)
        self.volumeSlider.setFixedWidth(120)
        self.volumeSlider.valueChanged.connect(self.set_volume)
        self.volumeLabel = QtWidgets.QLabel("🔊")

        # Playlist-Buttons
        self.addToPlaylistButton = QtWidgets.QPushButton("Zur Playlist hinzufügen")
        self.exportPlaylistButton = QtWidgets.QPushButton("Playlist exportieren (M3U)")
        self.importPlaylistButton = QtWidgets.QPushButton("Playlist öffnen (M3U)")
        self.removeFromPlaylistButton = QtWidgets.QPushButton("Aus Playlist entfernen")
        self.addToPlaylistButton.clicked.connect(self.add_to_playlist)
        self.exportPlaylistButton.clicked.connect(self.export_playlist)
        self.importPlaylistButton.clicked.connect(self.import_playlist)
        self.removeFromPlaylistButton.clicked.connect(self.remove_from_playlist)

        # Playlist Table (Anzeige der aktuellen Playlist)
        self.playlistTable = QtWidgets.QTableWidget()
        self.playlistTable.setColumnCount(7)
        self.playlistTable.setHorizontalHeaderLabels(["Datei", "Titel", "Interpret", "Album", "Dauer", "YouTube", "Spotify"])
        self.playlistTable.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.playlistTable.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.playlistTable.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.playlistTable.horizontalHeader().setStretchLastSection(True)
        self.playlistTable.setFixedHeight(180)
        self.playlistTable.doubleClicked.connect(self.play_from_playlist)
        self.playlistTable.setSortingEnabled(True)  # Sortierbar
        self.playlistTable.setAcceptDrops(True)
        self.playlistTable.setDropIndicatorShown(True)
        self.playlistTable.setDragDropMode(QtWidgets.QAbstractItemView.DropOnly)
        self.playlistTable.horizontalHeader().setStyleSheet("QHeaderView::section { background-color: #223366; color: white; font-weight: bold; }")

        # Drag & Drop Unterstützung für Playlist-Tabelle
        self.playlistTable.dragEnterEvent = self.playlist_drag_enter_event
        self.playlistTable.dragMoveEvent = self.playlist_drag_move_event
        self.playlistTable.dropEvent = self.playlist_drop_event

        # Playlist-Auswahl Dropdown
        self.playlistDropdown = QtWidgets.QComboBox()
        self.playlistDropdown.setMinimumWidth(200)
        self.playlistDropdown.addItem("<Letzte Playlist>")
        self.update_playlist_dropdown()
        self.playlistDropdown.currentIndexChanged.connect(self.dropdown_playlist_selected)

        # Layouts
        searchLayout = QtWidgets.QHBoxLayout()
        searchLayout.addWidget(self.searchEdit)
        searchLayout.addWidget(self.searchButton)
        searchLayout.addWidget(self.filterEdit)

        controlLayout = QtWidgets.QHBoxLayout()
        controlLayout.addWidget(self.playButton)
        controlLayout.addWidget(self.pauseButton)
        controlLayout.addWidget(self.stopButton)
        controlLayout.addWidget(self.nextButton)
        controlLayout.addWidget(self.shuffleButton)
        controlLayout.addWidget(self.slider)
        controlLayout.addWidget(self.timeLabel)
        controlLayout.addWidget(self.volumeLabel)
        controlLayout.addWidget(self.volumeSlider)

        playlistLayout = QtWidgets.QHBoxLayout()
        playlistLayout.addWidget(self.addToPlaylistButton)
        playlistLayout.addWidget(self.exportPlaylistButton)
        playlistLayout.addWidget(self.importPlaylistButton)
        playlistLayout.addWidget(self.removeFromPlaylistButton)
        playlistLayout.addWidget(self.playlistDropdown)

        layout = QtWidgets.QVBoxLayout()
        layout.addLayout(advancedSearchLayout)
        layout.addLayout(searchLayout)
        mainContentLayout = QtWidgets.QHBoxLayout()
        mainContentLayout.addWidget(self.songTable)
        mainContentLayout.addWidget(self.coverLabel)
        layout.addLayout(mainContentLayout)
        layout.addLayout(controlLayout)
        layout.addLayout(playlistLayout)
        layout.addWidget(QtWidgets.QLabel("Aktuelle Playlist:"))
        layout.addWidget(self.playlistTable)
        self.setLayout(layout)
        
        # Style
        style = """
        QWidget {
            background-color: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                                              stop:0 #1e3c72, stop:1 #2a5298);
            color: white;
            font-family: 'Arial';
            font-size: 16px;
        }
        QTableWidget {
            background-color: rgba(255, 255, 255, 0.2);
            border: 2px solid white;
            padding: 5px;
        }
        QPushButton {
            background-color: #ff6f61;
            border: none;
            padding: 10px;
            border-radius: 10px;
        }
        QPushButton:hover {
            background-color: #ff856a;
        }
        QLineEdit {
            background: #fff;
            color: #222;
            border-radius: 6px;
            padding: 4px 8px;
        }
        """
        self.setStyleSheet(style)

        # Lade letzte Playlist beim Start (kein DB-Zugriff, Songtabelle bleibt leer)
        self.load_last_playlist()

    def search_songs(self):
        # Sammle alle Suchfelder
        search_fields = {
            'main': self.searchEdit.text().strip(),
            'title': self.titleSearch.text().strip(),
            'artist': self.artistSearch.text().strip(),
            'album': self.albumSearch.text().strip(),
            'year': self.yearSearch.text().strip(),
            'genre': self.genreSearch.text().strip(),
        }
        self.load_songs(search_fields)
        self.filterEdit.clear()

    def load_songs(self, search_fields):
        self.songs = fetch_songs(self.db_path, search_fields)
        self.populate_table()
        self.filterEdit.clear()

    def populate_table(self):
        self.songTable.setRowCount(len(self.songs))
        for row, song in enumerate(self.songs):
            self.songTable.setItem(row, 0, QtWidgets.QTableWidgetItem(song['file_path']))
            self.songTable.setItem(row, 1, QtWidgets.QTableWidgetItem(song.get('title', '')))
            self.songTable.setItem(row, 2, QtWidgets.QTableWidgetItem(song.get('artist', '')))
            self.songTable.setItem(row, 3, QtWidgets.QTableWidgetItem(song.get('album', '')))
            self.songTable.setItem(row, 4, QtWidgets.QTableWidgetItem(song.get('duration', '')))
        self.songTable.resizeColumnsToContents()
        self.songTable.setColumnHidden(0, True)

    def populate_playlist_table(self):
        self.playlistTable.setRowCount(len(self.current_playlist))
        for row, song in enumerate(self.current_playlist):
            self.playlistTable.setItem(row, 0, QtWidgets.QTableWidgetItem(song['file_path']))
            self.playlistTable.setItem(row, 1, QtWidgets.QTableWidgetItem(song.get('title', '')))
            self.playlistTable.setItem(row, 2, QtWidgets.QTableWidgetItem(song.get('artist', '')))
            self.playlistTable.setItem(row, 3, QtWidgets.QTableWidgetItem(song.get('album', '')))
            self.playlistTable.setItem(row, 4, QtWidgets.QTableWidgetItem(song.get('duration', '')))
            # YouTube Button
            yt_btn = QtWidgets.QPushButton("YT")
            yt_btn.clicked.connect(partial(self.open_youtube, song))
            self.playlistTable.setCellWidget(row, 5, yt_btn)
            # Spotify Button
            sp_btn = QtWidgets.QPushButton("SP")
            sp_btn.clicked.connect(partial(self.open_spotify, song))
            self.playlistTable.setCellWidget(row, 6, sp_btn)
        self.playlistTable.resizeColumnsToContents()
        self.playlistTable.setColumnHidden(0, True)

    def play_selected(self):
        selected = self.songTable.selectionModel().selectedRows()
        if not selected:
            QtWidgets.QMessageBox.warning(self, "Keine Auswahl", "Bitte wählen Sie mindestens einen Song aus.")
            return
        self.current_playlist = [self.songs[idx.row()] for idx in selected]
        self.current_index = 0
        self.save_last_playlist()
        self.populate_playlist_table()
        self.play_song(self.current_playlist[self.current_index]['file_path'])

    def play_from_playlist(self, index):
        row = index.row()
        if 0 <= row < len(self.current_playlist):
            self.current_index = row
            self.play_song(self.current_playlist[self.current_index]['file_path'])

    def play_song(self, path):
        if not os.path.exists(path):
            QtWidgets.QMessageBox.critical(self, "Dateifehler", f"Datei nicht gefunden: {path}")
            return
        self.player.setMedia(QMediaContent(QUrl.fromLocalFile(path)))
        self.player.play()
        self.update_cover(path)

    def play_next(self):
        if self.current_playlist and self.current_index + 1 < len(self.current_playlist):
            self.current_index += 1
            self.play_song(self.current_playlist[self.current_index]['file_path'])
            self.playlistTable.selectRow(self.current_index)
        else:
            QtWidgets.QMessageBox.information(self, "Ende", "Das ist der letzte Song in der Playlist.")

    def pause_song(self):
        self.player.pause()

    def stop_song(self):
        self.player.stop()
        self.slider.setValue(0)
        self.timeLabel.setText("00:00 / 00:00")

    def update_position(self, position):
        self.slider.setValue(position)
        self.update_time_label()

    def update_duration(self, duration):
        self.slider.setRange(0, duration)
        self.update_time_label()

    def set_position(self, position):
        self.player.setPosition(position)

    def set_volume(self, value):
        self.player.setVolume(value)

    def update_time_label(self):
        pos = self.player.position() // 1000
        dur = self.player.duration() // 1000
        self.timeLabel.setText(f"{pos//60:02}:{pos%60:02} / {dur//60:02}:{dur%60:02}")

    def update_buttons(self, state):
        self.playButton.setEnabled(state != QMediaPlayer.PlayingState)
        self.pauseButton.setEnabled(state == QMediaPlayer.PlayingState)
        self.stopButton.setEnabled(state != QMediaPlayer.StoppedState)
        self.nextButton.setEnabled(bool(self.current_playlist))

    def add_to_playlist(self):
        selected = self.songTable.selectionModel().selectedRows()
        if not selected:
            QtWidgets.QMessageBox.warning(self, "Keine Auswahl", "Bitte wählen Sie Songs aus.")
            return
        for idx in selected:
            song = self.songs[idx.row()]
            if song not in self.current_playlist:
                self.current_playlist.append(song)
        self.save_last_playlist()
        self.populate_playlist_table()
        QtWidgets.QMessageBox.information(self, "Playlist", f"{len(selected)} Songs zur Playlist hinzugefügt.")

    def export_playlist(self):
        if not self.current_playlist:
            QtWidgets.QMessageBox.warning(self, "Keine Playlist", "Die Playlist ist leer.")
            return
        # Playlistnamen abfragen
        name, ok = QtWidgets.QInputDialog.getText(self, "Playlistname", "Name für den Export der Playlist:")
        if not ok or not name.strip():
            return
        name = name.strip()
        export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{name}_export")
        try:
            if not os.path.exists(export_dir):
                os.makedirs(export_dir)
            # Songs kopieren
            import shutil
            for song in self.current_playlist:
                src = song['file_path']
                if os.path.exists(src):
                    dst = os.path.join(export_dir, os.path.basename(src))
                    if not os.path.exists(dst):
                        shutil.copy2(src, dst)
            # M3U exportieren
            m3u_path = os.path.join(export_dir, f"{name}.m3u")
            with open(m3u_path, 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                for song in self.current_playlist:
                    f.write(f"#EXTINF:-1,{song.get('artist','')} - {song.get('title','')}\n{os.path.basename(song['file_path'])}\n")
            # TXT für Streaming-Dienste
            txt_path = os.path.join(export_dir, f"{name}_titles.txt")
            with open(txt_path, 'w', encoding='utf-8') as f:
                for song in self.current_playlist:
                    line = f"{song.get('artist','')} - {song.get('title','')}"
                    if line.strip() != '-':
                        f.write(line + "\n")
            QtWidgets.QMessageBox.information(self, "Export", f"Playlist und Songs exportiert nach: {export_dir}\n\nDie Datei '{name}_titles.txt' kann für Amazon, YouTube Music oder Spotify verwendet werden.")
            self.save_last_playlist()
            self.update_playlist_dropdown()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Fehler beim Export: {e}")

    def import_playlist(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Playlist öffnen", PLAYLIST_DIR, "M3U Dateien (*.m3u)")
        if not path:
            return
        self.import_playlist_from_path(path)

    def update_playlist_dropdown(self):
        self.playlistDropdown.blockSignals(True)
        self.playlistDropdown.clear()
        self.playlistDropdown.addItem("<Letzte Playlist>")
        if os.path.exists(PLAYLIST_DIR):
            for fname in sorted(os.listdir(PLAYLIST_DIR)):
                if fname.endswith('.m3u'):
                    self.playlistDropdown.addItem(fname)
        self.playlistDropdown.blockSignals(False)

    def dropdown_playlist_selected(self, idx):
        if idx == 0:
            self.load_last_playlist()
        else:
            fname = self.playlistDropdown.currentText()
            path = os.path.join(PLAYLIST_DIR, fname)
            if os.path.exists(path):
                self.import_playlist_from_path(path)

    def import_playlist_from_path(self, path):
        playlist = []
        try:
            with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        meta = {'file_path': line}
                        try:
                            audio = MP3(line, ID3=EasyID3)
                            meta['title'] = audio.get('title', [''])[0]
                            meta['artist'] = audio.get('artist', [''])[0]
                            meta['album'] = audio.get('album', [''])[0]
                            meta['duration'] = f"{int(audio.info.length//60):02}:{int(audio.info.length%60):02}"
                        except Exception:
                            meta['title'] = meta['artist'] = meta['album'] = ''
                            meta['duration'] = ''
                        playlist.append(meta)
            if playlist:
                self.current_playlist = playlist
                self.current_index = 0
                self.save_last_playlist()
                self.populate_playlist_table()
                self.songs = playlist.copy()
                self.populate_table()
                self.play_song(self.current_playlist[self.current_index]['file_path'])
                QtWidgets.QMessageBox.information(self, "Playlist geladen", f"{len(playlist)} Songs geladen. Die Wiedergabe startet automatisch.")
            else:
                QtWidgets.QMessageBox.warning(self, "Leere Playlist", "Keine Songs in der Playlist gefunden.")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Fehler", f"Fehler beim Laden: {e}")

    def remove_from_playlist(self):
        selected = self.songTable.selectionModel().selectedRows()
        if not selected:
            QtWidgets.QMessageBox.warning(self, "Keine Auswahl", "Bitte wählen Sie Songs aus, die entfernt werden sollen.")
            return
        to_remove = [self.songs[idx.row()] for idx in selected]
        # Entferne aus Playlist und aus songs (Suchbereich)
        self.current_playlist = [s for s in self.current_playlist if s not in to_remove]
        self.songs = [s for s in self.songs if s not in to_remove]
        self.save_last_playlist()
        self.populate_playlist_table()
        self.populate_table()
        QtWidgets.QMessageBox.information(self, "Entfernt", f"{len(to_remove)} Songs aus der Playlist entfernt.")

    def save_last_playlist(self):
        if not os.path.exists(PLAYLIST_DIR):
            os.makedirs(PLAYLIST_DIR)
        try:
            with open(LAST_PLAYLIST_PATH, 'w', encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                for song in self.current_playlist:
                    f.write(f"#EXTINF:-1,{song.get('artist','')} - {song.get('title','')}\n{song['file_path']}\n")
        except Exception as e:
            print(f"Fehler beim Speichern der letzten Playlist: {e}", file=sys.stderr)

    def load_last_playlist(self):
        if not os.path.exists(LAST_PLAYLIST_PATH):
            self.current_playlist = []
            self.current_index = -1
            self.populate_playlist_table()
            return
        playlist = []
        try:
            with open(LAST_PLAYLIST_PATH, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        meta = {'file_path': line}
                        try:
                            audio = MP3(line, ID3=EasyID3)
                            meta['title'] = audio.get('title', [''])[0]
                            meta['artist'] = audio.get('artist', [''])[0]
                            meta['album'] = audio.get('album', [''])[0]
                            meta['duration'] = f"{int(audio.info.length//60):02}:{int(audio.info.length%60):02}"
                        except Exception:
                            meta['title'] = meta['artist'] = meta['album'] = ''
                            meta['duration'] = ''
                        playlist.append(meta)
            self.current_playlist = playlist
            self.current_index = 0 if playlist else -1
            self.populate_playlist_table()
        except Exception as e:
            print(f"Fehler beim Laden der letzten Playlist: {e}", file=sys.stderr)
            self.current_playlist = []
            self.current_index = -1
            self.populate_playlist_table()

    def handle_media_status(self, status):
        # Automatisches Weiterspielen
        if status == QMediaPlayer.EndOfMedia:
            # Wenn Playlist aktiv, spiele nächsten Song in Playlist
            if self.current_playlist and 0 <= self.current_index < len(self.current_playlist) - 1:
                self.current_index += 1
                self.play_song(self.current_playlist[self.current_index]['file_path'])
                self.playlistTable.selectRow(self.current_index)
            # Wenn keine Playlist, aber Songs im Suchbereich, spiele nächsten Song in Songliste
            elif not self.current_playlist and self.songs:
                # Finde aktuellen Song in self.songs
                current_path = self.player.media().canonicalUrl().toLocalFile()
                idx = next((i for i, s in enumerate(self.songs) if s['file_path'] == current_path), -1)
                if 0 <= idx < len(self.songs) - 1:
                    next_song = self.songs[idx + 1]
                    self.play_song(next_song['file_path'])
                    self.songTable.selectRow(idx + 1)

    def playlist_drag_enter_event(self, event):
        if event.mimeData().hasFormat('application/x-qabstractitemmodeldatalist'):
            event.acceptProposedAction()
        else:
            event.ignore()

    def playlist_drag_move_event(self, event):
        if event.mimeData().hasFormat('application/x-qabstractitemmodeldatalist'):
            event.acceptProposedAction()
        else:
            event.ignore()

    def playlist_drop_event(self, event):
        if event.mimeData().hasFormat('application/x-qabstractitemmodeldatalist'):
            selected = self.songTable.selectionModel().selectedRows()
            added = 0
            for idx in selected:
                song = self.songs[idx.row()]
                if song not in self.current_playlist:
                    self.current_playlist.append(song)
                    added += 1
            if added > 0:
                self.save_last_playlist()
                self.populate_playlist_table()
                QtWidgets.QMessageBox.information(self, "Playlist", f"{added} Songs per Drag & Drop zur Playlist hinzugefügt.")
            event.acceptProposedAction()
        else:
            event.ignore()

    def shuffle_playlist(self):
        if not self.current_playlist:
            QtWidgets.QMessageBox.warning(self, "Keine Playlist", "Die Playlist ist leer.")
            return
        random.shuffle(self.current_playlist)
        self.current_index = 0
        self.save_last_playlist()
        self.populate_playlist_table()
        self.play_song(self.current_playlist[self.current_index]['file_path'])
        QtWidgets.QMessageBox.information(self, "Shuffle", "Playlist wurde gemischt und startet jetzt zufällig.")

    def open_youtube(self, song):
        query = f"{song.get('artist','')} {song.get('title','')}"
        url = f"https://www.youtube.com/results?search_query={urllib.parse.quote(query)}"
        threading.Thread(target=webbrowser.open_new_tab, args=(url,)).start()

    def open_spotify(self, song):
        query = f"{song.get('artist','')} {song.get('title','')}"
        url = f"https://open.spotify.com/search/{urllib.parse.quote(query)}"
        threading.Thread(target=webbrowser.open_new_tab, args=(url,)).start()

    def show_song_context_menu(self, pos):
        index = self.songTable.indexAt(pos)
        if not index.isValid():
            return
        row = index.row()
        # Hole Dateipfad aus der (versteckten) ersten Spalte
        file_path = self.songTable.item(row, 0).text()
        song = next((s for s in self.songs if s['file_path'] == file_path), None)
        if not song:
            QtWidgets.QMessageBox.warning(self, "Fehler", "Song nicht gefunden.")
            return
        menu = QtWidgets.QMenu(self)
        action_path = menu.addAction("Dateipfad anzeigen")
        action_play = menu.addAction("Abspielen")
        action_explorer = menu.addAction("Im Explorer anzeigen")
        action_update_tags = menu.addAction("MP3-Tags aktualisieren")
        action = menu.exec_(self.songTable.viewport().mapToGlobal(pos))
        if action == action_path:
            QtWidgets.QMessageBox.information(self, "Dateipfad", song['file_path'])
        elif action == action_play:
            self.current_playlist = [song]
            self.current_index = 0
            self.save_last_playlist()
            self.populate_playlist_table()
            self.play_song(song['file_path'])
        elif action == action_explorer:
            path = song['file_path']
            folder = os.path.dirname(path)
            if os.path.exists(path):
                # Windows: öffne Explorer und markiere Datei
                if sys.platform.startswith('win'):
                    os.startfile(folder)
                    # Alternativ: Datei markieren
                    # os.system(f'explorer /select,"{path}"')
                else:
                    QtWidgets.QMessageBox.information(self, "Explorer", f"Explorer-Öffnen wird nur unter Windows unterstützt. Pfad: {folder}")
            else:
                QtWidgets.QMessageBox.warning(self, "Fehler", f"Datei nicht gefunden: {path}")
        elif action == action_update_tags:
            try:
                # Stoppe Wiedergabe, falls diese Datei gerade läuft
                current_path = self.player.media().canonicalUrl().toLocalFile() if hasattr(self, 'player') else None
                if current_path and os.path.abspath(current_path) == os.path.abspath(song['file_path']):
                    self.player.stop()
                audio = MP3(song['file_path'], ID3=EasyID3)
                # Versuche Jahr und Genre zu lesen
                year = audio.get('date', [''])[0] if 'date' in audio else audio.get('year', [''])[0] if 'year' in audio else ''
                genre = audio.get('genre', [''])[0] if 'genre' in audio else ''
                song['year'] = year
                song['genre'] = genre
                dlg = TagEditorDialog(song, self)
                if dlg.exec_() == QtWidgets.QDialog.Accepted:
                    tags = dlg.get_tags()
                    audio['title'] = tags['title']
                    audio['artist'] = tags['artist']
                    audio['album'] = tags['album']
                    if tags['year']:
                        audio['date'] = tags['year']
                    if tags['genre']:
                        audio['genre'] = tags['genre']
                    audio.save()
                    # Nach dem Speichern neu einlesen
                    audio = MP3(song['file_path'], ID3=EasyID3)
                    song['title'] = audio.get('title', [''])[0]
                    song['artist'] = audio.get('artist', [''])[0]
                    song['album'] = audio.get('album', [''])[0]
                    song['duration'] = f"{int(audio.info.length//60):02}:{int(audio.info.length%60):02}"
                    song['year'] = audio.get('date', [''])[0] if 'date' in audio else audio.get('year', [''])[0] if 'year' in audio else ''
                    song['genre'] = audio.get('genre', [''])[0] if 'genre' in audio else ''
                    # Finde Index in self.songs und aktualisiere dort
                    for i, s in enumerate(self.songs):
                        if s['file_path'] == file_path:
                            self.songs[i] = song
                            break
                    self.populate_table()
                    QtWidgets.QMessageBox.information(self, "MP3-Tags", "MP3-Tags wurden gespeichert und neu geladen.")
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Fehler", f"Fehler beim Bearbeiten der MP3-Tags: {e}")

    def filter_table(self):
        filter_text = self.filterEdit.text().strip().lower()
        for row in range(self.songTable.rowCount()):
            visible = True
            if filter_text:
                row_text = " ".join([
                    self.songTable.item(row, col).text().lower() if self.songTable.item(row, col) else ''
                    for col in range(self.songTable.columnCount())
                ])
                if filter_text not in row_text:
                    visible = False
            self.songTable.setRowHidden(row, not visible)

    def set_cover_placeholder(self):
        # Einfache Platzhalter-Grafik (z.B. Musiknote)
        pixmap = QPixmap(180, 180)
        pixmap.fill(QtGui.QColor("#223366"))
        painter = QtGui.QPainter(pixmap)
        painter.setPen(QtGui.QColor("#fff"))
        painter.setFont(QtGui.QFont("Arial", 80))
        painter.drawText(pixmap.rect(), QtCore.Qt.AlignCenter, "🎵")
        painter.end()
        self.coverLabel.setPixmap(pixmap)

    def update_cover(self, path):
        try:
            tags = ID3(path)
            apic = tags.getall('APIC')
            if apic:
                cover_data = apic[0].data
                pixmap = QPixmap()
                pixmap.loadFromData(QByteArray(cover_data))
                self.coverLabel.setPixmap(pixmap.scaled(180, 180, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation))
                return
        except Exception:
            pass
        self.set_cover_placeholder()

class TagEditorDialog(QtWidgets.QDialog):
    def __init__(self, song, parent=None):
        super().__init__(parent)
        self.setWindowTitle("MP3-Tags bearbeiten")
        self.song = song
        layout = QtWidgets.QFormLayout(self)
        # Dateiname und Ordner anzeigen
        file_path = song.get('file_path', '')
        file_name = os.path.basename(file_path)
        folder_name = os.path.basename(os.path.dirname(file_path))
        self.fileLabel = QtWidgets.QLabel(file_name)
        self.folderLabel = QtWidgets.QLabel(folder_name)
        layout.addRow("Dateiname:", self.fileLabel)
        layout.addRow("Ordner:", self.folderLabel)
        self.titleEdit = QtWidgets.QLineEdit(song.get('title', ''))
        self.artistEdit = QtWidgets.QLineEdit(song.get('artist', ''))
        self.albumEdit = QtWidgets.QLineEdit(song.get('album', ''))
        self.yearEdit = QtWidgets.QLineEdit(song.get('year', ''))
        self.genreEdit = QtWidgets.QLineEdit(song.get('genre', ''))
        self.fromFilenameButton = QtWidgets.QPushButton("Tags aus Dateiname/Ordner übernehmen")
        self.fromFilenameButton.clicked.connect(self.fill_tags_from_filename)
        layout.addRow(self.fromFilenameButton)
        layout.addRow("Titel:", self.titleEdit)
        layout.addRow("Interpret:", self.artistEdit)
        layout.addRow("Album:", self.albumEdit)
        layout.addRow("Jahr:", self.yearEdit)
        layout.addRow("Genre:", self.genreEdit)
        self.searchButton = QtWidgets.QPushButton("Tags online suchen")
        self.searchButton.clicked.connect(self.search_tags_online)
        layout.addRow(self.searchButton)
        btns = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Save | QtWidgets.QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addRow(btns)
    def fill_tags_from_filename(self):
        file_path = self.song.get('file_path', '')
        file_name = os.path.basename(file_path)
        folder_name = os.path.basename(os.path.dirname(file_path))
        # Versuche Format "Interpret - Titel.mp3"
        name, _ = os.path.splitext(file_name)
        if ' - ' in name:
            artist, title = name.split(' - ', 1)
            self.artistEdit.setText(artist.strip())
            self.titleEdit.setText(title.strip())
        else:
            self.titleEdit.setText(name.strip())
        self.albumEdit.setText(folder_name)
    
    def search_tags_online(self):
        import urllib.parse, webbrowser
        artist = self.artistEdit.text().strip()
        title = self.titleEdit.text().strip()
        duration = self.song.get('duration', '').strip()
        query = f"{artist} {title} {duration} mp3 tags"
        url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"
        webbrowser.open_new_tab(url)
    def get_tags(self):
        return {
            'title': self.titleEdit.text(),
            'artist': self.artistEdit.text(),
            'album': self.albumEdit.text(),
            'year': self.yearEdit.text(),
            'genre': self.genreEdit.text(),
        }

def normalize_text(text):
    # Nur Buchstaben/Zahlen, alles klein, mehrere Leerzeichen zu einem
    text = text.lower()
    text = re.sub(r'[^a-z0-9 ]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def fetch_songs(db_path, search_fields):
    songs = []
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Angepasste SQL für optimierte Datenbankstruktur
        query = """
            SELECT directories.full_path || '/' || files.filename || COALESCE(extensions.name, '') as file_path
            FROM files
            JOIN directories ON files.directory_id = directories.id
            LEFT JOIN extensions ON files.extension_id = extensions.id
            WHERE extensions.name = '.mp3'
        """
        params = []
        
        # Baue WHERE-Klausel für zusätzliche Filter
        additional_where = []
        if search_fields['main']:
            for t in search_fields['main'].split():
                additional_where.append("(files.filename LIKE ? OR directories.full_path LIKE ?)")
                params.extend([f"%{t}%", f"%{t}%"])
        if search_fields['title']:
            additional_where.append("files.filename LIKE ?")
            params.append(f"%{search_fields['title']}%")
        if search_fields['artist']:
            additional_where.append("files.filename LIKE ?")
            params.append(f"%{search_fields['artist']}%")
        if search_fields['album']:
            additional_where.append("directories.full_path LIKE ?")
            params.append(f"%{search_fields['album']}%")
        if search_fields['year']:
            additional_where.append("files.filename LIKE ?")
            params.append(f"%{search_fields['year']}%")
        if search_fields['genre']:
            additional_where.append("files.filename LIKE ?")
            params.append(f"%{search_fields['genre']}%")
        
        if additional_where:
            query += " AND " + " AND ".join(additional_where)
            
        cursor.execute(query, params)
        rows = cursor.fetchall()
        song_dict = {}
        for row in rows:
            file_path = row[0]
            # Normalisiere Pfad für Windows-Kompatibilität
            file_path = os.path.normpath(file_path.replace('/', os.sep))
            meta = {'file_path': file_path}
            try:
                audio = MP3(file_path, ID3=EasyID3)
                meta['title'] = audio.get('title', [''])[0]
                meta['artist'] = audio.get('artist', [''])[0]
                meta['album'] = audio.get('album', [''])[0]
                meta['duration'] = f"{int(audio.info.length//60):02}:{int(audio.info.length%60):02}"
                meta['bitrate'] = getattr(audio.info, 'bitrate', 0)
                meta['duration_sec'] = int(round(audio.info.length))
            except Exception:
                meta['title'] = meta['artist'] = meta['album'] = ''
                meta['duration'] = ''
                meta['bitrate'] = 0
                meta['duration_sec'] = 0
            # Schlüssel für Duplikaterkennung: nur Titel, Interpret, Dauer (tolerant)
            norm_title = normalize_text(meta['title'])
            norm_artist = normalize_text(meta['artist'])
            found = False
            for (t, a, d_sec) in list(song_dict.keys()):
                if t == norm_title and a == norm_artist and abs(d_sec - meta['duration_sec']) <= 1:
                    # Behalte Song mit höchster Bitrate
                    if meta['bitrate'] > song_dict[(t, a, d_sec)]['bitrate']:
                        song_dict[(t, a, d_sec)] = meta
                    found = True
                    break
            if not found:
                song_dict[(norm_title, norm_artist, meta['duration_sec'])] = meta
        songs = list(song_dict.values())
        conn.close()
    except sqlite3.Error as e:
        print(f"DB Fehler: {e}", file=sys.stderr)
        sys.exit(1)
    return songs

def main():
    if len(sys.argv) < 2:
        print("Usage: {} <db_path>".format(sys.argv[0]), file=sys.stderr)
        sys.exit(1)
    db_path = sys.argv[1]
    
    app = QtWidgets.QApplication(sys.argv)
    player = SongPlayer(db_path)
    player.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()