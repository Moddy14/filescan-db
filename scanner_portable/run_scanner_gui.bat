@echo off
cd /d %~dp0
call venv\Scripts\activate.bat
REM Startet die GUI synchron
python gui_launcher.py
exit /b %errorlevel%
