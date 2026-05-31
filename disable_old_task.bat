@echo off
set PS1=%~dp0disable_old_task.ps1
powershell -Command "Start-Process powershell -Verb RunAs -ArgumentList '-NoProfile -ExecutionPolicy Bypass -File \"%PS1%\"' -Wait"
