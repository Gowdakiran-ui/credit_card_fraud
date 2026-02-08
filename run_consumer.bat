@echo off
REM Wrapper script to run consumer with venv activated
cd /d "%~dp0kafka"
call venv\Scripts\activate.bat
python consumer.py
