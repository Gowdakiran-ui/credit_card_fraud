@echo off
REM Wrapper script to run producer with venv activated
cd /d "%~dp0kafka"
call venv\Scripts\activate.bat
python producer.py
