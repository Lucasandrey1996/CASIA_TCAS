@echo off
REM Lance import_load.py avec l'environnement virtuel .venv
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo Erreur: .venv introuvable. Executez: python -m venv .venv
    echo Puis: .venv\Scripts\pip install pandas pyarrow
    exit /b 1
)

.venv\Scripts\python.exe "import_load.py" %*
exit /b %ERRORLEVEL%
