:: ---------------------------------------------------------------
:: \file install.bat
:: \note (c) 2026 by Jens Kallup - paule32 aka Blacky Cat
::       all rights reserved.
:: ---------------------------------------------------------------
@echo on
echo %path%
set "STARTDIR=%CD%"

"C:\Program Files\Python313\python.exe" -m venv venv

:: ---------------------------------------------------------------
:: KEEP THE FOLLOWING LINES UNTOUCHED
:: ---------------------------------------------------------------
set "PATH=%STARTDIR%\venv\Scripts\;%PATH%"
set "PYTHON_VENV=%STARTDIR%\venv"
:: ---------------------------------------------------------------
echo setup ...
dir "%PYTHON_VENV%\Scripts\"
%PYTHON_VENV%\Scripts\python -m pip install --upgrade pip

if not exist "%PYTHON_VENV%\Lib\site-packages\antrl4\" (
%PYTHON_VENV%\Scripts\python -m pip install antlr4-python3-runtime
)
if not exist "%PYTHON_VENV%\Lib\site-packages\polib.py" (
%PYTHON_VENV%\Scripts\python -m pip install polib
)
if not exist "%PYTHON_VENV%\Lib\site-packages\PyQt5\" (
%PYTHON_VENV%\Scripts\python -m pip install PyQt5 PyQtWebEngine
)
%PYTHON_VENV%\Scripts\python -m pip install matplotlib
echo done.
pause
