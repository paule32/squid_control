:: ---------------------------------------------------------------
:: \file start.bat
:: \note (c) 2026 by Jens Kallup - paule32 aka Blacky Cat
::       all rights reserved.
:: ---------------------------------------------------------------
@echo off
set "STARTDIR=%CD%"
:: ---------------------------------------------------------------
:: \brief now, the application should run fine. When you start the
::        application (or this batch script), the start of the
::        runner maybe faster - because the installed files are
::        present in venv directory.
:: ---------------------------------------------------------------
venv\Scripts\python.exe squid_manager.pyc
