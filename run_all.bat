@echo off
REM Forward to run_experiments.ps1 with execution policy bypass
powershell -ExecutionPolicy Bypass -File "%~dp0run_experiments.ps1" %*
