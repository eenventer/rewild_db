@echo off

rem Get the current directory.
set current_dir=%cd%

rem Change directory to the virtual environment.
cd "%current_dir%\.venv\Scripts"

rem Activate the virtual environment.
call activate

rem Change directory back to the original directory.
cd "%current_dir%"