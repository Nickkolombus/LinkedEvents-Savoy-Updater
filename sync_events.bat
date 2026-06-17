@echo off
REM Sync Savoy events for current year and next year

echo Syncing Savoy events...
echo.

REM Get current year
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
set current_year=%datetime:~0,4%

REM Calculate next year
set /a next_year=%current_year%+1

echo Running sync for %current_year%...
python main.py --year %current_year%
echo.

echo Running sync for %next_year%...
python main.py --year %next_year%
echo.

echo Sync complete!
pause

