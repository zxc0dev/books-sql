@echo off
cd /d %~dp0..

for /f "usebackq tokens=1,2 delims== eol=#" %%i in (".env") do set %%i=%%j

cd dbt
dbt docs generate --profiles-dir . --target-path ..\docs
dbt docs serve --profiles-dir . --target-path ..\docs