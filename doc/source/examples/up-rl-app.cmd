@echo off
setlocal
cd /d "%~dp0"

echo Building and starting Zuul RL app...
docker compose -f docker-compose.rl-app.yaml -p zuul-rl up -d --build
if errorlevel 1 (
  echo ERROR: docker compose failed. Is Docker Desktop running?
  exit /b 1
)

echo.
echo Waiting for bootstrap (up to 3 minutes)...
set /a WAIT_COUNT=0
:WAIT_BOOTSTRAP
docker compose -f docker-compose.rl-app.yaml -p zuul-rl ps rl-bootstrap 2>nul | findstr /i /c:"exited (0)" >nul
if not errorlevel 1 (
  echo Bootstrap finished.
  goto BOOTSTRAP_DONE
)
set /a WAIT_COUNT+=1
if %WAIT_COUNT% geq 36 goto BOOTSTRAP_DONE
timeout /t 5 /nobreak >nul
goto WAIT_BOOTSTRAP

:BOOTSTRAP_DONE
echo.
echo ==========================================
echo  Zuul RL app is up (active mode)
echo ==========================================
echo  Status page:  http://localhost:9090/t/example-tenant/status
echo                 http://localhost:19090/t/example-tenant/status
echo  Gerrit:       http://localhost:8080
echo.
echo  In the status page bottom-right panel:
echo    - Click "Run demo"
echo    - Watch throughput + RL-vs-TCP window graphs
echo.
echo  Hard refresh once: Ctrl+Shift+R
echo ==========================================
endlocal
