@echo off
cd /d "%~dp0"
echo Stopping Zuul RL app...
docker compose -f docker-compose.rl-app.yaml -p zuul-rl down
if errorlevel 1 (
  echo ERROR: docker compose down failed.
  exit /b 1
)
echo Done.
