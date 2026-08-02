$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
Write-Host "Stopping Zuul RL app..."
docker compose -f docker-compose.rl-app.yaml -p zuul-rl down
if ($LASTEXITCODE -ne 0) { exit 1 }
Write-Host "Done."
