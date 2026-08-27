# Build and start the turnkey RL Zuul app (active mode, UI demo button, graphs).
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Write-Host "Building and starting Zuul RL app..."
docker compose -f docker-compose.rl-app.yaml -p zuul-rl up -d --build
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: docker compose failed. Is Docker Desktop running?" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Waiting for bootstrap (up to 3 minutes)..."
for ($i = 0; $i -lt 36; $i++) {
    # Docker may write harmless proxy warnings to stderr; don't treat as terminating errors.
    $ps = cmd /c "docker compose -f docker-compose.rl-app.yaml -p zuul-rl ps rl-bootstrap 2>nul"
    if ($ps -match "exited \(0\)") {
        Write-Host "Bootstrap finished."
        break
    }
    Start-Sleep -Seconds 5
}

Write-Host ""
Write-Host "=========================================="
Write-Host " Zuul RL app is up (active mode)"
Write-Host "=========================================="
Write-Host " Status page:  http://localhost:9090/t/example-tenant/status"
Write-Host "               http://localhost:19090/t/example-tenant/status"
Write-Host " Gerrit:       http://localhost:8080"
Write-Host ""
Write-Host " In the status page bottom-right panel:"
Write-Host "   - Click 'Run demo'"
Write-Host "   - Watch throughput + RL-vs-TCP window graphs"
Write-Host ""
Write-Host " Hard refresh once: Ctrl+Shift+R"
Write-Host "=========================================="
