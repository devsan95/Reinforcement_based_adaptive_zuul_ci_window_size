# Zuul RL App (turnkey Docker)

Build images from the current code, start the stack, use everything from the Zuul UI.

## Requirements

- Docker Engine
- Docker Compose v2.20+ (for `include:`)

## Start (one command)

### Windows PowerShell

```powershell
cd C:\Users\vagga\Desktop\zuul-community\doc\source\examples
.\up-rl-app.ps1
```

Stop:

```powershell
.\down-rl-app.ps1
```

### Linux / WSL / macOS

```bash
cd doc/source/examples
bash up-rl-app.sh
```

Or manually:

```bash
docker compose -f docker-compose.rl-app.yaml -p zuul-rl up -d --build
```

## Open UI

http://localhost:9090/t/example-tenant/status

Hard refresh once: **Ctrl+Shift+R**

## Use from UI

1. Wait until the status page loads.
2. Bottom-right panel: click **Run demo**.
3. Continuous traffic runs for **~5 minutes**: **10 changes every 30 seconds**
   (topics `demo-burst` / `demo-steady` / `demo-depend`, Depends-On mix).
4. Progress shows e.g. `Batch 3/10 · 30 changes · ~2:00 left · 12 gate failures`.
5. Click **Extend session** to add another batch of 10 (does not cancel in-flight builds).
6. Graphs update live: RL vs TCP window, Extra in flight, Minutes saved (est.).

### Fail/pass rule

Exactly **5 fail + 5 pass** per batch of 10 (`batch_index` 0..4 fail). Deterministic across re-runs.

### Minutes saved (est.)

```
sum_over_failures(max(0, RL − TCP_after)) × avg_job_duration / 60
capped at session wall-clock minutes
```

Idle time does not inflate the estimate.

## Validation methodology

See **[VALIDATION.md](VALIDATION.md)** for Extra in flight, capacity, and success criteria.

## Rebuild after code changes

```powershell
cd C:\Users\vagga\Desktop\zuul-community\doc\source\examples
docker compose -f docker-compose.yaml -f docker-compose.rl-app.yaml -p zuul-rl up -d --build `
  scheduler web rl-control rl-bootstrap
docker exec zuul-rl-scheduler-1 zuul-scheduler full-reconfigure
```

## Stop

```bash
docker compose -f docker-compose.rl-app.yaml -p zuul-rl down
```
