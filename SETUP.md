# Community Zuul CI (latest) - Local Setup

This is the upstream community Zuul from https://opendev.org/zuul/zuul (master).

## Quick start

```bash
cd doc/source/examples
docker compose -p zuul-tutorial up -d
```

## URLs (this machine)

| Service | URL |
|---------|-----|
| Gerrit | http://localhost:8080 |
| Zuul Web UI | http://localhost:19090 |
| Build logs | http://localhost:8000 |
| Gerrit SSH | localhost:29418 |

> Port 19090 is used instead of 9000 because Zscaler VPN (`ZSATunnel.exe`) binds port 9000 on this host.

## Manage stack

```bash
# Status
docker compose -p zuul-tutorial ps

# Logs
docker compose -p zuul-tutorial logs -f scheduler web

# Stop
docker compose -p zuul-tutorial down

# Full reset (removes volumes)
docker compose -p zuul-tutorial down -v
```

## Tutorial

Follow the official quick-start:
https://zuul-ci.org/docs/zuul/latest/tutorials/quick-start.html

Use `http://localhost:19090` wherever the tutorial references `http://localhost:9000`.

## Version

Zuul scheduler reports: **14.2.1.dev40** (latest upstream master at clone time).

## Research stack (RL gate window)

This repo includes the research implementation for *Enhancing Zuul Throughput by
Adaptive Window Size using Reinforcement Learning*.

```bash
cd doc/source/examples
bash start-research-stack.sh
bash demo-change.sh
```

Full documentation: [research/README.md](research/README.md)

