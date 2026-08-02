#!/bin/sh
interval="${RL_TRAFFIC_INTERVAL:-20}"

while true; do
  python /app/traffic/generator.py --count 2 --gate || true
  sleep "$interval"
done
