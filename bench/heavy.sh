#!/usr/bin/env bash
# Formerly took a shared CPU lock against benchmarks. Benchmarks now suspend
# agent processes for the duration of each trial (bench/freeze_agents.py), so
# heavy work no longer waits for benchmarks or blocks them. Kept as a
# pass-through so existing commands keep working:
#   bash bench/heavy.sh <command...>
exec "$@"
