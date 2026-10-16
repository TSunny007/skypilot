#!/usr/bin/env bash
# Serialize every use of the single Kind cluster and the API server:
#   bash bench/cluster_lock.sh <command...>
# Anything that creates pods, launches jobs, or restarts the API server must
# go through this. Benchmark isolation from agent CPU work is handled by
# bench/freeze_agents.py (started by bench/bench.sh), not by a CPU lock.
L=/home/tsunny/Documents/skypilot-org/skypilot/bench
exec flock "$L/.cluster.lock" "$@"
