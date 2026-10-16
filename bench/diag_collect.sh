#!/usr/bin/env bash
# Pull in-pod bootstrap/Ray/job logs from every live pod of a trial.
# Usage: bash bench/diag_collect.sh <run_dir>   (pods must still be alive)
set -uo pipefail
RUN=$1
mkdir -p "$RUN/diag"
for pod in $(kubectl get pods -l skypilot-cluster-name -o jsonpath='{.items[*].metadata.name}'); do
  out=$RUN/diag/$pod
  mkdir -p "$out"
  kubectl logs --timestamps "$pod" -c ray-node > "$out/container_stdout.log" 2>&1
  # Epoch mtimes of bootstrap markers + start time of every process
  # (pods share the host kernel, so btime/starttime are host-comparable).
  kubectl exec "$pod" -c ray-node -- bash -c '
    echo "== markers"
    for f in /tmp/sky_core_pkgs_ready /tmp/apt_ssh_setup_complete \
             /tmp/ray_skypilot_installation_complete /tmp/env_setup_complete \
             /tmp/*.failed /tmp/*.log; do
      [ -e "$f" ] && echo "$f $(stat -c %.9Y "$f")"
    done
    echo "== procs (pid starttime_ticks cmd); btime=$(awk "/btime/{print \$2}" /proc/stat) clk=$(getconf CLK_TCK)"
    for p in /proc/[0-9]*; do
      st=$(cat $p/stat 2>/dev/null) || continue
      echo "${p#/proc/} $(echo "${st##*) }" | awk "{print \$20}") $(tr "\0\n" "  " < $p/cmdline 2>/dev/null | cut -c1-300)"
    done
    echo "== pyc (sky wheel is installed per pod; does first import compile?)"
    SP=$(ls -d ~/skypilot-runtime/lib/python3*/site-packages)
    echo "sky/__init__.py $(stat -c %.9Y $SP/sky/__init__.py)"
    for pkg in sky ray; do
      find $SP/$pkg -name "*.pyc" -newer /tmp/sky_core_pkgs_ready -printf "%T@\n" \
        | awk -v p=$pkg "{printf \"%d\\n\", \$1}" | sort | uniq -c | sed "s/^/${pkg}_pyc_written_at /"
      echo "$pkg pyc total: $(find $SP/$pkg -name "*.pyc" | wc -l), written since pod start: $(find $SP/$pkg -name "*.pyc" -newer /tmp/sky_core_pkgs_ready | wc -l)"
    done
    echo "== import A/B (driver imports)"
    PY=~/skypilot-runtime/bin/python
    MODS="from sky.skylet import autostop_lib, constants, job_lib; from sky.utils import log_utils, subprocess_utils"
    for i in 1 2; do
      s=$(date +%s.%N); $PY -c "$MODS"; e=$(date +%s.%N)
      echo "warm_import_s $(awk "BEGIN{print $e - $s}")"
      d=$(mktemp -d); s=$(date +%s.%N); PYTHONPYCACHEPREFIX=$d $PY -c "$MODS"; e=$(date +%s.%N)
      echo "cold_import_s(no pyc) $(awk "BEGIN{print $e - $s}")"; rm -rf $d
    done
  ' > "$out/inpod.txt" 2>&1
  kubectl exec "$pod" -c ray-node -- bash -c \
    'tar --format=posix -czf - /tmp/*.log /tmp/ray_skypilot/session_latest/logs ~/sky_logs ~/.sky/skylet.log ~/.sky/sky_app 2>/dev/null' \
    > "$out/files.tgz" 2> /dev/null
  mkdir -p "$out/files" && tar -xzf "$out/files.tgz" -C "$out/files" 2> /dev/null
done
