#!/usr/bin/env bash
# Runs INSIDE a pod (piped via `kubectl exec -i pod -- bash -s <role> <outdir>`).
# Side-effect free: imports, `ray status`, ray.init() as a short-lived driver.
ROLE=${1:?}; OUT=${2:?}
PY=$HOME/skypilot-runtime/bin/python
PYSPY=$HOME/skypilot-runtime/bin/py-spy
RAYP=$(cat ~/.sky/ray_path)
mkdir -p "$OUT/cmds" "$OUT/importtime" "$OUT/pyspy"
CSV=$OUT/wall.csv; : > "$CSV"
{ echo "== caps"; grep Cap /proc/self/status; echo "== ptrace_scope"; cat /proc/sys/kernel/yama/ptrace_scope
  grep Seccomp /proc/self/status; echo "== py-spy"; $PYSPY --version; uptime; nproc; } > "$OUT/feasibility.txt" 2>&1
( cd "$OUT" && $PYSPY record -o selftest.json --format speedscope --rate 250 -- $PY -c 'import time; time.sleep(0.3)' ) >> "$OUT/feasibility.txt" 2>&1
echo "selftest rc=$?" >> "$OUT/feasibility.txt"

defcmd() { cat > "$OUT/cmds/$1.sh"; }
export PY RAYP
defcmd C1a_portread <<'X'
cd $HOME; $PY -c "import json, os; d = os.path.expanduser(os.environ.get('SKY_RUNTIME_DIR', '~')); print(json.load(open(os.path.join(d, '.sky/ray_port.json')))['ray_port'])" 2>/dev/null || echo 6380
X
defcmd C1b_message_utils <<'X'
cd $HOME; $PY -c "from sky.utils import message_utils; print(message_utils.encode_payload({'ray_port': 6380}))"
X
defcmd C2a_ray_import <<'X'
cd $HOME; $PY -c "import ray.scripts.scripts"
X
defcmd C2b_ray_status <<'X'
cd $HOME; RAY_ADDRESS=127.0.0.1:6380 $PY $RAYP status
X
defcmd C3a_autostop_imports <<'X'
cd $HOME; $PY -u -c "from sky.skylet import autostop_lib, constants"
X
defcmd C3b_preamble_imports <<'X'
cd $HOME; $PY -u -c "import os;import getpass;import sys;from sky import exceptions;from sky.skylet import log_lib, job_lib, constants"
X
defcmd C3c_attempt_skylet_deps <<'X'
cd $HOME; $PY -u -c "import os, shlex, signal, socket, subprocess, time, psutil; from sky.skylet import constants, runtime_utils; from sky.utils import common_utils"
X
defcmd C3d_import_sky <<'X'
cd $HOME; $PY -c "import sky"
X
defcmd C4_first_task_imports <<'X'
cd $HOME; $PY -c "from sky.skylet import autostop_lib, constants, job_lib; from sky.utils import log_utils, subprocess_utils"
X
defcmd C5a_import_ray <<'X'
cd $HOME; $PY -c "import ray"
X
defcmd C5b_ray_init <<'X'
cd $HOME; $PY -c "
import ray
ray.init(address='127.0.0.1:6380', logging_level='ERROR')
ray.shutdown()"
X
defcmd Z0_python_noop <<'X'
$PY -c "pass"
X
defcmd Z1_python_noop_nosite <<'X'
$PY -S -c "pass"
X

if [ "$ROLE" = head ]; then
  CMDS="Z0_python_noop Z1_python_noop_nosite C1a_portread C1b_message_utils C2a_ray_import C2b_ray_status C3a_autostop_imports C3b_preamble_imports C3c_attempt_skylet_deps C3d_import_sky C4_first_task_imports C5a_import_ray C5b_ray_init"
else
  CMDS="Z0_python_noop Z1_python_noop_nosite C4_first_task_imports C3d_import_sky C1b_message_utils"
fi

[ -n "${3:-}" ] && CMDS=$3
now() { date +%s.%N; }
runc() {  # runc <name> <cond> <extra env assignments...>
  local name=$1 cond=$2; shift 2
  env "$@" bash "$OUT/cmds/$name.sh"
}
for name in $CMDS; do
  bash "$OUT/cmds/$name.sh" >/dev/null 2>&1   # warm-up (primes pycache)
  for cond in warm cold; do
    for i in 1 2 3; do
      if [ $cond = cold ]; then d=$(mktemp -d); E="PYTHONPYCACHEPREFIX=$d"; else d=''; E="X_NOP=1"; fi
      s=$(now); runc $name $cond $E >"$OUT/cmds/$name.$cond.$i.out" 2>&1; rc=$?; e=$(now)
      echo "$name,$cond,$i,$(awk "BEGIN{print $e-$s}"),$rc" >> "$CSV"
      [ -n "$d" ] && rm -rf "$d"
    done
    # importtime (single run; stderr holds the table)
    if [ $cond = cold ]; then d=$(mktemp -d); E="PYTHONPYCACHEPREFIX=$d"; else d=''; E="X_NOP=1"; fi
    runc $name $cond $E PYTHONPROFILEIMPORTTIME=1 >/dev/null 2>"$OUT/importtime/$name.$cond.txt"
    [ -n "$d" ] && rm -rf "$d"
    # py-spy
    if [ $cond = cold ]; then d=$(mktemp -d); E="PYTHONPYCACHEPREFIX=$d"; else d=''; E="X_NOP=1"; fi
    ( cd "$OUT/pyspy" && env $E $PYSPY record --rate 250 --subprocesses --format speedscope \
        -o "$name.$cond.json" -- bash "$OUT/cmds/$name.sh" >/dev/null 2>"$name.$cond.err" )
    [ -n "$d" ] && rm -rf "$d"
  done
done
echo done > "$OUT/DONE"
