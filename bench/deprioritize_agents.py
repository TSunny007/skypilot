"""Keep agent work from perturbing benchmarks.

Every second, drop any process whose working directory is inside an agent
worktree or the session scratchpad to the lowest CPU priority (nice 19) and
the idle I/O class. Benchmark processes (API server, controllers, Kind) keep
normal priority, so agents only get CPU the benchmark is not using.

    setsid nohup python3 bench/deprioritize_agents.py &
"""
import ctypes
import os
import re
import time

PATTERN = re.compile(r'^(/home/tsunny/Documents/skypilot-org/skypilot/\.claude/'
                     r'worktrees/|/home/tsunny/Documents/skypilot-org/'
                     r'skypilot-team|/tmp/claude-1000/)')
_libc = ctypes.CDLL(None, use_errno=True)
_SYS_IOPRIO_SET = 251  # x86_64
_IOPRIO_WHO_PROCESS = 1
_IOPRIO_IDLE = 3 << 13

seen = set()
while True:
    pids = {int(p) for p in os.listdir('/proc') if p.isdigit()}
    seen &= pids
    for pid in pids - seen:
        try:
            cwd = os.readlink(f'/proc/{pid}/cwd')
        except OSError:
            continue
        seen.add(pid)
        if PATTERN.match(cwd):
            try:
                os.setpriority(os.PRIO_PROCESS, pid, 19)
                _libc.syscall(_SYS_IOPRIO_SET, _IOPRIO_WHO_PROCESS, pid,
                              _IOPRIO_IDLE)
            except OSError:
                pass
    time.sleep(1)
