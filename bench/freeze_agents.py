"""Suspend agent work for the duration of one benchmark trial.

    python3 bench/freeze_agents.py <bench_pid> [max_seconds]

Every 0.5 s, SIGSTOPs any process whose working directory is inside an agent
worktree or the session scratchpad (the same set bench/deprioritize_agents.py
renices). On exit it SIGCONTs every process it stopped. It exits when
<bench_pid> exits, on SIGTERM/SIGINT, or after max_seconds (default 600), so
agents are never left suspended. Logs what it froze to stdout.

Why: nice values only rank processes within one cgroup, and the Kind pods run
in Docker's system.slice while agents share user.slice with the API server,
so renicing agents does not protect the pods. Trials ran 1.3-1.5x slower on
every post-Ready stage while agents were busy.
"""
import os
import re
import signal
import sys
import time

PATTERN = re.compile(r'^(/home/tsunny/Documents/skypilot-org/skypilot/\.claude/'
                     r'worktrees/|/home/tsunny/Documents/skypilot-org/'
                     r'skypilot-team|/tmp/claude-1000/)')

bench_pid = int(sys.argv[1])
deadline = time.time() + (float(sys.argv[2]) if len(sys.argv) > 2 else 600)
me = os.getpid()
stopped = {}  # pid -> cmdline
running = True


def _stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def _cmd(pid):
    try:
        with open(f'/proc/{pid}/cmdline', 'rb') as f:
            return f.read().replace(b'\0', b' ').decode(errors='replace')[:160]
    except OSError:
        return '?'


try:
    while running and time.time() < deadline:
        if not os.path.exists(f'/proc/{bench_pid}'):
            break
        for p in os.listdir('/proc'):
            if not p.isdigit():
                continue
            pid = int(p)
            if pid == me or pid in stopped:
                continue
            try:
                cwd = os.readlink(f'/proc/{pid}/cwd')
            except OSError:
                continue
            if PATTERN.match(cwd):
                try:
                    os.kill(pid, signal.SIGSTOP)
                    stopped[pid] = _cmd(pid)
                    print(f'{time.time():.3f} froze {pid} {stopped[pid]}',
                          flush=True)
                except OSError:
                    pass
        time.sleep(0.5)
finally:
    for pid in stopped:
        try:
            os.kill(pid, signal.SIGCONT)
        except OSError:
            pass
    print(f'{time.time():.3f} thawed {len(stopped)} processes', flush=True)
