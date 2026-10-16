"""Trace short-lived kubectl/rsync processes on the host (diagnostic runs).

Polls /proc every ~0.06s (0.05s sleep + ~10ms scan, ~17% of one core). For
every process whose comm matches, records its start time (from
/proc/<pid>/stat starttime, 10ms resolution), the time it was last seen
alive, and its command line. Processes living <~60ms can be missed. Writes
JSONL to stdout on SIGTERM/SIGINT.

Usage: python bench/proctrace.py > proctrace.jsonl &
"""
import json
import os
import signal
import sys
import time

WATCH = {'kubectl', 'rsync'}
CLK_TCK = os.sysconf('SC_CLK_TCK')
with open('/proc/stat', encoding='utf-8') as f:
    BTIME = next(int(l.split()[1]) for l in f if l.startswith('btime'))

procs = {}  # (pid, starttime_ticks) -> record


def flush(*_):
    for rec in sorted(procs.values(), key=lambda r: r['start']):
        sys.stdout.write(json.dumps(rec) + '\n')
    sys.stdout.flush()
    os._exit(0)  # pylint: disable=protected-access


signal.signal(signal.SIGTERM, flush)
signal.signal(signal.SIGINT, flush)

while True:
    now = time.time()
    for pid in os.listdir('/proc'):
        if not pid.isdigit():
            continue
        try:
            with open(f'/proc/{pid}/stat', encoding='utf-8') as f:
                stat = f.read()
            comm = stat[stat.index('(') + 1:stat.rindex(')')]
            if comm not in WATCH:
                continue
            fields = stat[stat.rindex(')') + 2:].split()
            ppid, start_ticks = int(fields[1]), int(fields[19])
            key = (int(pid), start_ticks)
            rec = procs.get(key)
            if rec is None:
                with open(f'/proc/{pid}/cmdline', 'rb') as f:
                    cmd = f.read().replace(b'\0', b' ').decode(errors='replace')
                rec = procs[key] = {
                    'pid': int(pid),
                    'ppid': ppid,
                    'comm': comm,
                    'start': BTIME + start_ticks / CLK_TCK,
                    'first_seen': now,
                    'cmd': cmd[:4000],
                }
            rec['last_seen'] = now
        except (FileNotFoundError, ProcessLookupError, ValueError):
            continue
    time.sleep(0.05)
