"""Print a SkyPilot log as a timeline relative to a trial's t0.

Usage: python bench/logtl.py <run_dir> <log_file> [grep_regex]

SkyPilot log lines look like `D 10-15 13:18:09.405 PID=123 file.py:282] msg`
(local time, no year). Consecutive lines from the same file:line (multi-line
dumps) are collapsed to their first line.
"""
import datetime
import os
import re
import sys

LINE_RE = re.compile(r'^([DIWE]) (\d\d)-(\d\d) (\d\d:\d\d:\d\d\.\d+) '
                     r'PID=(\d+) ([\w.]+:\d+)\] ?(.*)$')


def parse(path, t0):
    """Yield (seconds since t0, level, pid, file:line, msg), collapsed."""
    year = datetime.datetime.fromtimestamp(t0).year
    prev_loc = None
    with open(path, encoding='utf-8', errors='replace') as f:
        for line in f:
            m = LINE_RE.match(line.rstrip('\n'))
            if not m:
                continue
            lvl, mo, dd, hms, pid, loc, msg = m.groups()
            if loc == prev_loc:
                continue
            prev_loc = loc
            ts = datetime.datetime.strptime(f'{year}-{mo}-{dd} {hms}',
                                            '%Y-%m-%d %H:%M:%S.%f').timestamp()
            yield ts - t0, lvl, pid, loc, msg


def main():
    run, path = sys.argv[1], sys.argv[2]
    pat = re.compile(sys.argv[3]) if len(sys.argv) > 3 else None
    with open(os.path.join(run, 't0'), encoding='utf-8') as f:
        t0 = float(f.read())
    for rel, lvl, pid, loc, msg in parse(path, t0):
        out = f'{rel:8.3f} {lvl} {pid:>7} {loc:34} {msg[:160]}'
        if pat is None or pat.search(out):
            print(out)


if __name__ == '__main__':
    main()
