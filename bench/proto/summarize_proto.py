"""Summarize prototype trials (bench/runs/proto/*) into bench/proto/results.csv.

All times are seconds after t0 (host clock right before `kubectl create`).
The SkyPilot equivalent of t0 here is its create-pods call (p_create_pods_call
in bench/results.csv), so compare `create_to_slowest_rank` with
SkyPilot's `t0_to_slowest_rank_HEADLINE - p_create_pods_call`.
"""
import csv
import glob
import json
import os
import re
import sys

BENCH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RANK_RE = re.compile(r'\[rank (\d+)\] started at ([0-9.]+) on (\S+)')
MARK_RE = re.compile(r'\[proto\] (\w+) ([0-9.]+)')


def pod_times(run, t0):
    """First host-time watch event at which each pod reached each state."""
    out = {}
    for line in open(os.path.join(run, 'pod_events.jsonl')):
        ev = json.loads(line)
        o = ev['obj']
        name = o['metadata']['name']
        d = out.setdefault(name, {})
        d.setdefault('created', ev['t'] - t0)
        for c in o.get('status', {}).get('conditions', []) or []:
            if c.get('status') == 'True':
                d.setdefault(c['type'], ev['t'] - t0)
        for cs in o.get('status', {}).get('containerStatuses', []) or []:
            if 'running' in (cs.get('state') or {}):
                d.setdefault('running', ev['t'] - t0)
    return out


def summarize(run):
    meta = json.load(open(os.path.join(run, 'meta.json')))
    t0 = meta['t0']
    pods = pod_times(run, t0)
    ranks, marks = {}, {}
    for f in glob.glob(os.path.join(run, '*.log')):
        name = os.path.basename(f)[:-4]
        if name.endswith('.ray_start'):
            continue
        for line in open(f, errors='replace'):
            m = RANK_RE.search(line)
            if m:
                ranks[int(m.group(1))] = float(m.group(2)) - t0
            m = MARK_RE.search(line)
            if m:
                marks.setdefault(name, {})[m.group(1)] = float(m.group(2)) - t0
    ready = [d.get('Ready') for d in pods.values()]
    row = {
        'trial': meta['trial'], 'mode': meta['mode'], 'n': meta['n'],
        'pull': meta['pull'],
        'create_returned': meta['t_create_returned'] - t0,
        'first_pod_created': min(d['created'] for d in pods.values()),
        'all_pods_ready': max(ready) if None not in ready else None,
        'first_rank': min(ranks.values()) if ranks else None,
        'slowest_rank': (max(ranks.values())
                         if len(ranks) == meta['n'] else None),
        'ranks_seen': len(ranks),
    }
    if row['all_pods_ready'] is not None and row['slowest_rank'] is not None:
        row['ready_to_slowest_rank'] = row['slowest_rank'] - row['all_pods_ready']
        row['rank_spread'] = row['slowest_rank'] - row['first_rank']
    # Per-pod container start (from the pod's own first line) and markers.
    for name, mk in sorted(marks.items()):
        role = 'head' if name.endswith('-head') else 'w' + name.rsplit('worker', 1)[-1]
        for k, v in mk.items():
            row[f'{role}.{k}'] = v
    for name, d in sorted(pods.items()):
        role = 'head' if name.endswith('-head') else 'w' + name.rsplit('worker', 1)[-1]
        for k in ('PodScheduled', 'running', 'Ready'):
            if k in d:
                row[f'{role}.k8s_{k}'] = d[k]
    return row


def main():
    runs = sys.argv[1:] or sorted(glob.glob(os.path.join(BENCH, 'runs/proto/*')))
    if len(sys.argv) > 1:
        # Single-run mode: print it, then rebuild the CSV from all runs.
        for r in runs:
            row = summarize(r)
            for k, v in row.items():
                print(f'{k:32s} {v:.3f}' if isinstance(v, float) else f'{k:32s} {v}')
        runs = sorted(glob.glob(os.path.join(BENCH, 'runs/proto/*')))
    rows = []
    for r in runs:
        try:
            rows.append(summarize(r))
        except Exception as e:  # pylint: disable=broad-except
            print(f'skip {r}: {e}', file=sys.stderr)
    keys = []
    for r in rows:
        keys += [k for k in r if k not in keys]
    with open(os.path.join(BENCH, 'proto/results.csv'), 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: (f'{v:.3f}' if isinstance(v, float) else v)
                        for k, v in r.items()})


if __name__ == '__main__':
    main()
