#!/usr/bin/env python3
"""Summarise `python -X importtime` stderr and py-spy speedscope profiles.

Usage:
  importtime_top.py import <file> [-n 25]
  importtime_top.py spy <speedscope.json> [-n 20]
"""
import argparse
import collections
import json
import re
import sys

LINE = re.compile(r'import time:\s+(\d+)\s+\|\s+(\d+)\s+\|(\s*)(\S.*)$')
PKGS = ['sky', 'ray', 'pydantic', 'pydantic_core', 'grpc', 'google', 'requests',
        'urllib3', 'yaml', 'psutil', 'numpy', 'pandas', 'filelock', 'colorama',
        'rich', 'click', 'jsonschema', 'sqlalchemy', 'aiohttp', 'cryptography',
        'typing_extensions', 'packaging', 'pkg_resources', 'setuptools', 'msgpack',
        'opencensus', 'prometheus_client', 'redis', 'wheel']


def parse_importtime(path):
    """Returns list of (module, self_us, cum_us, depth, line_no) and parents."""
    rows = []
    with open(path, errors='replace') as f:
        for i, line in enumerate(f):
            m = LINE.search(line)
            if not m or m.group(4) == 'imported package':
                continue
            self_us, cum_us, ind, mod = int(m.group(1)), int(m.group(2)), m.group(3), m.group(4).strip()
            rows.append([mod, self_us, cum_us, len(ind) // 2, i])
    # Reconstruct tree: children precede parent in output, deeper first.
    parent = {}
    stack = []  # (idx, depth) of pending nodes waiting for a parent
    for idx, r in enumerate(rows):
        d = r[3]
        while stack and rows[stack[-1]][3] > d:
            parent[stack.pop()] = idx
        stack.append(idx)
    return rows, parent


def top_pkg(mod):
    return mod.split('.')[0]


def cmd_import(args):
    rows, parent = parse_importtime(args.file)
    if not rows:
        print('no importtime lines'); return
    total_top = sum(r[2] for r in rows if r[3] == 0)
    print(f'{args.file}: {len(rows)} modules, top-level cumulative total '
          f'{total_top / 1e6:.3f}s, sum of self {sum(r[1] for r in rows) / 1e6:.3f}s')
    print(f'\nTop {args.n} by CUMULATIVE (ms)')
    for r in sorted(rows, key=lambda r: -r[2])[:args.n]:
        print(f'  {r[2] / 1e3:9.1f}  {r[0]}')
    print(f'\nTop {args.n} by SELF (ms)')
    for r in sorted(rows, key=lambda r: -r[1])[:args.n]:
        print(f'  {r[1] / 1e3:9.1f}  {r[0]}')
    selfs = collections.Counter()
    for r in rows:
        selfs[top_pkg(r[0])] += r[1]
    print('\nTop-level package totals (sum of SELF time, ms; no double counting)')
    for p, v in selfs.most_common(args.n):
        print(f'  {v / 1e3:9.1f}  {p}')
    sel = {p: selfs[p] for p in PKGS if p in selfs}
    print('  watched:', ', '.join(f'{p}={v / 1e3:.0f}' for p, v in sel.items()))
    if args.chain:
        idx = [i for i, r in enumerate(rows) if r[0] == args.chain]
        for i in idx[:1]:
            chain = [rows[i][0]]
            while i in parent:
                i = parent[i]; chain.append(rows[i][0])
            print('\nimport chain (leaf -> root):', ' <- '.join(chain))


def cmd_spy(args):
    d = json.load(open(args.file))
    frames = d['shared']['frames']
    incl = collections.Counter()
    selfc = collections.Counter()
    total = 0.0
    for p in d['profiles']:
        if p['type'] == 'sampled':
            for s, w in zip(p['samples'], p['weights']):
                total += w
                seen = set()
                for fi in s:
                    fr = frames[fi]
                    key = f"{fr['name']} ({fr.get('file', '?').split('site-packages/')[-1]}:{fr.get('line', '')})"
                    if key not in seen:
                        incl[key] += w; seen.add(key)
                if s:
                    fr = frames[s[-1]]
                    selfc[f"{fr['name']} ({fr.get('file', '?').split('site-packages/')[-1]})"] += w
        elif p['type'] == 'evented':
            stack = []
            last = p['events'][0]['at'] if p['events'] else 0
            for ev in p['events']:
                dt = ev['at'] - last; last = ev['at']
                for fi in set(stack):
                    fr = frames[fi]
                    incl[f"{fr['name']} ({fr.get('file', '?').split('site-packages/')[-1]})"] += dt
                if ev['type'] == 'O': stack.append(ev['frame'])
                else:
                    if stack: stack.pop()
            total += last
    unit = d['profiles'][0].get('unit', 'seconds') if d['profiles'] else ''
    print(f'{args.file}: {len(d["profiles"])} profile(s), total weight {total:.3f} {unit}')
    print(f'\nTop {args.n} frames by INCLUSIVE time')
    for k, v in incl.most_common(args.n):
        print(f'  {v:8.3f}  {100 * v / total if total else 0:5.1f}%  {k}')
    print(f'\nTop {args.n} by SELF time (leaf)')
    for k, v in selfc.most_common(args.n):
        print(f'  {v:8.3f}  {k}')


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='mode', required=True)
    a = sub.add_parser('import'); a.add_argument('file'); a.add_argument('-n', type=int, default=25)
    a.add_argument('--chain', help='module to print import chain for')
    a.set_defaults(fn=cmd_import)
    b = sub.add_parser('spy'); b.add_argument('file'); b.add_argument('-n', type=int, default=20)
    b.set_defaults(fn=cmd_spy)
    args = ap.parse_args(); args.fn(args)


if __name__ == '__main__':
    sys.exit(main())
