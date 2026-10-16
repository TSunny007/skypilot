"""Compare trial groups from bench/results.csv.

Usage: python bench/compare.py base=trial2,trial3,base4,base5 a1=a1c1,a1c2,a1c3
"""
import csv
import statistics as st
import sys

COLS = [
    't0_to_first_pod_created', 't0_to_all_pods_ready',
    'all_ready_to_slowest_rank', 't0_to_slowest_rank_HEADLINE',
    'req_cluster_launch_created', 'p_provision_start', 'p_ray_not_ready_checks',
    'queue_saw_RUNNING', 'terminal_seen', 'teardown_done', 'host_cpu_busy_pct',
    'host_load1_max'
]
rows = {r['trial']: r for r in csv.DictReader(open('bench/results.csv'))}
groups = [(g.split('=')[0], g.split('=')[1].split(',')) for g in sys.argv[1:]]
for name, trials in groups:
    print(f'{name}: ' + '  '.join(
        f'{t}={rows[t]["t0_to_slowest_rank_HEADLINE"]}' for t in trials))
print(f'{"metric":30}' + ''.join(f'{n:>22}' for n, _ in groups))
for c in COLS:
    out = f'{c:30}'
    for _, trials in groups:
        v = [float(rows[t][c]) for t in trials if rows[t].get(c)]
        out += (f'{st.mean(v):9.2f} [{min(v):.1f}-{max(v):.1f}]'.rjust(22)
                if v else ' ' * 22)
    print(out)
