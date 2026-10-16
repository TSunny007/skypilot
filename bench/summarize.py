"""Turn bench/runs/<trial>/ artifacts into bench/results.csv + bench/pods.csv.

Usage: python bench/summarize.py [run_dir ...]   (default: runs/trial*, by t0)

All times are seconds relative to the trial's t0 (host clock, taken right
before `sky jobs launch`). Two sources for pod milestones:
  w_*   host time the pod-watch event arrived (sub-second; primary)
  k8s_* timestamps K8s itself recorded (RFC3339, truncated to whole seconds)
The first trial in the series is marked cold.
"""
import csv
import datetime
import glob
import json
import os
import re
import sys

import logtl

BENCH = os.path.dirname(os.path.abspath(__file__))
# provision.log milestones: column -> regex on the message (first match wins).
PROV_MARKS = (
    ('p_provision_start', r'SkyPilot version'),
    ('p_create_pods_call', r'calling create_namespaced_pod'),
    ('p_saw_all_running', r'all pods are scheduled and running'),
    ('p_pods_up', r'Pods are up'),
    ('p_file_mounts_end', r'End:\s+internal_file_mounts'),
    ('p_setup_runtime_end', r'End:\s+setup_runtime_on_cluster'),
    ('p_ray_ready', r'Ray cluster is ready'),
    ('p_skylet_end', r'End:\s+start_skylet_on_head_node'),
    ('p_cluster_launched', r'Cluster launched'),
)
RANK_RE = re.compile(r'\[rank (\d+)\] started at ([0-9.]+) on (\S+)')
CONDS = ('PodScheduled', 'Initialized', 'ContainersReady', 'Ready')


def k8s_ts(s):
    if not s:
        return None
    return datetime.datetime.fromisoformat(s.replace('Z', '+00:00')).timestamp()


def cond_true(pod, ctype):
    for c in (pod.get('status') or {}).get('conditions') or []:
        if c['type'] == ctype and c['status'] == 'True':
            return c
    return None


def container_started(pod):
    for cs in (pod.get('status') or {}).get('containerStatuses') or []:
        running = (cs.get('state') or {}).get('running')
        if running:
            return running.get('startedAt')
    return None


def pods_from_watch(run):
    """Per pod: first-arrival time of each milestone + last live object."""
    pods = {}
    path = os.path.join(run, 'pod_events.jsonl')
    if not os.path.exists(path):
        return pods
    with open(path, encoding='utf-8') as f:
        for line in f:
            ev = json.loads(line)
            obj, t = ev['obj'], ev['t']
            name = obj['metadata']['name']
            p = pods.setdefault(name, {'obj': None})
            p.setdefault('w_created', t)
            if ev['type'] == 'DELETED':
                p.setdefault('w_deleted', t)
                continue
            if obj['metadata'].get('deletionTimestamp'):
                p.setdefault('w_terminating', t)
                continue  # Conditions start flipping back; ignore.
            p['obj'] = obj
            for c in CONDS:
                if cond_true(obj, c):
                    p.setdefault(f'w_{c}', t)
            if container_started(obj):
                p.setdefault('w_container_running', t)
    return pods


def pods_from_snapshot(run):
    path = os.path.join(run, 'pods.json')
    if not os.path.exists(path):
        return {}
    with open(path, encoding='utf-8') as f:
        items = json.load(f)['items']
    return {p['metadata']['name']: {'obj': p} for p in items}


def add_k8s_fields(p):
    obj = p.get('obj')
    if not obj:
        return
    p['role'] = obj['metadata'].get('labels', {}).get('ray-node-type', '?')
    p['image'] = obj['spec']['containers'][0]['image']
    p['k8s_created'] = k8s_ts(obj['metadata'].get('creationTimestamp'))
    for c in CONDS:
        ct = cond_true(obj, c)
        p[f'k8s_{c}'] = k8s_ts(ct['lastTransitionTime']) if ct else None
    p['k8s_container_started'] = k8s_ts(container_started(obj))


def ranks(run):
    """rank -> (epoch, hostname); job logs first, launch client log fallback."""
    out = {}
    for fn in ('job_logs.txt', 'launch.log'):
        path = os.path.join(run, fn)
        if not os.path.exists(path):
            continue
        with open(path, encoding='utf-8', errors='replace') as f:
            for m in RANK_RE.finditer(f.read()):
                out.setdefault(int(m.group(1)), (float(m.group(2)), m.group(3)))
        if out:
            break
    return out


def load_json(run, rel, default=None):
    path = os.path.join(run, rel)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return default
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def queue_first_seen(run):
    """status -> first poll response time that showed it."""
    out = {}
    path = os.path.join(run, 'queue_transitions.log')
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            for line in f:
                _, ts_resp, st = line.split()
                out.setdefault(st, float(ts_resp))
    return out


def image_cached_before(run, image):
    for path in glob.glob(os.path.join(run, 'pre', 'images_*.json')):
        with open(path, encoding='utf-8') as f:
            for img in json.load(f).get('images', []):
                if image in (img.get('repoTags') or []):
                    return True
    return False


def rnd(x):
    return None if x is None else round(x, 3)


def summarize(run, cold):
    meta = load_json(run, 'meta.json', {})
    t0 = meta.get('t0')
    if t0 is None:
        with open(os.path.join(run, 't0'), encoding='utf-8') as f:
            t0 = float(f.read())
    pods = pods_from_watch(run) or pods_from_snapshot(run)
    for p in pods.values():
        add_k8s_fields(p)
    rk = ranks(run)
    host_to_rank = {h: (r, t) for r, (t, h) in rk.items()}

    rel = lambda t: None if t is None else t - t0  # noqa: E731
    pod_rows = []
    for name, p in sorted(pods.items()):
        r, rt = host_to_rank.get(name, (None, None))
        row = {'trial': os.path.basename(run), 'pod': name,
               'role': p.get('role'), 'rank': r}
        for k in ('w_created', 'w_PodScheduled', 'w_Initialized',
                  'w_container_running', 'w_ContainersReady', 'w_Ready',
                  'k8s_created', 'k8s_PodScheduled', 'k8s_Initialized',
                  'k8s_container_started', 'k8s_ContainersReady', 'k8s_Ready',
                  'w_terminating', 'w_deleted'):
            row[k] = rnd(rel(p.get(k)))
        row['rank_started'] = rnd(rel(rt))
        pod_rows.append(row)

    def agg(fn, key, role=None):
        vals = [p.get(key) for p in pods.values()
                if p.get(key) is not None and (role is None or
                                               p.get('role') == role)]
        return fn(vals) if vals else None

    rank_ts = [t for t, _ in rk.values()]
    first_rank = min(rank_ts) if rank_ts else None
    last_rank = max(rank_ts) if rank_ts else None
    res = {'trial': os.path.basename(run), 'cold': int(cold),
           'job_id': meta.get('job_id'), 'status': meta.get('status'),
           'n_pods': len(pods), 'n_ranks_seen': len(rk)}
    image = next((p.get('image') for p in pods.values() if p.get('image')),
                 None)
    res['image_cached_before'] = (int(image_cached_before(run, image))
                                  if image else None)
    for src in ('w', 'k8s'):
        created = agg(min, f'{src}_created')
        all_ready = agg(max, f'{src}_Ready')
        head_ready = agg(max, f'{src}_Ready', 'head')
        last_worker_ready = agg(max, f'{src}_Ready', 'worker')
        sfx = '' if src == 'w' else '_k8s'
        res[f't0_to_first_pod_created{sfx}'] = rnd(rel(created))
        res[f't0_to_all_pods_ready{sfx}'] = rnd(rel(all_ready))
        res[f'all_ready_to_first_rank{sfx}'] = rnd(
            first_rank - all_ready if first_rank and all_ready else None)
        res[f'all_ready_to_slowest_rank{sfx}'] = rnd(
            last_rank - all_ready if last_rank and all_ready else None)
        res[f'head_ready_to_last_worker_ready{sfx}'] = rnd(
            last_worker_ready - head_ready
            if head_ready and last_worker_ready else None)
        if src == 'w':
            # Headline sits right after the pod-ready columns.
            res['t0_to_slowest_rank_HEADLINE'] = rnd(rel(last_rank))
            res['t0_to_first_rank'] = rnd(rel(first_rank))
            res['rank_spread'] = rnd(
                last_rank - first_rank if rank_ts else None)

    # Provision stages (provision.log, ms timestamps, host clock).
    prov = os.path.join(run, 'logs', 'provision.log')
    if os.path.exists(prov):
        lines = list(logtl.parse(prov, t0))
        for col, pat in PROV_MARKS:
            rx = re.compile(pat)
            res[col] = rnd(next((t for t, *_, msg in lines if rx.search(msg)),
                                None))
        res['p_ray_not_ready_checks'] = sum(
            'Ray cluster is not ready yet' in msg for *_, msg in lines)
    # API requests (requests.db created_at / finished_at).
    for r in load_json(run, 'db/requests.json') or []:
        key = {'sky.jobs.launch': 'req_jobs_launch',
               'sky.launch': 'req_cluster_launch'}.get(r['name'])
        if key and f'{key}_created' not in res:
            res[f'{key}_created'] = rnd(rel(r['created_at']))
            res[f'{key}_finished'] = rnd(rel(r['finished_at']))
    # Managed-job status: first seen by `sky jobs queue` polling (coarse).
    qfs = queue_first_seen(run)
    for st in ('STARTING', 'RUNNING', 'SUCCEEDED'):
        res[f'queue_saw_{st}'] = rnd(rel(qfs.get(st)))
    # SkyPilot's own records.
    spot = (load_json(run, 'db/spot.json') or [{}])[0]
    for k in ('submitted_at', 'start_at', 'end_at'):
        res[f'db_{k}'] = rnd(rel(spot.get(k)))
    la = load_json(run, 'db/launch_attempts.json') or []
    if la:
        a = max(la, key=lambda x: x.get('attempt_seq') or 0)
        for k in ('provision_start', 'instances_requested', 'admitted',
                  'instances_ready'):
            res[f'la_{k}'] = rnd(rel(a.get(k)))
        res['n_launch_attempts'] = len(la)
        # SkyPilot's own phase split, recomputed offline with the formula in
        # sky/metrics/launch_phases.py:compute_job_timeline (the t_* columns
        # are only written when server metrics are on).
        if all(spot.get(k) for k in ('eligible_at', 'submitted_at',
                                     'start_at')):
            first = min(la, key=lambda x: x.get('attempt_seq') or 0)
            retry = a['provision_start'] - first['provision_start']
            startup_from = a.get('instances_requested') or a['provision_start']
            res['sp_controller_queue'] = rnd(spot['submitted_at'] -
                                             spot['eligible_at'])
            res['sp_retry_overhead'] = rnd(retry)
            res['sp_provision_setup'] = rnd(startup_from -
                                            spot['submitted_at'] - retry)
            if a.get('admitted'):
                res['sp_queue_wait'] = rnd(a['admitted'] - startup_from)
            res['sp_node_startup'] = rnd(a['instances_ready'] -
                                         (a.get('admitted') or startup_from))
            res['sp_runtime_setup'] = rnd(spot['start_at'] -
                                          a['instances_ready'])
            res['sp_time_to_running'] = rnd(spot['start_at'] -
                                            spot['eligible_at'])
    # Host CPU busy % over [t0, slowest rank] from host_load.log (/proc/stat).
    hl = os.path.join(run, 'host_load.log')
    end = res.get('t0_to_slowest_rank_HEADLINE')
    if os.path.exists(hl) and end:
        samples = []
        for line in open(hl):
            f = line.split()
            if len(f) < 9 or f[4] != 'cpu':
                continue
            vals = [int(x) for x in f[5:]]
            idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
            samples.append((float(f[0]) - t0, sum(vals), idle, float(f[1])))
        win = [x for x in samples if 0 <= x[0] <= end]
        if len(win) >= 2:
            dt, di = win[-1][1] - win[0][1], win[-1][2] - win[0][2]
            res['host_cpu_busy_pct'] = rnd(100.0 * (dt - di) / dt if dt else None)
            res['host_load1_max'] = max(x[3] for x in win)
    if meta:
        res['terminal_seen'] = rnd(rel(meta['t_terminal_seen']))
        res['teardown_done'] = rnd(rel(meta['t_teardown_done']))
        res['launch_client_exit'] = rnd(rel(meta['t_launch_exit']))
    return res, pod_rows


def main():
    runs = sys.argv[1:] or [
        d for d in glob.glob(os.path.join(BENCH, 'runs', 'trial*'))
        if os.path.exists(os.path.join(d, 't0'))
    ]

    def t0_of(d):
        with open(os.path.join(d, 't0'), encoding='utf-8') as f:
            return float(f.read())

    runs = sorted(runs, key=t0_of)
    results, all_pods = [], []
    for i, run in enumerate(runs):
        res, pod_rows = summarize(run, cold=(i == 0))
        results.append(res)
        all_pods.extend(pod_rows)
    for path, rows in ((os.path.join(BENCH, 'results.csv'), results),
                       (os.path.join(BENCH, 'pods.csv'), all_pods)):
        if not rows:
            continue
        keys = list(dict.fromkeys(k for r in rows for k in r))
        with open(path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(rows)
    for r in results:
        print(json.dumps(r))


if __name__ == '__main__':
    main()
