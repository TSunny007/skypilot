"""Generate prototype pods + headless services for the pod-side design floor.

Not SkyPilot code: raw Kubernetes objects derived from a pod spec SkyPilot
actually created (bench/runs/trial3/pods.json), with only the container
command replaced. Same image, resources, env, pull policy, /dev/shm volume.

  python3 bench/proto/proto.py <mode> <id> [--n 4] [--pull Always]

mode:
  selfstart  each pod rendezvouses on the per-pod headless services (DNS +
             TCP barrier) and runs `run:` directly. No Ray on the path.
  rayfast    Ray kept but started at container start (no `ray stop`, no
             dashboard); the head submits a slim placement-group driver
             itself as soon as its GCS is up.
"""
import argparse
import copy
import json
import os

BENCH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(BENCH, 'runs/trial3/pods.json')

RUN = ('echo "[rank ${SKYPILOT_NODE_RANK}] started at $(date +%s.%N) '
       'on $(hostname)"')

ENTRY = r'''
set -u
ts(){ date +%s.%N; }
echo "[proto] container_start $(ts) rank=$PROTO_RANK host=$(hostname) user=$(id -un)"
NS=default
PY=/home/sky/skypilot-runtime/bin/python
RAY=/home/sky/skypilot-runtime/bin/ray
names=("$PROTO_ID-head"); for i in $(seq 1 $((PROTO_N-1))); do names+=("$PROTO_ID-worker$i"); done

if [ "$PROTO_MODE" = selfstart ]; then
  # "This node finished its local bootstrap" = listener is up.
  $PY -c '
import socket
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", 7777)); s.listen(128)
while True:
    c, _ = s.accept(); c.close()' &
  ips=()
  for nm in "${names[@]}"; do
    fq="$nm.$NS.svc.cluster.local"; ip=""
    until ip=$(getent hosts "$fq" | awk '{print $1; exit}') && [ -n "$ip" ] \
          && timeout 1 bash -c ": < /dev/tcp/$ip/7777" 2>/dev/null; do sleep 0.05; done
    ips+=("$ip")
  done
  echo "[proto] barrier_done $(ts)"
  export SKYPILOT_NODE_RANK=$PROTO_RANK SKYPILOT_NUM_NODES=$PROTO_N
  export SKYPILOT_NODE_IPS="$(printf '%s\n' "${ips[@]}")"
  bash -c "$PROTO_RUN"
elif [ "$PROTO_MODE" = rayfast ]; then
  FLAGS="--disable-usage-stats --num-cpus=1 --temp-dir=/tmp/ray_proto --node-ip-address=$SKYPILOT_RAY_NODE_IP"
  # Same fix SkyPilot applies before every ray start (constants.py:404): the
  # image's runtime venv lacks setuptools, and Ray's dashboard agent (which
  # the raylet fate-shares with) dies importing pkg_resources without it.
  UV=$HOME/.local/bin/uv; command -v uv >/dev/null && UV=uv
  UV_LINK_MODE=copy $UV pip install -q --python $PY "setuptools<70" "click<8.3.0" > /tmp/uv_fix.log 2>&1
  echo "[proto] ray_deps_fixed $(ts) rc=$?"
  if [ "$PROTO_RANK" = 0 ]; then
    $RAY start --head --port=6380 --include-dashboard=false $FLAGS > /tmp/ray_start.log 2>&1
    echo "[proto] ray_head_up $(ts) rc=$?"
    printf '%s\n' "$PROTO_DRIVER" > /tmp/proto_driver.py
    $PY -u /tmp/proto_driver.py 2>&1
  else
    head="$PROTO_ID-head.$NS.svc.cluster.local"
    until timeout 1 bash -c ": < /dev/tcp/$head/6380" 2>/dev/null; do sleep 0.1; done
    echo "[proto] head_reachable $(ts)"
    $RAY start --address=$head:6380 $FLAGS > /tmp/ray_start.log 2>&1
    echo "[proto] ray_worker_up $(ts) rc=$?"
  fi
fi
sleep 900
'''

DRIVER = r'''
import os, subprocess, time
def ts(): return f'{time.time():.6f}'
print('[proto] driver_start', ts(), flush=True)
import ray
from ray.util.placement_group import placement_group, placement_group_table
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
print('[proto] driver_imported', ts(), flush=True)
N = int(os.environ['PROTO_N'])
ray.init(address='auto', log_to_driver=False)
print('[proto] ray_init_done', ts(), flush=True)
pg = placement_group([{'CPU': 1}] * N, strategy='STRICT_SPREAD')
ray.get(pg.ready())
print('[proto] pg_ready', ts(), flush=True)
b2n = placement_group_table(pg)['bundles_to_node_id']
addr = {n['NodeID']: n['NodeManagerAddress'] for n in ray.nodes() if n['Alive']}
bundle_ips = [addr[b2n[i]] for i in range(N)]
head_ip = os.environ['SKYPILOT_RAY_NODE_IP']
order = sorted(range(N), key=lambda i: (bundle_ips[i] != head_ip, bundle_ips[i]))
ips = [bundle_ips[i] for i in order]

@ray.remote
def run(rank, ips, cmd):
    env = dict(os.environ, SKYPILOT_NODE_RANK=str(rank),
               SKYPILOT_NUM_NODES=str(len(ips)), SKYPILOT_NODE_IPS='\n'.join(ips))
    with open('/proc/1/fd/1', 'w') as out:
        return subprocess.call(['bash', '-c', cmd], env=env, stdout=out, stderr=out)

refs = [run.options(num_cpus=1, scheduling_strategy=PlacementGroupSchedulingStrategy(
            pg, placement_group_bundle_index=b)).remote(r, ips, os.environ['PROTO_RUN'])
        for r, b in enumerate(order)]
print('[proto] tasks_submitted', ts(), flush=True)
print('[proto] results', ray.get(refs), ts(), flush=True)
'''


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('mode', choices=['selfstart', 'rayfast'])
    ap.add_argument('id')
    ap.add_argument('--n', type=int, default=4)
    ap.add_argument('--pull', default='Always')
    ap.add_argument('--cpu', default=None,
                    help='override the CPU request (N=8 does not fit 1 CPU '
                    'each on the single 8-core Kind node)')
    a = ap.parse_args()

    src = json.load(open(SRC))['items'][0]
    sspec = src['spec']
    sc = sspec['containers'][0]
    keep_vols = [v for v in sspec.get('volumes', [])
                 if not v['name'].startswith('kube-api-access')]
    keep_mounts = [m for m in sc.get('volumeMounts', [])
                   if not m['name'].startswith('kube-api-access')]

    resources = copy.deepcopy(sc.get('resources', {}))
    if a.cpu:
        resources.setdefault('requests', {})['cpu'] = a.cpu
    items = []
    for i in range(a.n):
        name = f'{a.id}-head' if i == 0 else f'{a.id}-worker{i}'
        labels = {'bench-proto': a.id, 'component': name}
        env = [e for e in sc.get('env', [])]
        env += [{'name': 'PROTO_MODE', 'value': a.mode},
                {'name': 'PROTO_ID', 'value': a.id},
                {'name': 'PROTO_N', 'value': str(a.n)},
                {'name': 'PROTO_RANK', 'value': str(i)},
                {'name': 'PROTO_RUN', 'value': RUN},
                {'name': 'PROTO_DRIVER', 'value': DRIVER}]
        container = {
            'name': sc['name'],
            'image': sc['image'],
            'imagePullPolicy': a.pull,
            'resources': resources,
            'env': env,
            'volumeMounts': keep_mounts,
            'command': ['/bin/bash', '-c', '--'],
            'args': [ENTRY],
        }
        spec = {
            'restartPolicy': 'Never',
            'terminationGracePeriodSeconds': 0,
            'containers': [container],
            'volumes': keep_vols,
        }
        for k in ('serviceAccountName', 'dnsPolicy', 'enableServiceLinks',
                  'tolerations'):
            if k in sspec:
                spec[k] = copy.deepcopy(sspec[k])
        items.append({'apiVersion': 'v1', 'kind': 'Service',
                      'metadata': {'name': name, 'labels': {'bench-proto': a.id}},
                      'spec': {'clusterIP': 'None',
                               'selector': {'component': name}}})
        items.append({'apiVersion': 'v1', 'kind': 'Pod',
                      'metadata': {'name': name, 'labels': labels},
                      'spec': spec})
    # Services first (SkyPilot also creates them before the pods).
    items.sort(key=lambda o: o['kind'] != 'Service')
    print(json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': items}))


if __name__ == '__main__':
    main()
