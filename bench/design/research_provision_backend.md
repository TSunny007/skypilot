# Research: provision + backend launch path on Kubernetes (what the teams don't cover yet)

Scope: `sky/provision/kubernetes/`, `sky/provision/provisioner.py`, `sky/provision/instance_setup.py`, `sky/backends/cloud_vm_ray_backend.py`, and what they call on the path pod create → job queued on the head (`KubernetesCommandRunner`, `backend_utils`). Read-only research. Code anchors are master `9397155e0` (A1's merge doesn't touch these lines except `config.py` Service creation). Team B anchors are `skypilot-teamB@8915810e4`, Team C anchors `teamC@f7483484d`.

Tags: **[V]** measured in `bench/runs/*`, **[C]** read in code, **[H]** hypothesis or estimate.

## 0. Summary

- Almost the whole post-Ready window is in this scope: **pods Ready → job queued is 37.4 s mean** (31.3–49.5 over 12 runs) [V]. All of it is a single host-driven serial chain of **11 + k host→pod round trips** (k = number of Ray checks, 1–3). A SkyPilot `kubectl` process is alive for 30.9 of the 31.8 s (ANALYSIS §5.9).
- The teams cover the Ray wait (C), what each exec costs inside the pod (A2), and the post-launch execs (A3). **Nobody owns how many round trips there are, or the ordering and barriers between them.** That structure is what stays on the critical path after A2 and C land. It is also on Team B's critical path: B's head waits on `runtime_ready`, which the host sends only after the all-node `setup_runtime_on_cluster`.
- Top new findings, all in the ranked table in §2:
  1. the head submit chain is 5 serial round trips and could be 1;
  2. skylet start waits behind the Ray wait for no reason;
  3. all-node barriers between per-node stages cost 1.8–7.0 s in the diag runs;
  4. P2 is mostly waiting on the in-pod PyPI install (owned by no one);
  5. P1 is 3 serial round trips per pod and could be 1.
- **Structural:** push the static runtime (creds, `sky_ray.yml`, wheel, `setup_commands`' static work) into the pod spec. Then the host makes **zero round trips between Ready and submit**, and the host path stops scaling with N.

## 1. Measured map of the scope

### 1.1 Stage durations (s), from `provision.log`, `requests.db` and pod watch

Computed with `bench/summarize.py` over every run that has the data.
- **detect** = all pods Ready (watch) → `all pods are scheduled and running`
- **info** = → `Pods are up` (`get_cluster_info`: LIST + GET svc + `whoami` exec)
- **P1** = `internal_file_mounts`
- **P2** = `setup_runtime_on_cluster`
- **P3** = Ray wait
- **P4** = skylet start
- **P5–8** = "Cluster launched" → `sky.launch` request finished

| run | detect | info | P1 | P2 | P3 | P4 | P5–8 | Ready→queued | queued→last rank | headline |
|---|---|---|---|---|---|---|---|---|---|---|
| trial2 | 0.76 | 0.29 | 1.61 | 3.93 | 20.67 | 2.01 | 7.35 | 36.61 | 8.58 | 54.95 |
| trial3 | 1.02 | 0.28 | 1.52 | 3.96 | 20.76 | 1.98 | 7.28 | 36.78 | 8.51 | 54.09 |
| base4 | 0.93 | 0.28 | 2.29 | 4.36 | 16.89 | 2.30 | 7.53 | 34.59 | 9.35 | 52.54 |
| base5 | 0.25 | 0.31 | 2.09 | 5.13 | 18.11 | 2.11 | 8.17 | 36.17 | 9.60 | 55.00 |
| base6 | 0.84 | 0.34 | 2.73 | 6.01 | 25.10 | 2.89 | 11.54 | 49.46 | 16.02 | 74.96 |
| diag1 | 0.54 | 0.29 | 1.51 | 4.39 | 15.94 | 2.19 | 7.61 | 32.47 | 8.76 | 50.39 |
| diag2 | 0.86 | 0.20 | 1.16 | 8.97 | 10.47 | 2.11 | 7.53 | 31.30 | 9.90 | 53.22 |
| a1w0 | 0.73 | 0.38 | 1.72 | 5.36 | 23.79 | 2.53 | 8.36 | 42.87 | 11.02 | 68.28 |
| a1t1 | 1.04 | 0.28 | 1.32 | 5.49 | 16.93 | 2.30 | 7.64 | 35.00 | 9.01 | 50.62 |
| profile1 | 0.24 | 0.48 | 1.66 | 4.80 | 15.81 | 2.40 | 7.48 | 32.87 | 9.26 | 51.80 |
| profile2 | 0.18 | 0.30 | 1.54 | 4.57 | 18.03 | 2.63 | 8.22 | 35.47 | 10.54 | 56.26 |
| profile3 | 0.65 | 0.31 | 1.56 | 5.02 | 26.87 | 2.76 | 8.52 | 45.69 | 11.57 | 71.91 |
| **mean** | **0.67** | **0.31** | **1.73** | **5.17** | **19.11** | **2.35** | **8.10** | **37.44** | 10.18 | 57.84 |

Excluding P3 (Team C's), the host-serial steps are **18.3 s mean** of host round-trip time per launch.

### 1.2 The exec chain, diag1 / diag2 proctrace

Seconds after t0, with duration in parentheses. All are children of the one long-worker PID.

| # | Step | Target | diag1 | diag2 | Code |
|---|---|---|---|---|---|
| 1 | `whoami` (ssh user) | head | 9.60 (0.29) | 12.75 (0.29) | `kubernetes/instance.py:3302-3317` |
| 2 | `mkdir -p ~/.sky/.runtime_files` | ×4 ∥ | 9.81 (0.34–0.50) | 12.88 (0.36–0.44) | `instance_setup.py:915-927` |
| 3 | `echo ~` (home dir for the rsync target) | ×4 ∥ | 10.04–10.19 (0.42–0.67) | 13.15–13.17 (0.34–0.47) | `command_runner.py:432-457, 638-644` |
| 4 | rsync + `rsync_helper.sh` kubectl exec | ×4 ∥ | 10.40–10.64 (0.69–0.83) | 13.40–13.48 (0.37–0.65) | `command_runner.py:1958-2025` |
| 5 | `setup_commands` | ×4 ∥ | 11.32: head 2.73, workers 4.14–4.49 | 14.05: head 2.08, workers 3.59 / 7.59 / 9.07 | `instance_setup.py:496-555` |
| 6 | Ray check(s) | head | 15.70 (9.37), 25.92 (5.88) | 23.01 (10.64) | `provisioner.py:838-856` (C) |
| 7 | skylet start | head | 31.64 (2.34) | 33.49 (2.24) | `provisioner.py:915`, `instance_setup.py:809-900` |
| 8 | `set_autostop` codegen | head | 33.88 (2.69) | 35.63 (2.28) | `cloud_vm_ray_backend.py:6135-6144` |
| 9 | autodown annotations (K8s API, 1 LIST + 8 PATCH) | — | 36.57 → 36.71 | — | `cloud_vm_ray_backend.py:6150`; `kubernetes/utils.py:5326-5370` |
| 10 | `add_job` codegen | head | 36.69 (2.16) | 38.05 (2.42) | `cloud_vm_ray_backend.py:4548` |
| 11 | `echo ~` again | head | 38.70 (0.21) | 40.39 (0.22) | `command_runner.py:638-644` |
| 12 | driver rsync + helper (44,651 B) | head | 38.81 (0.31) | 40.52 (0.31) | `cloud_vm_ray_backend.py:4339, 4449-4452` |
| 13 | `queue` codegen | head | 38.98 (2.33) | 40.70 (2.35) | `cloud_vm_ray_backend.py:4458` |

- **Per launch:** 25 kubectl + 5 host rsync processes (5N + 8 + k). Each one also spawns a `/bin/sh` and a `python subprocess_daemon.py` (`log_lib.py:227-241` → `subprocess_utils.py:458-500`) [V counts, C daemon].
- **Caveat:** proctrace durations are start (`/proc` ticks + btime) → last scan (60 ms). They likely overstate trivial execs by up to ~0.1 s. Idle transport is 0.11–0.17 s (`runs/profile1/profile/transport.csv`).
- **Host CPU** is pinned at **8.0/8** cores during Ray start: a1t1 +20.8…+24.0 s, a1w0 +32.1…+36.4 s (`host_load.log`) [V]. Host-side process spawning competes with in-pod work on Kind.

## 2. Ranked findings

Ranked by the headline impact expected **after** A1/A2/C land. Savings are on the critical path unless noted.

| # | Finding | Code anchor | Measured cost | Tag | Covered? | Proposed fix | Risk | Owner |
|---|---|---|---|---|---|---|---|---|
| 1 | **Head submit chain is 5 serial round trips** (autostop exec → annotation PATCHes → add_job exec → `echo ~` exec → driver rsync+helper → queue exec). Two of them start Python, plus the queue exec. The driver is uploaded separately only because 44.6 KB is over the 32 KB *URL-encoded* inline limit. | `execution.py:685-745` (PRE_EXEC/EXEC); `cloud_vm_ray_backend.py:6135-6153, 4518-4580, 4306-4516` (inline check 4449, upload 4339, queue 4458) | P5–8 **8.10 s mean** (7.28–11.54, 12 runs). diag1: 2.69 + 0.12 + 2.16 + 0.21 + 0.31 + 2.33. gzip+base64 of the real `sky_job_1`: 9.3 KB → **13.2 KB URL-encoded inline, vs 83.9 KB raw** | [V] cost, [V] size, [H] saving | **None.** A2 makes each exec cheaper, not fewer. PLAN W4 Option C is unassigned | One exec: `set_autostop` + `add_job` + write driver + `queue` in a single codegen snippet. The driver goes inline as gz+b64, or on stdin of `kubectl exec -i`. The driver codegen gets a `@@JOB_ID@@` placeholder that the snippet fills from `add_job`'s return (on a fresh cluster the id is 1, which B already relies on). Annotations run on the host concurrently. **−4.5 to −5 s today, −1.2 to −2 s after A2** | Med. Crosses the PRE_EXEC/EXEC stages. Error attribution needs per-step exit markers. Gate it on "fresh launch with the wheel just installed", and keep the old path for `sky exec` and old runtimes (no `SKYLET_VERSION` bump needed when the wheel was just installed) | New (A4 "head chain") or A3 (owns the job methods in the backend) |
| 2 | **Skylet start is serialized after the Ray wait**, but needs only the wheel. Skylet doesn't import Ray. Autostop is a config row (`autostop_lib.py:207`). `queue` launches the driver directly (`job_lib.py:282-307`). | `provisioner.py:915` (after the loop at 838-856); `instance_setup.py:809-900` | P4 **2.35 s mean** (1.98–2.89) [V], all on the critical path today | [V] cost, [C] independence | **None.** C's head-only wait doesn't move it | Start `start_skylet_on_head_node` in a thread right after the head's P2. Join (and raise) before `post_provision_runtime_setup` returns, or before the first gRPC use. It hides under the Ray wait: diag1 head P2 done 14.05, head GCS 19.2, so the 2.3 s skylet start fits. **−2.0 to −2.9 s today, ≥ −1 s after C** | Low–med. Failure propagation from a thread; `_auto_retry` semantics | C (owns the provisioner wait) or A4 |
| 3 | **All-node barriers between per-node stages.** `internal_file_mounts` → barrier → `setup_runtime_on_cluster` → barrier → head-only steps. The head waits for the slowest worker's P2, and P2's variance is the in-pod PyPI install. **On Team B's path this barrier is the submit gate:** `runtime_ready` is sent after all-node P2 (`skypilot-teamB:sky/provision/provisioner.py:776-787`) | `provisioner.py:761-769`; `instance_setup.py:429-460` (each stage is a separate fan-out and join) | Head P2 done vs last worker: **diag1 14.05 vs 15.81 (1.8 s); diag2 16.13 vs 23.12 (7.0 s)**. Worker P2-end spread from log mtimes: 0.36–5.4 s across 12 runs | [V] | **None.** B keeps the all-node gate on purpose; its log calls it "cheap" because `submit.py`'s cold `import sky` overlaps it. After A2 that overlap disappears | Per-node pipelines. Each pod runs its own P1→P2 (fused, #5) as soon as it is Running. The head chain proceeds after the **head's** P2. Each worker's rank is gated per node by a marker (`~/.sky/.runtime_ready`) checked by C's stdlib per-node task, or by B's runner before `run:`. With C's slim per-node task, workers don't need the wheel before their rank starts, only creds, ulimit, ssh config and fuse. **−1.8 to −7 s on B's gate, and on C's head-only path** | Med. Correctness: a worker's setup must still precede its own rank. Needs a per-node gate with a timeout and failure surfaced as a setup failure | B (fast_start gate) + A4/C (generic path) |
| 4 | **P2 is mostly waiting on the in-pod PyPI `skypilot` install marker**, not doing work. Worker logs: "Ray and skypilot dependencies installation completed in **1–7 s**", wheel 0–1 s, configs 0 s. The wait loop polls at 0.5 s after an unconditional 0.5 s sleep | template `kubernetes-ray.yml.j2:1460` (PyPI install), `:2077-2084` (marker waits) | P2 **5.17 s mean** (3.93–8.97); diag2's 9 s was one worker's 7 s PyPI step | [V] | **None.** PLAN W5 is unassigned. C's guarded pins shorten STEP 2 but keep the PyPI install | W5: install deps only (no-op on the default image), or skip it when the image has the deps. Poll markers at 0.05 s or use `inotifywait`. Shrinks P2 to ~1 s and removes most of #3's variance | Low–med (dependency drift; see PLAN W5) | C (owns template STEP 2) |
| 5 | **P1 is 3 serial round trips per pod** (`mkdir`, `echo ~`, rsync whose helper is itself a kubectl exec). The code already carries a TODO for the mkdir trick (`instance_setup.py:911`). All runtime files are already packed into one dir (`backend_utils.py:352-427`) | `instance_setup.py:904-963`; `command_runner.py:570-700, 1958-2025`; `utils/kubernetes/rsync_helper.sh` | P1 **1.73 s mean** (1.16–2.73) [V]. diag1 per pod: 0.34–0.50 + 0.42–0.67 + 0.69–0.83 | [V] | **None** | One exec per pod: `tar -cz <runtime dir> \| kubectl exec -i … -- sh -c 'mkdir -p ~/.sky/.runtime_files && tar -xz -C ~/.sky/.runtime_files && <setup_commands>'`. That **fuses P1+P2** (the move-into-place commands already lead `setup_commands`, `backend_utils.py:404-411`) and folds `whoami` + home-dir into its output. **−1.0 to −1.5 s** | Low–med. Needs `tar` in the image (rsync is already required). Keep rsync as the fallback | A4 |
| 6 | **Home-dir lookup is an uncached exec** before every rsync to `~/…`. Runners are rebuilt per call (`kubernetes/instance.py:4817-4855`), so nothing persists. Paid N× in P1 and again before the driver upload | `command_runner.py:432-457, 531-560, 638-644` | 0.34–0.67 s per P1 [V]; 0.21–0.22 s before the driver [V] | [V] | **None** | Cache per (context, namespace, pod), in-process or in `InstanceInfo`, or take it from the `whoami` exec. Subsumed by #1/#5 if they land; a 10-line stopgap otherwise. **−0.4 to −0.8 s** | Low | A4 |
| 7 | **Pod-Running detection is a 1 s LIST poll, and it waits for all pods.** Each poll also LISTs events for every still-pending pod (`instance.py:1662, 1680` → `_get_pod_events`), so API load is O(N) per second | `kubernetes/instance.py:1508-1886` (sleep 1886); schedule loop 1086-1451 (sleep 1451) | Lag Ready(watch) → "scheduled and running": **0.67 s mean** (0.18–1.04, 12 runs). Ready spread at N=4: 1.2–2.0 s. Proto: `Always` adds ~0.5 s × (N−1) | [V] | **None** (PLAN appendix C2) | Watch (the helper exists at `adaptors/kubernetes.py:426`) with poll fallback, or a 0.2 s poll for the first 30 s. Event LISTs at most every ~5 s. Combined with #3, start each pod's bootstrap on its own Running event (streaming). **−0.5 s, more at larger N** | Low (watch reconnects; keep the poll as fallback) | A1 (already in this file) or A4 |
| 8 | **The gRPC skylet path is off for the launch chain.** Under `SKYPILOT_ENABLE_GRPC`, autostop / add_job / queue become RPCs. The driver goes inline in `QueueJob` (100 KB limit, `backend_utils.py:168`, `command_runner.py:61`), so there's no home-dir or rsync step | `env_options.py:29`; `cloud_vm_ray_backend.py:4379-4445, 4523-4539, 6109-6133` | Not measured. Est. P5–8 8.1 → ~3 s (skylet start + tunnel open + 3 RPCs) | [C] path, [H] saving | **Partly.** A3 uses gRPC for status only; nobody owns it for the launch chain | **No-code E2E experiment:** set the env var in the client (it is forwarded to the controller env, ANALYSIS §7.4). Count execs in proctrace. If it holds up, it is an alternative to #1 | Med. The flag is "internal / testing"; one `kubectl port-forward` per cluster (~47 MB) | Architect (E2E) |
| 9 | **`whoami` exec in `get_cluster_info`**, serial before P1. The K8s runner never uses `ssh_user` (`command_runner.py:1672` `del kwargs`); only the SSH config and the handle need it | `kubernetes/instance.py:3298-3320` | "info" **0.31 s mean** (0.20–0.48); exec 0.29 s | [V] | **None** | Fold it into #5's single exec, or run it concurrently with P1. **−0.2 to −0.3 s** | Low | A4 |
| 10 | **Autodown annotations:** 1 LIST + **2N serial PATCHes**, after the autostop exec. The values are known at launch (controller: idle 10, down) | `kubernetes/utils.py:5299-5370`; `cloud_vm_ray_backend.py:6150` | Annotations appear at 36.68 / 36.71 (diag1 pod watch); window 0.12 s at N=4. [H] ~1–2 s at N=64 | [V] N=4, [H] scaling | **None** | Put them in the pod spec at creation for jobs-controller launches, or one merge-patch per pod in parallel and concurrent with #1 | Low (keep the patch path for `sky autostop`) | A4 or B |
| 11 | **Per-exec host overhead:** kubectl + `/bin/sh` + a Python `subprocess_daemon.py` per call. On exec-auth clusters (EKS `aws eks get-token`, other plugins), every kubectl process re-runs the auth plugin, because client-go caches credentials only in-process | `log_lib.py:227-241`; `subprocess_utils.py:458-500`; `command_runner.py:1766-1890` | 30 processes per 4-node launch, plus ~30 daemons [V/C]. Host 8/8 busy during Ray start [V]. EKS per-exec auth: **[H] 0.3–1.5 s per round trip**, ×11+k serial | [V] Kind, [H] EKS | **None** | Fewer round trips (#1, #5) is the main lever. Medium-term: exec through the in-process Python client (`kubernetes.stream`, cached token, no process) for short commands; `use_kill_pg` instead of the daemon | Med (stream API quirks: stdin, large output) | Later / A4 |
| 12 | **Setup-bearing managed jobs:** `_setup(detach_setup=True)` rsyncs a per-node setup script to every node before PRE_EXEC (absolute target, so 1 rsync + helper per node) | `cloud_vm_ray_backend.py:4175-4206` | Not exercised by the bench (no `setup:`). [H] ~0.3–0.5 s round trip on the path | [C] | **B only** (fast_start ships setup in the manifest) | Ship it inline in the driver, or in #5's tarball | Low | A4 |

### Already covered (do not duplicate)

| Item | Owner | Note |
|---|---|---|
| Ray-ready check: 5.9–10.6 s per check, quantization, 1 s loop sleep (`provisioner.py:838-856`, `instance_setup.py:318-337`) | C (payload done in `ba3732f5e`; head-only wait pending) | **[V] flag for C:** the in-pod head start also ends in `RAY_HEAD_WAIT_INITIALIZED_COMMAND` (`instance_setup.py:343-348, 638`). That is one more `ray status` (2.4 s warm / 6.8 s cold CPU) on the head, concurrent with the provisioner's checks (seen in diag1 head `runtime-setup.log`). It gates nothing on K8s [C]. Not mentioned in C's commit |
| `import sky` per exec | A2 | Shrinks #1, #2 and #8's costs but not their count |
| K8s Service creation serial | A1 | 0.24 s → parallel |
| Post-launch verify execs / RUNNING lag | A3 | — |
| Job baked into the pod spec | B | B still pays `info` + P1 + all-node P2 + `runtime_ready` exec ≈ **8.2 s mean after Ready** before the head may submit (detect 0.67 + 0.31 + 1.73 + 5.17 + ~0.3) [V parts]. #3, #4, #5, #7, #9 and §3 S1 apply directly. Minor: B's host gang poll (`fast_start.py:373-405`, 1 s + exec) delays `sky.launch` return, and so A3's RUNNING fast path, by up to ~1.2 s |

## 3. Structural opportunities

**S1. Zero host round trips between pod Ready and job submit: push the runtime into the pod spec.** This generalizes B's manifest idea to the runtime, for every K8s launch, not only opt-in managed jobs.
- **What moves into the pod spec:**
  - **Secret:** cloud credentials and the cluster SSH key.
  - **ConfigMap:** `sky_ray.yml`.
  - **Wheel:** content-addressed **ConfigMap shards** (`skywheel-<hash>-<k>`; 3.2 MB → 4 shards of ≤1 MiB `binaryData`), created once per (namespace, wheel hash) and mounted `optional: true`. The hash is known before the template renders (`push_bootstrap.md` §2).
  - **Container script:** the static `setup_commands` work (ulimit, ssh config, fuse). It installs the wheel with `--no-deps` once the shards are present, writes `current_sky_wheel_hash`, then starts the skylet.
- **What the host does after Ready:** watch the pods, then one submit call (#1 or #8), or nothing on B's path.
- **What leaves the host path:** info + P1 + P2 + P4. That is **9.6 s mean of serial host time** (0.31 + 1.73 + 5.17 + 2.35) [V parts]. It runs in-pod instead, overlapped with Ray start and pod start.
- **N-scaling:** the per-node host fan-out (5N kubectl/rsync processes, thread-pool waves past `get_parallel_threads('kubernetes')` = max(4, cpu−1)×4 = 28 on this host) disappears.
- **Headline [H]:** C path −5 to −8 s; B path −3 to −7 s (B's `WAIT_WHEEL`/`RUNTIME` gates become in-pod events).
- **Risks:** etcd object size (1 MiB per object), RBAC for ConfigMap create (B already creates one), garbage-collecting old wheel shards, credentials persisted in etcd (B already accepted this for `fast_start_secrets`). Keep today's path as the fallback when shards can't be created.

**S2. Per-node streaming pipeline instead of stage-synchronous fan-outs.**
- Today the host runs "all pods Running → all P1 → all P2 → head-only".
- Instead, a pod watch starts each pod's single bootstrap exec (#5) on that pod's Running event. The head chain continues when the head is ready. The **gang is the only cross-node barrier**: the placement group, or B's rendezvous.
- **Removes:** detect lag (0.67 s), barrier slack (1.8–7.0 s in the diag runs), and the ~0.5 s × (N−1) Ready stagger under `imagePullPolicy: Always` (proto N=8) from the head's path.
- This is the host-driven version of S1. It's worth doing if S1's wheel delivery is rejected.

**S3. The head submit is one call.** #1 (one folded exec) or #8 (gRPC).
- **Combined with #2:** the round-trip depth after Ready goes from **11 + k (13 in diag1) to 2 + k** (fused bootstrap, Ray check, submit). With S1 it is 1 + k.

**Expected headline impact of S2 + S3 + #2 + #4 [H]**, a critical-path model built from the §1 means:

| Path | Host Ready→submit today | After | Then gated by |
|---|---|---|---|
| C (head-only Ray wait), no A2 | ≈ 18.8 s (0.67 + 0.31 + 1.73 + 5.17 + ~0.5 check + 2.35 + 8.10) | ≈ 6 s (detect ~0.2, fused bootstrap on head ~1–2.5, check ~0.5, skylet hidden, one submit ~2.6) | worker Ray join; ≈ Ready+18 s in diag1 before C's levers |
| C + A2 | ≈ 13 s | ≈ 4 s | same |
| B (no workdir) | ≈ 8.2 s | ≈ 1–3 s (S1: ≈ 0) | in-pod head Ray up + `submit.py` |

So the in-scope work removes **~8–13 s of host-serial time** after A2 and C. The headline gain is that amount, capped by when the in-pod Ray gang forms. That is why it pairs with C's levers and B's design rather than replacing them.

## 4. Checked, not worth it (don't re-investigate)

- **Exec transport speed on Kind.** `kubectl exec true` costs 0.11–0.17 s idle; login and `-i` shells add <30 ms [V `profile1/transport.csv`]. Fewer round trips is the lever, not a faster transport, except on exec-auth clusters (#11).
- **K8s Python client reuse.** `core_api()` and friends are `lru_cache(scope='request')` (`adaptors/kubernetes.py:352-430`); there is no per-call kubeconfig parse.
- **`_bulk_provision` post-run wait/backoff.** Already skipped for K8s (`provisioner.py:136-139`); `wait_instances` is a no-op (`kubernetes/instance.py:2814`).
- **`pre_init` exec chain** (`kubernetes/instance.py:1890`): no caller on this path [C]. No exec runs between pod create and `whoami` [V].
- **`_wait_for_pods_to_schedule`.** Pods were already bound at the first LIST (diag1 6.297 → 6.365) [V]. Its 1 s sleep matters only with slow schedulers or autoscalers.
- **Pod create.** 4 creates land within 20–70 ms, in parallel [V].
- **SA / role / rolebinding checks** in `config.py`: ~0.04 s (diag1 6.161 → 6.197) [V].
- **`_update_after_cluster_provisioned` on a fresh cluster.** No exec: `prev_cluster_status` is None, and the `update_status` exec is skipped [V proctrace]. DB and SSH-config writes take ms.
- **Cluster status lock.** Held through `_locked_provision`, uncontended, no waits seen. `check_rsync_installed` and `check_owner_identity` take ms [C].
- **`get_command_runners` / `_execute_task_n_nodes`.** No K8s API calls; IPs are cached in the handle [C].
- **`sync_workdir` / `sync_file_mounts` for the bench task.** No execs, only the "Syncing files." message [V].
- **P1 → P2 cross-node barrier at N=4.** P1 ends within 0.16 s across pods (diag1) [V]. It matters only past ~28 nodes (thread-pool waves) [C].
- **`rsync -z` on the already-compressed 3.2 MB wheel; local `cp -r` of runtime files pre-provision** (`backend_utils.py:418-427`, <0.3 s per the code comment). Tens of ms each [H].
- **Autodown annotations at N=4** (0.12 s). Only #10's scaling matters.
- **gRPC tunnel open poll** (0.1 s, `backend_utils.py:4791-4796`). Relevant only if #8 is adopted.
- **Pre-provision window** (`sky.launch` start → `provisioner.py:278`, 1.0–1.9 s). Not investigated here: it is A1's row 10 and needs py-spy or timestamps.

## 5. Open measurements (cheap, would firm up the ranking)

1. **Diag E2E with `SKYPILOT_ENABLE_GRPC=1`** (#8): exec count and P5–8 with gRPC on.
2. **A2 after-numbers for the in-pod imports** of `job_lib`, `autostop_lib` and `attempt_skylet`. These set the residual value of #1 and #2 after A2.
3. **N=8 run:** P1/P2 fan-out, annotation PATCHes (#10), pending-pod event LISTs (#7).
4. **One `time kubectl exec … true` on an EKS or GKE context** to size #11 off Kind.
5. **Head P2 end time in non-diag runs.** Only the diag runs have it. Worker mtimes alone underestimate #3.
