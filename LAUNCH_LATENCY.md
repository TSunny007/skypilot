# Faster multi-node launches on Kubernetes

Design doc · Tarun Sunkaraneni · 2026-10-15 · code: [`ll/consolidated`](https://github.com/TSunny007/skypilot/tree/ll/consolidated) + [PRs 1–8](https://github.com/TSunny007/skypilot/pulls) · data: [`bench/notes.md`](https://github.com/TSunny007/skypilot/blob/ll/consolidated/bench/notes.md) · long form: [`bench/ANALYSIS.md`](https://github.com/TSunny007/skypilot/blob/ll/consolidated/bench/ANALYSIS.md), [`bench/DESIGN.md`](https://github.com/TSunny007/skypilot/blob/ll/consolidated/bench/DESIGN.md) (on `ll/consolidated`)

## TL;DR

- **Metric:** seconds from `sky jobs launch` until all N=4 nodes' `run:` has started (local Kind, consolidation-mode API server).
- **Result:** 54.15 s → **26.65 s (−51%)** with opt-in self-starting pods (`kubernetes.fast_start: true`). That's 2 valid trials (26.40, 26.90 s) against a 4-trial baseline (52.5–55.0 s).
- **The win came from removing the host-driven bootstrap, not from speeding up its steps.** Making pods Ready 3 s sooner on the old path changed nothing (55.3 s).
- **Status catches up too.** RUNNING appears 0.2–1.5 s after the last rank (was +6.7 s), and SUCCEEDED 9–14 s after (was +18 s).

## Diagnosis

- **Kubernetes is not the bottleneck.** Pods are Ready at 9.1 s; 83% of the time comes after that.
- **The host assembles the cluster from outside.** After Ready, the API-server host runs one serial chain of 11+k `kubectl exec` round trips: file mounts, runtime setup, k Ray checks, skylet, autostop, add_job, driver upload and queue.
  - Each exec starts Python in the pod and imports `sky` (~2 s).
  - Each Ray-ready check costs 6–10 s.
- **The image is unfinished.** Every pod reinstalls SkyPilot from PyPI and downgrades setuptools and click so Ray can start.
- **The control plane polls:** a 10 s controller claim poll, a scheduler subprocess (~1.9 s cold import), and monitor ticks that run every 25–28 s.

## Design: pods start themselves

- **Push, don't drive.** The job (the generated Ray driver, setup and metadata) is rendered into a ConfigMap, with task secrets in a Secret, mounted into every pod.
  - Each pod starts Ray at container start and finds its peers through the per-pod headless Services.
  - The head registers and queues the job through the existing `job_lib` API. The Ray placement group is the only gang barrier.
  - The host only watches a state file on the head.
- **Existing seams.** It is built on `ProvisionRuntimeMetadata` and the provisioner template overrides from #9510. The backend skips steps the pods did, and the job queue, logs, status and teardown are unchanged.
- **Opt-in and scoped:** managed jobs on Kubernetes, non-pool, fresh clusters. With the flag off, the pod template is byte-identical.
- **Default-path PRs (1–5)** help every launch:
  - control plane: event-driven claim, in-process submit, parallel Service creation;
  - a lazy `import sky`;
  - Ray levers: no dashboard head, no stale `ray stop`, no `import sky` on workers, no `check_ip` barrier, a GCS node count instead of `ray status`;
  - status fast paths, and the submit chain in one exec;
  - the skylet started under the Ray wait, and a dependencies-only pod install.

## Results (s after `sky jobs launch`, N=4)

| Stage | Baseline | Control plane (PR 1) | Self-starting pods |
|---|---|---|---|
| All pods Ready | 9.1 | 6.0 | 6.4 |
| Host Ray-ready checks | 1.5 | 2 | 0 |
| Ready → job queued | 36.0 | 40.2 | 19.2 |
| Queued → last rank | 9.0 | 9.1 | 0.7 |
| **All ranks started** | **54.15** | 55.3 | **26.65** |

**gRPC (no code change):** on the default path it cuts the submit chain from 8.0 s to 2.6 s and makes status 9–15 s faster. It leaks one `kubectl port-forward` per job.

**Where the remaining 26.65 s goes** (sf2, measured; details in [`bench/design/wheel_and_submit.md`](https://github.com/TSunny007/skypilot/blob/ll/consolidated/bench/design/wheel_and_submit.md)). The critical path is the head pod's own environment setup, then its Ray start:

| Time after launch | Step on the critical path | Duration |
|---|---|---|
| 0 → 3.9 s | Before any pod exists: CLI 1.7 s, controller 0.9 s, optimizer 1.2 s | 3.9 s |
| 3.9 → 5.4 s | Head container start (image pull policy `Always` adds ~0.9 s) | 1.5 s |
| 5.4 → ~13.0 s | In-pod environment setup: PyPI `skypilot` 2.8 s, Ray patches 1.8–2.3 s, setuptools/click pin 0.7 s | ~7.6 s |
| ~13.0 → 22.5 s | `ray start --head` under load | ~9.5 s |
| 22.5 → 22.65 s | Submit: cold `import job_lib` 0.3 s, add_job + queue | 0.15 s |
| 22.65 → 26.9 s | Worker join, driver start, rank dispatch | ~4.2 s |

- **Off the critical path:** the host's wheel copy and install (6.3 → 14.0 s) finishes ~8.5 s before the head's Ray is up. Most of it is waiting on that same in-pod setup; the install itself takes ~0.5 s.
- **DNS:** the 6 s peer DNS wait seen once did not reproduce (28 pods, ≤1.45 s). It fits one 5 s resolver timeout.

**Why the in-pod environment work came after the structural work.** These costs depend on the workload's environment:
- whether the image already has SkyPilot's dependencies, Ray's pins and patches (custom images often don't);
- network access to PyPI;
- image pull policy and cache state.

Our single 8-core Kind node with a warm cache cannot produce numbers that generalize for them. So we attacked the costs present in every environment first: the host-driven serial bootstrap, Ray on the critical path, and control-plane polling. The environment work is still **low-hanging fruit**: the fixes are small and known (next section), and together worth an estimated 5–9 s.

## Limitations

- **Small n, one environment:**
  - 2 fast-start trials;
  - one 8-core Kind node;
  - N=4 (N=8 only in the prototype);
  - CPU pods, warm image cache.
- **Contention:** host CPU load from parallel development invalidated several batches until we froze other processes during trials. The gRPC headline is unusable for this reason.
- **Fast start v1 limits:**
  - assumes a fresh cluster (job id 1);
  - rank order is ordinal, which differs from today's only at N≥11;
  - no hostNetwork, TPUs, pools, HA or pod re-creation;
  - task secrets sit in a Secret until teardown;
  - NodeLocal DNSCache is untested.
- **Unmeasured end to end:** the GCS node count, the post-Ready chain, and the one-exec submit and monitor changes. They are in `ll/consolidated` with unit or raw-pod tests only. Unit-test triage of the combined branch is unfinished.
- **A known regression:** Team D's deps-only pod install (PR #4), as committed, makes the later wheel install resolve 107 dependencies online. That adds 1.4–3.7 s on the fast-start path [measured in raw pods]. It needs `--no-deps` or `--offline` before merge; with that, it saves ~2 s.

## Not done (by expected value)

1. **Shrink the head's environment setup.** Low-hanging fruit, but environment-dependent (see above):
   - fix the deps-only install with `--no-deps`: −2 s;
   - apply the Ray patches without importing Ray: −1.6 to −2.1 s;
   - bake Ray bytecode, the pins and the patches into the default image: −3.7 to −4 s, plus −1.7 s on `ray start` [measured in raw pods];
   - use `IfNotPresent` for the date-tagged default image: −0.9 s.
2. **Wheel delivery:** it becomes co-critical once item 1 lands. Ship it as ConfigMap shards in the pod spec, or at least as one exec per pod with the head not waiting for the other pods.
3. **Peer DNS:** a trailing-dot FQDN plus a bounded lookup removes the 5 s resolver-timeout tail.
4. **Event-driven status:** the head writes job state as a pod annotation, and one watch per controller picks it up. Expected terminal status in 1–3 s, with no per-job exec polling.
5. **gRPC:** fix the port-forward leak, then enable it (−5 s on the default path).
6. **Not yet:** head-only Ray wait (blocked: the status refresh would mark the cluster INIT), per-node pipelining, one tar exec per pod, a pod watch instead of the 1 s poll.
7. **Tabled:** pre-resolved launch plan, speculative pods, warm pools, a burst benchmark, a Ray-free rank launcher.

## How far it can go

- **The prototype gives the floor.** It used raw pods from the same image on the same cluster:
  - without Ray, ranks started 1.0 s after pod creation (1.3 s at N=8);
  - with a Ray gang started at container start, after 12–15 s.
- **Projection [estimate]:**
  - fast start + the code-only fixes in item 1 (a day): **~21.5–22.5 s**;
  - + the image work and wheel delivery (a week): **~15–18 s** end to end with Ray;
  - with a Ray-free rank launcher: ~6–8 s;
  - with a ~2 s control plane on top: ~4–5 s.

## If SkyPilot only supported Kubernetes

The launch path becomes "compile the task into Kubernetes objects, apply, watch":

- **Launch = one apply** of an indexed Job/JobSet, or pods plus a headless Service:
  - the runtime is baked into the image;
  - the task arrives as a ConfigMap/Secret;
  - rank comes from the completion index, and peers through stable DNS;
  - gang scheduling uses Kueue or coscheduling.

  There's no SSH emulation, no `ray up`, no host exec chain and no wheel shipping.
- **Status = watch.** Pod and Job conditions plus a job-state annotation feed a controller informer. Status lands within ~1 s, with no per-job execs.
- **The lifecycle comes from Kubernetes primitives:**
  - teardown deletes the owner (ownerReferences GC);
  - autodown is `ttlSecondsAfterFinished`;
  - deadlines are `activeDeadlineSeconds`;
  - retries use `podFailurePolicy` plus a controller relaunch;
  - logs come from the API.
- **SkyPilot still owns** choosing contexts and clusters with failover, storage, managed-job recovery, and the CLI/SDK.
- **Expected launch:** admission + scheduling + container start, about **3–6 s**.

Fast start is that design inside today's VM-shaped abstraction. Pods already start themselves and the host only watches; the remaining host steps (wheel install, skylet start) vanish once the runtime ships in the image.

## Next steps

1. Merge PRs 1–5 after test triage. Soak fast start behind the flag, then make it the default for managed jobs on Kubernetes.
2. Do item 1 above: a day of code-only fixes (~4–5 s), then the image work and wheel delivery (~4–6 s more); then event-driven status.
3. Re-benchmark on a real multi-node cluster (cold pulls, real network) at N=8, 16 and 64.
