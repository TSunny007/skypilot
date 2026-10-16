# Faster multi-node launches on Kubernetes

Design doc · Tarun Sunkaraneni · 2026-10-15
Code: branch `ll/consolidated` (everything) and PRs 1–8 on this fork · Data: `bench/notes.md` · Deeper write-ups: `bench/ANALYSIS.md`, `bench/DESIGN.md`

## TL;DR

- **Metric:** seconds from `sky jobs launch` until all N=4 nodes' `run:` has started, on a local Kind cluster with the API server in consolidation mode.
- **Result:** 54.15 s → **26.65 s (−51%)** with opt-in self-starting pods (`kubernetes.fast_start: true`). That's 2 valid trials (26.40, 26.90 s) against a 4-trial baseline (52.5–55.0 s).
- **The win came from removing the host-driven bootstrap, not from speeding its steps up.** Making pods Ready 3 s sooner on the old path changed nothing (55.3 s).
- **Status catches up too:** RUNNING now appears 0.2–1.5 s after the last rank (was +6.7 s), and SUCCEEDED 9–14 s after (was +18 s).

## Diagnosis

- **Kubernetes is not the bottleneck.** Pods are Ready 9.1 s after launch; 83% of the 54 s comes after that.
- **After Ready, the host drives the cluster into existence from outside.** The API-server host runs one serial chain of 11+k `kubectl exec` round trips: file mounts, runtime setup, k Ray-ready checks, skylet start, autostop, add_job, driver upload and queue.
  - Each exec starts Python in the pod and imports `sky` (~2 s).
  - Each Ray-ready check costs 6–10 s, and their count is quantized.
- **The default image is unfinished.** Every pod reinstalls SkyPilot from PyPI and downgrades setuptools and click so that Ray can start.
- **The control plane polls:**
  - a 10 s controller claim poll;
  - a scheduler subprocess (~1.9 s, mostly a cold `import sky`);
  - "15 s" monitor ticks that actually run every 25–28 s.

## Design: pods start themselves

- **Push, don't drive.** At launch, the job (the generated Ray driver, setup and metadata) is rendered into a ConfigMap, and task secrets into a Secret. Both are mounted into every pod.
  - Each pod starts Ray at container start.
  - Pods find their peers through the per-pod headless Services.
  - The head registers and queues the job through the existing `job_lib` API.
  - The Ray placement group is the only gang barrier.
  - The host only watches a state file on the head.
- **It builds on existing seams.** It uses `ProvisionRuntimeMetadata` and the provisioner template overrides from #9510. The backend skips the steps the pods already did, and job queue, logs, status and teardown work unchanged.
- **Opt-in and scoped:** managed jobs on Kubernetes, non-pool, fresh clusters. With the flag off, the pod template is byte-identical.
- **Default-path changes that help every launch (PRs 1–5):**
  - an event-driven controller claim, in-process job submit, and parallel Service creation;
  - a lazy `import sky`, so on-cluster code no longer loads the server stack;
  - Ray levers: no dashboard head, no stale `ray stop`, no `import sky` on Ray workers, no `check_ip` barrier, and a GCS node count instead of `ray status`;
  - status fast paths: RUNNING without 4 verification execs, the end time folded into the status read, and the submit chain in one exec;
  - a post-Ready chain with the skylet started under the Ray wait and a dependencies-only pod install.

## Results (s after `sky jobs launch`, N=4)

| Stage | Baseline | Control plane (PR 1) | Self-starting pods |
|---|---|---|---|
| All pods Ready | 9.1 | 6.0 | 6.4 |
| Host Ray-ready checks | 1.5 | 2 | 0 |
| Ready → job queued | 36.0 | 40.2 | 19.2 |
| Job queued → last rank | 9.0 | 9.1 | 0.7 |
| **All ranks started** | **54.15** | 55.3 | **26.65** |
| Rank spread | ~3.5 | ~3.5 | 1.0–1.5 |

- **gRPC experiment** (default path, no code change): the submit chain drops from 8.0 s to 2.6 s, and status lags arrive 9–15 s sooner. But it leaks one `kubectl port-forward` per job.
- **Where the remaining 26.65 s goes:**
  - 3.9 s before any pod exists (CLI 1.7, controller 0.9, optimizer 1.2);
  - 2.4 s of pod start;
  - 7.7 s while the host copies and installs the wheel into every pod, behind an all-node barrier;
  - 8.6 s while the head waits for Ray, then runs a cold `import sky` on the fresh wheel (no bytecode), add_job and queue;
  - ~4 s for Ray to dispatch the ranks.
- **Hidden for now:** a 6 s peer DNS rendezvous runs under the wheel install, so it isn't yet on the critical path.

## Limitations

- **Small n and one environment:**
  - 2 fast-start trials, 3–4 for the others;
  - one 8-core Kind node;
  - N=4 only (N=8 only in the raw-pod prototype);
  - CPU pods and a warm image cache.
- **Host contention:** CPU load from parallel development work invalidated several batches until other processes were frozen during trials. The gRPC headline is unusable for this reason.
- **Fast start v1 limits:**
  - it assumes a fresh cluster (job id 1);
  - rank order is ordinal, which differs from today's order only at N≥11;
  - no hostNetwork, TPUs, pools, HA or pod re-creation;
  - task secrets sit in a Kubernetes Secret until teardown;
  - behaviour with NodeLocal DNSCache is untested.
- **Not measured end to end:** the GCS node count, the post-Ready chain commits, and the one-exec submit plus monitor changes. They are in `ll/consolidated` and covered only by unit or raw-pod tests.
- **Unit tests:** triage of the combined branch isn't finished. Some failures are pre-existing on master or environment-only (missing cloud SDKs).

## Not done (by expected value)

1. **Bytecode:** compile the wheel at install, or ship it prebuilt in the image: −4–6 s [estimate].
2. **Wheel delivery:** put the wheel in the pod spec (content-addressed ConfigMap shards) or in the image. That removes the 7.7 s host install and the all-node barrier.
3. **Peer DNS:** set `publishNotReadyAddresses` on the headless Services [hypothesis]. The prototype resolved peers in 0.1 s.
4. **Event-driven status:** the head pod writes job state as a pod annotation, and one K8s watch per controller picks it up. Expected: SUCCEEDED/FAILED in 1–3 s and preemption noticed in ~1 s, with no per-job exec polling.
5. **gRPC:** fix the port-forward leak and enable it, worth −5 s on the default path.
6. **Smaller items:**
   - a head-only Ray wait (blocked: the status refresh would mark the cluster INIT and relaunch it);
   - per-node pipelining instead of all-node barriers;
   - one tar exec per pod for file mounts;
   - a pod watch instead of the 1 s poll.
7. **Tabled:** a pre-resolved launch plan, speculative pod creation, warm pools, a burst benchmark (16 LONG workers against 88 launch slots), and a Ray-free rank launcher.

## How far it can go

- **The prototype gives the floor.** It used raw pods from the same image on the same cluster.
  - Without Ray, all ranks started 1.0 s after pod creation (1.3 s at N=8).
  - With a Ray gang started at container start, they started after 12–15 s.
- **Projection [estimate]:**
  - fast start + bytecode + wheel in the image + the DNS fix: ~13–16 s end to end with Ray;
  - with a Ray-free rank launcher: ~6–8 s (3.9 s control plane + 2.4 s pod start + ~1 s);
  - trimming the control plane to ~2 s as well: ~4–5 s.

## If SkyPilot only supported Kubernetes

The launch path becomes "compile the task into Kubernetes objects, apply them, watch":

- **Launch is one apply.** An indexed Job/JobSet (or pods plus a headless Service) with:
  - the SkyPilot runtime baked into the image;
  - the task as a ConfigMap/Secret;
  - rank from the completion index, and peers through stable DNS (hostname + subdomain);
  - gang scheduling through Kueue or coscheduling.

  There's no SSH emulation, no `ray up`, no host exec chain and no wheel shipping.
- **Status is a watch, not a poll.** Pod and Job conditions, plus a job-state annotation, feed a controller informer. RUNNING, SUCCEEDED and FAILED land within ~1 s, with no per-job exec traffic.
- **The lifecycle comes from Kubernetes primitives:**
  - teardown deletes the owner (ownerReferences garbage collection);
  - autodown is `ttlSecondsAfterFinished`;
  - deadlines are `activeDeadlineSeconds`;
  - retries use `podFailurePolicy` plus a controller relaunch;
  - logs come from the K8s API.
- **SkyPilot still owns** choosing among contexts and clusters with failover, storage and file mounts, managed-job recovery semantics, and the CLI/SDK.
- **Expected launch:** admission + scheduling + container start, about 3–6 s.
- **Fast start is a step toward this inside today's VM-shaped abstraction.** Pods already start themselves and the host already only watches. What is left on the host (wheel install, skylet start) disappears once the runtime ships in the image.

## Recommended next steps

- Merge the default-path PRs (1–5) after test triage. Keep fast start behind the flag and soak it, then make it the default for managed jobs on Kubernetes.
- Do items 1–3 (cheap, an estimated 10–14 s), then event-driven status.
- Re-benchmark on a real multi-node cluster (cold image pulls, real network) at N=8, 16 and 64.
