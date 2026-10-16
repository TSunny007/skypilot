# Launch-latency work trial: start here

This fork holds a work trial on SkyPilot's multi-node managed-job launch latency on Kubernetes.

**The result.** A 4-node managed job on a local Kind cluster used to take **54.15 s** from `sky jobs launch` until every rank's `run:` had started (mean of 4 warm trials). On [`ll/benchmarked-stack`](https://github.com/TSunny007/skypilot/tree/ll/benchmarked-stack) with the opt-in `kubernetes.fast_start: true`, it takes **26.65 s**: 26.40 and 26.90 s over 2 trials, −51%.
- **The main change is structural.** A managed-job pod now carries its job and starts it itself. Before, the API server host waited for Ray and then pushed the job through a serial chain of `kubectl exec` calls.
- **It ships alongside** cheaper Ray startup, a faster control plane and lazy imports. No run isolates each part's share.
- **Where the remaining 27 s goes** is measured: the host's wheel install takes 7.7 s, a cold `import sky` on the head plus the Ray dispatch take most of the rest, and there's a hidden 6 s DNS wait. The next levers are listed with each one.
- **Scope of the evidence.** Everything was measured on one 8-core Kind node. The docs tag each claim as measured [V], read in code [C] or estimated [H].

## Where to look, in order
0. **[`LAUNCH_LATENCY.md`](LAUNCH_LATENCY.md)**: the 2-page design doc (diagnosis, design, results, limitations, what is not done, how far it can go, and the "Kubernetes-only SkyPilot" launch path).
1. **[`ll/consolidated`](https://github.com/TSunny007/skypilot/tree/ll/consolidated)** has all the code plus the `bench/` docs:
   - [`DESIGN.md`](https://github.com/TSunny007/skypilot/blob/ll/consolidated/bench/DESIGN.md): the Self-Starting Pods proposal;
   - [`ANALYSIS.md`](https://github.com/TSunny007/skypilot/blob/ll/consolidated/bench/ANALYSIS.md): the measured diagnosis of the original 54 s;
   - [`notes.md`](https://github.com/TSunny007/skypilot/blob/ll/consolidated/bench/notes.md): every result, in order. The last sections cover the headline.
2. **[`ll/benchmarked-stack`](https://github.com/TSunny007/skypilot/tree/ll/benchmarked-stack)** is the exact tree that measured 26.65 s. Its PR has the per-stage table and the breakdown of the remaining time.
3. **PRs [#1](https://github.com/TSunny007/skypilot/pull/1)–[#6](https://github.com/TSunny007/skypilot/pull/6)** are the review-sized diffs, one per area (table below); see all [open pull requests](https://github.com/TSunny007/skypilot/pulls).

## Branches
| Branch | PR | What it holds | Base |
|---|---|---|---|
| [`ll/consolidated`](https://github.com/TSunny007/skypilot/tree/ll/consolidated) | [#8](https://github.com/TSunny007/skypilot/pull/8) | Everything: all code below plus `bench/` docs and harness. Partly benchmarked | master |
| [`ll/benchmarked-stack`](https://github.com/TSunny007/skypilot/tree/ll/benchmarked-stack) | [#7](https://github.com/TSunny007/skypilot/pull/7) | The exact measured tree: A1 + A2 + A3 (3 commits) + C (4 commits) + B | master |
| [`ll/1-control-plane`](https://github.com/TSunny007/skypilot/tree/ll/1-control-plane) | [#1](https://github.com/TSunny007/skypilot/pull/1) | A1: in-process job submit; wake an idle controller instead of a 10 s poll; parallel Service creates | master |
| [`ll/2-lazy-imports`](https://github.com/TSunny007/skypilot/tree/ll/2-lazy-imports) | [#5](https://github.com/TSunny007/skypilot/pull/5) | A2: lazy `import sky`; import cycles broken; slimmer on-cluster imports | master |
| [`ll/3-status-and-submit`](https://github.com/TSunny007/skypilot/tree/ll/3-status-and-submit) | [#2](https://github.com/TSunny007/skypilot/pull/2) | A3: RUNNING right after launch; end time in the status read; add + queue in one exec; monitor schedule | master |
| [`ll/4-ray-levers`](https://github.com/TSunny007/skypilot/tree/ll/4-ray-levers) | [#3](https://github.com/TSunny007/skypilot/pull/3) | C: faster Ray bring-up; a stdlib-only per-node task; bundle IPs and live node count from the GCS | master |
| [`ll/5-post-ready-chain`](https://github.com/TSunny007/skypilot/tree/ll/5-post-ready-chain) | [#4](https://github.com/TSunny007/skypilot/pull/4) | D: skylet starts under the Ray wait; deps-only install at pod start instead of PyPI SkyPilot | `ll/4-ray-levers` |
| [`ll/6-fast-start`](https://github.com/TSunny007/skypilot/tree/ll/6-fast-start) | [#6](https://github.com/TSunny007/skypilot/pull/6) | B: opt-in self-starting pods for managed jobs (`kubernetes.fast_start`) | `ll/4-ray-levers` |

## How to reproduce
```bash
git checkout ll/consolidated && uv pip install -e ".[kubernetes]"   # and a Kind cluster: sky local up
sky api stop; sky api start --deploy
printf 'kubernetes:\n  fast_start: true\n' > ~/.sky/config.yaml   # back up any existing config first
bash bench/bench.sh sfw0; bash bench/bench.sh sf1; bash bench/bench.sh sf2   # warm-up + 2 trials
python bench/summarize.py bench/runs/sfw0 bench/runs/sf1 bench/runs/sf2 && python bench/compare.py sf=sf1,sf2
```

## Please dig in
Check out the branches and read the open PRs: their descriptions carry more color than this page. They cover the design decisions and the alternatives we rejected, what was and wasn't measured, test plans with expected output, and known risks and open bugs. If something looks off, the team logs in `bench/teams/` usually explain why it was done that way.

---

<p align="center">
  <a href="https://skypilot.ai/">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/skypilot-org/skypilot/master/docs/source/images/skypilot-wide-dark-1k.png">
      <img alt="SkyPilot" src="https://raw.githubusercontent.com/skypilot-org/skypilot/master/docs/source/images/skypilot-wide-light-1k.png" width=55%>
    </picture>
  </a>
</p>

<p align="center">
  <a href="https://docs.skypilot.co/">
    <img alt="Documentation" src="https://img.shields.io/badge/docs-gray?logo=readthedocs&logoColor=f5f5f5">
  </a>

  <a href="https://github.com/skypilot-org/skypilot/releases">
    <img alt="GitHub Release" src="https://img.shields.io/github/release/skypilot-org/skypilot.svg">
  </a>

  <a href="http://slack.skypilot.co">
    <img alt="Join Slack" src="https://img.shields.io/badge/SkyPilot-Join%20Slack-blue?logo=slack">
  </a>

  <a href="https://github.com/skypilot-org/skypilot/releases">
    <img alt="Downloads" src="https://img.shields.io/pypi/dm/skypilot">
  </a>

</p>

<h3 align="center">
    Manage all your AI compute
</h3>

SkyPilot is a system to run, manage, and scale AI workloads on any AI infrastructure.

SkyPilot gives **AI teams** a simple interface to run jobs on any infra.
**Infra teams** get a unified control plane to manage any AI compute — with advanced scheduling, scaling, and orchestration.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./docs/source/images/skypilot-abstractions-long-2-dark.png">
  <img src="./docs/source/images/skypilot-abstractions-long-2.png" alt="SkyPilot Abstractions">
</picture>

-----

:fire: *News* :fire:
- [Oct 2026] **SkyPilot AI Infra Meetup** with H Company in SF on Oct 8: [**register now**](https://partiful.com/e/kazBr6qiMhabcqdVm4nf)
- [Sep 2026] **RL is Everything, Everywhere, All at Once**: [**blog**](https://skypilot.ai/blog/rl-everything)
- [Sep 2026] **SkyPilot Agent Sessions**: Mission Control for Coding Agents, on Your Infrastructure: [**blog**](https://skypilot.ai/blog/agent-sessions)
- [Jul 2026] **Serving Kimi K3 on your own GPUs with SkyPilot**: [**blog**](https://skypilot.ai/blog/kimi-k3)
- [Jul 2026] **Run AI workloads on any cloud, store on Hugging Face**, no egress fees to worry about: [**Hugging Face blog**](https://huggingface.co/blog/skypilot-hf-storage)
- [Jun 2026] **SkyPilot Endpoints**: production-ready inference on every cluster you own: [**blog**](https://skypilot.ai/blog/skypilot-endpoints)
- [Jun 2026] **Announcing SkyPilot Sandboxes**: run untrusted, LLM-generated code on the Kubernetes clusters you already own. [**Learn more**](https://skypilot.ai/blog/sandboxes), [**join early access**](https://forms.gle/o4keAryXsVazNjyGA)
- [May 2026] **How Multiverse doubled their GPU utilization with SkyPilot**: [**case study**](https://multiversecomputing.com/papers/2x-gpu-utilization-same-hardware-discover-our-efficiency-playbook)
- [Apr 2026] Introducing **GPU Compass**: One dashboard to browse, compare pricing, and launch across every GPU cloud. Try it at [**gpus.skypilot.co**](https://gpus.skypilot.co).
- [Apr 2026] **Research-Driven Agents**: Agents read arxiv papers before coding, landed 5 llama.cpp kernel fusions and +15% faster flash attention in ~3 hours for ~$29: [**blog**](https://skypilot.ai/blog/research-driven-agents), [**HackerNews**](https://news.ycombinator.com/item?id=47706141)
- [Mar 2026] **Scaling Karpathy's Autoresearch**: Autoresearch runs 1 experiment at a time. We gave it 16 GPUs and let it run in parallel: [**blog**](https://skypilot.ai/blog/scaling-autoresearch), [**HackerNews**](https://news.ycombinator.com/item?id=47442435)
- [Mar 2026] **How H Company Unlocked Online RL and Unified their AI Platform**: [**case study**](https://hcompany.ai/unlocking-online-rl-skypilot)

## Overview

SkyPilot **is easy to use for AI users**:
- Quickly spin up compute on your own infra
- Environment and job as code — simple and portable
- Easy job management: queue, run, and auto-recover many jobs

SkyPilot **makes Kubernetes easy for AI & Infra teams**:

- Slurm-like ease of use, cloud-native robustness
- Local dev experience on K8s: SSH into pods, sync code, or connect IDE
- Turbocharge your clusters: gang scheduling, multi-cluster, and scaling

SkyPilot **unifies multiple clusters, clouds, and hardware**:
- One interface to use reserved GPUs, Kubernetes clusters, Slurm clusters, or 20+ clouds
- [Flexible provisioning](https://docs.skypilot.co/en/latest/examples/auto-failover.html) of GPUs, TPUs, CPUs, with smart failover
- [Team deployment](https://docs.skypilot.co/en/latest/reference/api-server/api-server.html) and resource sharing

SkyPilot **maximizes GPU fleet utilization**:
* Autostop: automatic cleanup of idle resources
* Binpacking: workload binpacking on shared clusters
* Intelligent scheduler: automatically schedule on the most available infra

SkyPilot supports your existing GPU, TPU, and CPU workloads, with no code changes.

Install with uv ([also supported](https://docs.skypilot.co/en/latest/getting-started/installation.html): pip, nightly, from source)
```bash
# Choose your clouds:
uv pip install "skypilot[kubernetes,aws,gcp,azure,oci,nebius,lambda,runpod,fluidstack,paperspace,cudo,ibm,scp,seeweb,shadeform,verda]"
```

To use SkyPilot directly with your agent (Claude Code, Codex, etc.), install the [SkyPilot Skill](https://docs.skypilot.co/en/latest/getting-started/skill.html). Tell your agent:
```
Fetch and follow https://github.com/skypilot-org/skypilot/blob/HEAD/agent/INSTALL.md to install the skypilot skill
```

<p align="center">
  <img src="docs/source/_static/intro.gif" alt="SkyPilot">
</p>

Current supported infra: Kubernetes, Slurm, AWS, GCP, Azure, OCI, CoreWeave, Nebius, Lambda Cloud, RunPod, Fluidstack,
Cudo, Digital Ocean, Paperspace, Cloudflare, Samsung, IBM, Vast.ai, VMware vSphere, Seeweb, Prime Intellect, Shadeform, Verda Cloud, VastData, Crusoe.
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/skypilot-org/skypilot/master/docs/source/images/cloud-logos-dark.png">
    <img alt="SkyPilot" src="https://raw.githubusercontent.com/skypilot-org/skypilot/master/docs/source/images/cloud-logos-light.png" width=85%>
  </picture>
</p>
<!-- source xcf file: https://drive.google.com/drive/folders/1S_acjRsAD3T14qMeEnf6FFrIwHu_Gs_f?usp=drive_link -->


## Getting started

[Install SkyPilot](https://docs.skypilot.co/en/latest/getting-started/installation.html) in 1 minute. Then, launch your first cluster in 2 minutes in [Quickstart](https://docs.skypilot.co/en/latest/getting-started/quickstart.html).

SkyPilot is BYOC: Everything is launched within your cloud accounts, VPCs, and clusters.

## Benefits of SkyPilot on Kubernetes

SkyPilot makes Kubernetes AI-native.

It turbocharges your existing Kubernetes clusters by **accelerating AI/ML velocity**:

- AI-friendly interface to launch jobs and deployments
- Much simplified interactive dev for K8s (SSH / sync code / connect IDE to pods)

...and **optimizing GPU scheduling, utilization, and scaling**:

- Advanced scheduling: Gang scheduling, multi-node jobs, and queueing
- Multi-cluster support: Bring all your clusters under one control plane
- Multi-cloud support: One consistent interface to manage many providers

See [SkyPilot vs Vanilla Kubernetes](https://docs.skypilot.co/en/latest/reference/kubernetes/skypilot-and-vanilla-k8s.html) and this [blog post](https://skypilot.ai/blog/ai-on-kubernetes) for more details.

## SkyPilot in 1 minute

A SkyPilot task specifies: resource requirements, data to be synced, setup commands, and the task commands.

Once written in this [**unified interface**](https://docs.skypilot.co/en/latest/reference/yaml-spec.html) (YAML or Python API), the task can be launched on any available infra (Kubernetes, Slurm, cloud, etc.).  This avoids vendor lock-in, and allows easily moving jobs to a different provider.

Paste the following into a file `my_task.yaml`:

```yaml
resources:
  accelerators: A100:8  # 8x NVIDIA A100 GPU

num_nodes: 1  # Number of VMs to launch

# Working directory (optional) containing the project codebase.
# Its contents are synced to ~/sky_workdir/ on the cluster.
workdir: ~/torch_examples

# Commands to be run before executing the job.
# Typical use: pip install -r requirements.txt, git clone, etc.
setup: |
  cd mnist
  pip install -r requirements.txt

# Commands to run as a job.
# Typical use: launch the main program.
run: |
  cd mnist
  python main.py --epochs 1
```

Prepare the workdir by cloning:
```bash
git clone https://github.com/pytorch/examples.git ~/torch_examples
```

Launch with `sky launch` (note: [access to GPU instances](https://docs.skypilot.co/en/latest/cloud-setup/quota.html) is needed for this example):
```bash
sky launch my_task.yaml
```

SkyPilot then performs the heavy-lifting for you, including:
1. Find the cheapest & available infra across your clusters or clouds
2. Provision the GPUs (pods or VMs), with auto-failover if the infra returned capacity errors
3. Sync your local `workdir` to the provisioned cluster
4. Auto-install dependencies by running the task's `setup` commands
5. Run the task's `run` commands, and stream logs

See [Quickstart](https://docs.skypilot.co/en/latest/getting-started/quickstart.html) to get started with SkyPilot.

## Runnable examples

See [**SkyPilot examples**](https://docs.skypilot.co/en/docs-examples/examples/index.html) that cover: development, training, serving, LLM models, AI apps, and common frameworks.

Latest featured examples:

| Task | Examples |
|----------|----------|
| Training | [Verl](https://docs.skypilot.co/en/latest/examples/training/verl.html), [Finetune Llama 4](https://docs.skypilot.co/en/latest/examples/training/llama-4-finetuning.html), [TorchTitan](https://docs.skypilot.co/en/latest/examples/training/torchtitan.html), [PyTorch](https://docs.skypilot.co/en/latest/getting-started/tutorial.html), [DeepSpeed](https://docs.skypilot.co/en/latest/examples/training/deepspeed.html), [NeMo](https://docs.skypilot.co/en/latest/examples/training/nemo.html), [Ray](https://docs.skypilot.co/en/latest/examples/training/ray.html), [Unsloth](https://docs.skypilot.co/en/latest/examples/training/unsloth.html), [Jax/TPU](https://docs.skypilot.co/en/latest/examples/training/tpu.html), [OpenRLHF](https://docs.skypilot.co/en/latest/examples/training/openrlhf.html) |
| Serving | [vLLM](https://docs.skypilot.co/en/latest/examples/serving/vllm.html), [SGLang](https://docs.skypilot.co/en/latest/examples/serving/sglang.html), [Ollama](https://docs.skypilot.co/en/latest/examples/serving/ollama.html) |
| Models | [DeepSeek-R1](https://docs.skypilot.co/en/latest/examples/models/deepseek-r1.html), [Llama 4](https://docs.skypilot.co/en/latest/examples/models/llama-4.html), [Llama 3](https://docs.skypilot.co/en/latest/examples/models/llama-3.html), [CodeLlama](https://docs.skypilot.co/en/latest/examples/models/codellama.html), [Qwen](https://docs.skypilot.co/en/latest/examples/models/qwen.html), [Kimi-K2](https://docs.skypilot.co/en/latest/examples/models/kimi-k2.html), [Kimi-K2-Thinking](https://docs.skypilot.co/en/latest/examples/models/kimi-k2-thinking.html), [Mixtral](https://docs.skypilot.co/en/latest/examples/models/mixtral.html) |
| AI apps | [RAG](https://docs.skypilot.co/en/latest/examples/applications/rag.html), [vector databases](https://docs.skypilot.co/en/latest/examples/applications/vector_database.html) (ChromaDB, CLIP) |
| Common frameworks | [Airflow](https://docs.skypilot.co/en/latest/examples/frameworks/airflow.html), [Jupyter](https://docs.skypilot.co/en/latest/examples/frameworks/jupyter.html), [marimo](https://docs.skypilot.co/en/latest/examples/frameworks/marimo.html)  |

Source files can be found in [`llm/`](https://github.com/skypilot-org/skypilot/tree/master/llm) and [`examples/`](https://github.com/skypilot-org/skypilot/tree/master/examples).

## Learn more
To learn more, see [SkyPilot Overview](https://docs.skypilot.co/en/latest/overview.html), [SkyPilot docs](https://docs.skypilot.co/en/latest/), and [SkyPilot blog](https://skypilot.ai/blog).

SkyPilot adopters: [Testimonials and Case Studies](https://skypilot.ai/case-studies)

Follow updates:
- [Slack](http://slack.skypilot.co)
- [X](https://twitter.com/skypilot_org)
- [LinkedIn](https://www.linkedin.com/company/skypilot-oss/)
- [YouTube](https://www.youtube.com/@skypilot-org)
- [SkyPilot Blog](https://skypilot.ai/blog)

## Questions and feedback
We are excited to hear your feedback:
* For issues and feature requests, please [open a GitHub issue](https://github.com/skypilot-org/skypilot/issues/new).
* For questions, please use [GitHub Discussions](https://github.com/skypilot-org/skypilot/discussions).

For general discussions, join us on the [SkyPilot Slack](http://slack.skypilot.co).

## Contributing
We welcome all contributions to the project! See [CONTRIBUTING](CONTRIBUTING.md) for how to get involved.

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=76c302d0-bd8d-4233-b7df-237d353375cd" />
