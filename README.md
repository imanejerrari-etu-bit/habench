# HABench

Scripts, Kubernetes manifests and raw results used in the paper
**"HABench: A Benchmarking Framework for Evaluating High Availability of Heterogeneous Database Systems in Kubernetes Environments"**
(Imane Jerrari, Ismail Assayad; submitted to *Cluster Computing*). Licensed under Apache-2.0.

All experiments were run on a single-node Minikube cluster (Docker driver, 4 vCPU, 6000 MB RAM, Kubernetes v1.34.0)
on Windows 11 + WSL2. Results are specific to this testbed.

## Repository layout

```
manifests/            Kubernetes manifests of the evaluated deployments
  01_postgresql.yaml    CloudNativePG cluster (3 instances)
  02_mongodb.yaml       MongoDB 6.0 replica set (3-member StatefulSet)
  03_mysql.yaml         MySQL 8.0.35 Group Replication (3-member StatefulSet, single-primary)
  04_mariadb.yaml       MariaDB Galera attempt (could not be deployed on the WSL2 kernel used)
  join_gr.sql           helper to (re)join a member to the Group Replication group
  other-variants/       earlier or alternative manifests, not used for the reported results
scripts/
  latency/              write-latency benchmarks (bench_pg_inpod.py, bench_mysql.py, bench_mongodb.js)
  rto/                  failover (RTO/RPO) benchmarks (bench_rto_pg_inpod.py, bench_rto_mongodb.py, bench_rto_mysql_gr_v2.py)
  legacy/               earlier versions (port-forward based, or runs that did not produce usable data)
results/                raw and summary results (see below)
analysis/analyze_rto.py recomputes the RTO statistics of the paper from results/
```

## How each system was configured in the benchmarks

| System | Deployment | Latency modes (script) | Failover (RTO) test |
|---|---|---|---|
| PostgreSQL 15 | CloudNativePG `Cluster`, 3 instances | per-session `SET synchronous_commit` = `off` / `remote_write` / `on` | pod of the primary deleted; write probe through the read-write service |
| MongoDB 6.0 | 3-member StatefulSet, replica set `rs0` initialised with `rs.initiate` | `writeConcern` w:1 / w:2 / w:"majority" | writes and probes with w:"majority" |
| MySQL 8.0.35 | 3-member StatefulSet, Group Replication, single-primary | per-session `group_replication_consistency` = `EVENTUAL` / `BEFORE_ON_PRIMARY_FAILOVER` / `AFTER` | see `results/mysql_gr_rto_run_2026-03-13.csv` |

`synchronous_standby_names` is not set in the PostgreSQL manifest or scripts.

## Results files

| File | Content |
|---|---|
| `pg_latency_results.*`, `mysql_latency_results.*`, `mongodb_latency_results.*` | write-latency summaries per mode (n = 10,000 writes: 10 runs x 1,000), with 95% bootstrap CI where available |
| `pg_rto_raw.csv`, `pg_rto_summary.json` | PostgreSQL failover tests (n = 30) |
| `mg_rto_raw.csv`, `mg_rto_summary.json` | MongoDB failover tests (29 recorded tests) |
| `mysql_gr_rto_run_2026-03-13.csv` | MySQL GR failover run, transcribed from the console log (the script printed the values but its summary files were not saved); `scripted_bootstrap_used` marks tests where the script itself had to bootstrap the group; the `rpo_reported` column is not reliable (row counts were unavailable on some pods) |
| `analysis_output.txt` | output of `analysis/analyze_rto.py` |

The `*_summary.json` files report a Student-t interval; the paper reports bootstrap intervals computed by `analysis/analyze_rto.py`.
Per-write latency observations were not saved; only the summaries above exist.

## Reproducing the experiments

1. Start Minikube: `minikube start -p habench --cpus=4 --memory=6000 --driver=docker`.
2. Create the namespace `habench`, then deploy one system at a time with `kubectl apply -f manifests/0X_*.yaml`
   (MongoDB additionally needs `rs.initiate(...)` once the three pods are running).
3. Run a benchmark pod (`python:3.11-slim` with the client libraries) in the cluster, copy a script from `scripts/` into it with
   `kubectl cp` and run it with `kubectl exec`. The MongoDB latency script runs with `mongosh` inside the primary pod.
4. Delete the PVCs between systems.

Credentials in the manifests and scripts belong to a disposable local test cluster. Change them before any other use.
Exact client library versions were not recorded.

## Analysis

```
pip install numpy pandas scipy
python analysis/analyze_rto.py
```

## Notes on the failover scripts

The three failover scripts differ slightly (see the paper, Sect. 5.4):
- PostgreSQL (`bench_rto_pg_inpod.py`): the primary pod is deleted manually from a second terminal; probing starts immediately (0.3 s / 0.5 s).
- MongoDB (`bench_rto_mongodb.py`): the script deletes the pod itself and starts probing 2 s later (0.5 s interval). `mg_rto_raw.csv` contains tests 2 to 30; the script writes no record when it cannot identify the primary.
- MySQL GR (`bench_rto_mysql_gr_v2.py`): the script deletes the pod itself, starts probing 5 s later (about 3 s between attempts, through the `mysql` client run with `kubectl exec`), and bootstraps the group if no PRIMARY emerges; the run was interrupted after 13 tests.
