"""Recompute the RTO statistics reported for HABench from the raw per-test files.

Usage (from the repository root):  python analysis/analyze_rto.py
Requires: numpy, pandas, scipy
"""
import numpy as np, pandas as pd
from scipy import stats

R = "results/"
rng = np.random.default_rng(42)

def boot_ci(x, n=10_000):
    m = [rng.choice(x, len(x)).mean() for _ in range(n)]
    return np.percentile(m, [2.5, 97.5])

def describe(name, x):
    x = np.asarray(x, float)
    q1, med, q3 = np.percentile(x, [25, 50, 75])
    w, p = stats.shapiro(x)
    lo, hi = boot_ci(x)
    print(f"{name}: n={len(x)} mean={x.mean():.3f} sd={x.std(ddof=1):.3f} median={med:.3f} IQR=[{q1:.2f}, {q3:.2f}] "
          f"min={x.min():.3f} max={x.max():.3f}")
    print(f"   Shapiro-Wilk W={w:.3f} p={p:.2g};  95% bootstrap CI of the mean = [{lo:.2f}, {hi:.2f}]")

def cliffs_delta(a, b):
    a, b = np.asarray(a), np.asarray(b)
    return (np.sum(a[:, None] > b[None, :]) - np.sum(a[:, None] < b[None, :])) / (len(a) * len(b))

def cohen_d(a, b):
    sp = np.sqrt(((len(a)-1)*np.var(a, ddof=1) + (len(b)-1)*np.var(b, ddof=1)) / (len(a)+len(b)-2))
    return (np.mean(a) - np.mean(b)) / sp

pg = pd.read_csv(R + "pg_rto_raw.csv")
pg = pg[pg["success"] == True]["rto_seconds"].to_numpy(float)
mg = pd.read_csv(R + "mg_rto_raw.csv")
mg = mg[mg["success"] == True]["rto_seconds"].to_numpy(float)
my = pd.read_csv(R + "mysql_gr_rto_run_2026-03-13.csv")

print("=== RTO (seconds) ===")
describe("PostgreSQL (CloudNativePG)", pg)
describe("MongoDB (replica set)", mg)
u, p = stats.mannwhitneyu(pg, mg, alternative="two-sided")
print(f"PostgreSQL vs MongoDB: Mann-Whitney U={u:.0f} p={p:.2g}  Cliff's delta={cliffs_delta(pg, mg):.2f}  Cohen's d={cohen_d(pg, mg):.2f}")

print("\n=== MySQL Group Replication, run of 2026-03-13 (transcribed from the console log) ===")
auto = my[~my["scripted_bootstrap_used"]]["rto_seconds"].to_numpy(float)
boot = my[my["scripted_bootstrap_used"]]["rto_seconds"].to_numpy(float)
print(f"tests logged: {len(my)}; recovered without scripted bootstrap: {len(auto)}; with scripted bootstrap: {len(boot)}")
print(f"no bootstrap : mean={auto.mean():.3f} sd={auto.std(ddof=1):.3f} min={auto.min():.3f} max={auto.max():.3f}")
print(f"bootstrap    : mean={boot.mean():.3f} sd={boot.std(ddof=1):.3f} min={boot.min():.3f} max={boot.max():.3f}")
print("RPO values logged by this script are not reliable (row count unavailable on some pods).")

print("\n=== Write latency (from results/*_latency_results.csv) ===")
for f in ["pg_latency_results.csv", "mysql_latency_results.csv", "mongodb_latency_results.csv"]:
    print(pd.read_csv(R + f).to_string(index=False))
