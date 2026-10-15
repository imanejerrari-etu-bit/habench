import psycopg2
import time
import uuid
import statistics
import numpy as np
import csv
import json
from datetime import datetime

N_INSERTIONS = 1000
N_RUNS       = 10
WARMUP       = 50

# Connexion directe via DNS Kubernetes
CONN_PARAMS = {
    'host': 'pg-habench-rw.habench.svc.cluster.local',
    'port': 5432,
    'database': 'app',
    'user': 'app',
    'password': 'FPB1Q51OfjcvyAO9FesEoqzS837LyjLpI7PNPRCtDJNAwmVKW6MGoFzqf3sRXWUE'
}

MODES = {
    'async':     'off',
    'semi_sync': 'remote_write',
    'sync':      'on'
}

def init_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bench_writes (
                id BIGSERIAL PRIMARY KEY,
                test_uuid VARCHAR(36),
                payload VARCHAR(256),
                ts TIMESTAMPTZ DEFAULT NOW(),
                mode VARCHAR(20)
            )
        """)
    conn.commit()

def run_benchmark(mode_name, sync_commit):
    print("\n[" + mode_name.upper() + "] synchronous_commit = " + sync_commit)
    all_latencies = []

    conn = psycopg2.connect(**CONN_PARAMS, connect_timeout=30)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = '" + sync_commit + "'")
    init_table(conn)

    for _ in range(WARMUP):
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO bench_writes (test_uuid, payload, mode) VALUES (%s, %s, %s)",
                (str(uuid.uuid4()), 'x' * 200, mode_name)
            )
        conn.commit()

    for run in range(N_RUNS):
        run_lats = []
        for _ in range(N_INSERTIONS):
            t0 = time.perf_counter()
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO bench_writes (test_uuid, payload, mode) VALUES (%s, %s, %s)",
                    (str(uuid.uuid4()), 'x' * 200, mode_name)
                )
            conn.commit()
            run_lats.append((time.perf_counter() - t0) * 1000)

        all_latencies.extend(run_lats)
        print("  Run " + str(run+1).rjust(2) + "/" + str(N_RUNS) +
              ": mean=" + str(round(statistics.mean(run_lats), 2)) + "ms")

    conn.close()

    mean_val = statistics.mean(all_latencies)
    std_val  = statistics.stdev(all_latencies)
    p50 = float(np.percentile(all_latencies, 50))
    p95 = float(np.percentile(all_latencies, 95))
    p99 = float(np.percentile(all_latencies, 99))

    rng = np.random.default_rng(42)
    boot = [float(np.mean(rng.choice(all_latencies, len(all_latencies), replace=True))) for _ in range(2000)]
    ci_low  = float(np.percentile(boot, 2.5))
    ci_high = float(np.percentile(boot, 97.5))

    result = {
        'db': 'postgresql', 'mode': mode_name, 'n': len(all_latencies),
        'mean_ms': round(mean_val, 3), 'std_ms': round(std_val, 3),
        'p50_ms': round(p50, 3), 'p95_ms': round(p95, 3), 'p99_ms': round(p99, 3),
        'ci_low': round(ci_low, 3), 'ci_high': round(ci_high, 3)
    }
    print("  RESULT: mean=" + str(result['mean_ms']) + "ms P95=" + str(result['p95_ms']) +
          "ms CI=[" + str(result['ci_low']) + ", " + str(result['ci_high']) + "]")
    return result

if __name__ == '__main__':
    print("HABench -- PostgreSQL Latency Benchmark (in-pod)")
    print("Host: " + CONN_PARAMS['host'])
    print("Started: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Test connexion
    print("\nTest connexion...")
    conn = psycopg2.connect(**CONN_PARAMS, connect_timeout=10)
    conn.close()
    print("Connexion OK!")

    results = []
    for mode_name, sync_val in MODES.items():
        r = run_benchmark(mode_name, sync_val)
        results.append(r)

    async_mean = next(r['mean_ms'] for r in results if r['mode'] == 'async')
    for r in results:
        r['overhead_pct'] = round(((r['mean_ms'] - async_mean) / async_mean) * 100, 1)

    with open('/tmp/pg_latency_results.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    with open('/tmp/pg_latency_results.json', 'w') as f:
        json.dump(results, f, indent=2)

    print("\n========== FINAL RESULTS ==========")
    print("Mode         Mean(ms)   P95(ms)   P99(ms)   Overhead")
    print("-" * 55)
    for r in results:
        print(r['mode'].ljust(12) + str(r['mean_ms']).rjust(10) +
              str(r['p95_ms']).rjust(10) + str(r['p99_ms']).rjust(10) +
              (str(r['overhead_pct']) + "%").rjust(10))
    print("\nResultats: /tmp/pg_latency_results.csv / .json")
