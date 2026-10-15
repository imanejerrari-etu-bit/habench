import mysql.connector
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

CONN_PARAMS = {
    'host': 'mysql-0.mysql.habench.svc.cluster.local',
    'port': 3306,
    'database': 'habench_db',
    'user': 'root',
    'password': 'habench2024'
}

MODES = {
    'async':     'OFF',
    'semi_sync': 'OFF',  # MySQL GR gère via w=1/2/majority
    'sync':      'OFF'
}

WRITE_CONCERNS = {
    'async':     1,
    'semi_sync': 2,
    'sync':      'majority'
}

def init_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bench_writes (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            test_uuid VARCHAR(36),
            payload VARCHAR(256),
            ts DATETIME DEFAULT NOW(),
            mode VARCHAR(20)
        )
    """)
    conn.commit()
    cursor.close()

def set_gr_consistency(conn, mode):
    cursor = conn.cursor()
    if mode == 'async':
        cursor.execute("SET SESSION group_replication_consistency='EVENTUAL'")
    elif mode == 'semi_sync':
        cursor.execute("SET SESSION group_replication_consistency='BEFORE_ON_PRIMARY_FAILOVER'")
    else:
        cursor.execute("SET SESSION group_replication_consistency='AFTER'")
    cursor.close()

def run_benchmark(mode_name):
    print("\n[" + mode_name.upper() + "] GR consistency mode")
    all_latencies = []

    conn = mysql.connector.connect(**CONN_PARAMS, connection_timeout=30)
    init_table(conn)
    set_gr_consistency(conn, mode_name)

    # Warmup
    cursor = conn.cursor()
    for _ in range(WARMUP):
        cursor.execute(
            "INSERT INTO bench_writes (test_uuid, payload, mode) VALUES (%s, %s, %s)",
            (str(uuid.uuid4()), 'x' * 200, mode_name)
        )
        conn.commit()

    # Mesures
    for run in range(N_RUNS):
        run_lats = []
        for _ in range(N_INSERTIONS):
            t0 = time.perf_counter()
            cursor.execute(
                "INSERT INTO bench_writes (test_uuid, payload, mode) VALUES (%s, %s, %s)",
                (str(uuid.uuid4()), 'x' * 200, mode_name)
            )
            conn.commit()
            run_lats.append((time.perf_counter() - t0) * 1000)

        all_latencies.extend(run_lats)
        print("  Run " + str(run+1).rjust(2) + "/" + str(N_RUNS) +
              ": mean=" + str(round(statistics.mean(run_lats), 2)) + "ms")

    cursor.close()
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
        'db': 'mysql_gr', 'mode': mode_name, 'n': len(all_latencies),
        'mean_ms': round(mean_val, 3), 'std_ms': round(std_val, 3),
        'p50_ms': round(p50, 3), 'p95_ms': round(p95, 3), 'p99_ms': round(p99, 3),
        'ci_low': round(ci_low, 3), 'ci_high': round(ci_high, 3)
    }
    print("  RESULT: mean=" + str(result['mean_ms']) + "ms P95=" + str(result['p95_ms']) +
          "ms CI=[" + str(result['ci_low']) + ", " + str(result['ci_high']) + "]")
    return result

if __name__ == '__main__':
    print("HABench -- MySQL Group Replication Latency Benchmark")
    print("Started: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    print("\nTest connexion...")
    conn = mysql.connector.connect(**CONN_PARAMS, connection_timeout=10)
    conn.close()
    print("Connexion OK!")

    results = []
    for mode_name in ['async', 'semi_sync', 'sync']:
        r = run_benchmark(mode_name)
        results.append(r)

    async_mean = next(r['mean_ms'] for r in results if r['mode'] == 'async')
    for r in results:
        r['overhead_pct'] = round(((r['mean_ms'] - async_mean) / async_mean) * 100, 1)

    with open('/tmp/mysql_latency_results.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    with open('/tmp/mysql_latency_results.json', 'w') as f:
        json.dump(results, f, indent=2)

    print("\n========== FINAL RESULTS ==========")
    print("Mode         Mean(ms)   P95(ms)   P99(ms)   Overhead")
    print("-" * 55)
    for r in results:
        print(r['mode'].ljust(12) + str(r['mean_ms']).rjust(10) +
              str(r['p95_ms']).rjust(10) + str(r['p99_ms']).rjust(10) +
              (str(r['overhead_pct']) + "%").rjust(10))
    print("\nResultats: /tmp/mysql_latency_results.csv / .json")
