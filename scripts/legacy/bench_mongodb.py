import pymongo
import time
import uuid
import statistics
import numpy as np
import csv
import json
from datetime import datetime, timezone

NAMESPACE    = "habench"
N_INSERTIONS = 1000
N_RUNS       = 10
WARMUP       = 50

MODES = {
    'async':     1,
    'semi_sync': 2,
    'sync':      'majority'
}

# Replica set via 3 port-forwards
MONGO_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"

def get_collection(w):
    client = pymongo.MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=15000,
        connectTimeoutMS=10000
    )
    db = client["habench_db"]
    col = db.get_collection(
        "bench_writes",
        write_concern=pymongo.WriteConcern(w=w, wtimeout=30000)
    )
    return client, col

def run_benchmark(mode_name, w):
    print("\n[" + mode_name.upper() + "] write concern w=" + str(w))
    all_latencies = []

    for run in range(N_RUNS):
        try:
            client, col = get_collection(w)

            # Warmup
            for _ in range(WARMUP):
                col.insert_one({
                    "test_uuid": str(uuid.uuid4()),
                    "payload":   "x" * 200,
                    "mode":      mode_name,
                    "ts":        datetime.now(timezone.utc)
                })

            # Mesures
            run_lats = []
            for _ in range(N_INSERTIONS):
                t0 = time.perf_counter()
                col.insert_one({
                    "test_uuid": str(uuid.uuid4()),
                    "payload":   "x" * 200,
                    "mode":      mode_name,
                    "ts":        datetime.now(timezone.utc)
                })
                run_lats.append((time.perf_counter() - t0) * 1000)

            all_latencies.extend(run_lats)
            print("  Run " + str(run+1).rjust(2) + "/" + str(N_RUNS) +
                  ": mean=" + str(round(statistics.mean(run_lats), 2)) + "ms")
            client.close()

        except Exception as e:
            print("  Run " + str(run+1) + " ERREUR: " + str(e))

    if not all_latencies:
        return None

    mean_val = statistics.mean(all_latencies)
    std_val  = statistics.stdev(all_latencies)
    p50      = float(np.percentile(all_latencies, 50))
    p95      = float(np.percentile(all_latencies, 95))
    p99      = float(np.percentile(all_latencies, 99))

    rng = np.random.default_rng(42)
    boot_means = [
        float(np.mean(rng.choice(all_latencies, len(all_latencies), replace=True)))
        for _ in range(2000)
    ]
    ci_low  = float(np.percentile(boot_means, 2.5))
    ci_high = float(np.percentile(boot_means, 97.5))

    result = {
        'db':      'mongodb',
        'mode':    mode_name,
        'w':       str(w),
        'n':       len(all_latencies),
        'mean_ms': round(mean_val, 3),
        'std_ms':  round(std_val, 3),
        'p50_ms':  round(p50, 3),
        'p95_ms':  round(p95, 3),
        'p99_ms':  round(p99, 3),
        'ci_low':  round(ci_low, 3),
        'ci_high': round(ci_high, 3)
    }

    print("  RESULT: mean=" + str(result['mean_ms']) + "ms"
          + " P95=" + str(result['p95_ms']) + "ms"
          + " P99=" + str(result['p99_ms']) + "ms"
          + " CI=[" + str(result['ci_low']) + ", " + str(result['ci_high']) + "]")
    return result


if __name__ == '__main__':
    print("HABench -- MongoDB Latency Benchmark (Replica Set)")
    print("URI: " + MONGO_URI)
    print("Insertions: " + str(N_INSERTIONS) + " x " + str(N_RUNS) + " runs per mode")
    print("Started: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("")
    print("IMPORTANT: Verifier que les 3 port-forwards sont actifs :")
    print("  kubectl port-forward pod/mongodb-0 27017:27017 -n habench")
    print("  kubectl port-forward pod/mongodb-1 27018:27017 -n habench")
    print("  kubectl port-forward pod/mongodb-2 27019:27017 -n habench")
    print("")

    # Test connexion
    print("Test connexion replica set...")
    for attempt in range(10):
        try:
            client, col = get_collection(1)
            col.insert_one({"test": "ping", "ts": datetime.now(timezone.utc)})
            # Verifier qu'on voit bien le replica set
            status = client.admin.command("replSetGetStatus")
            members = status.get("members", [])
            print("Connexion OK! Membres du replica set: " + str(len(members)))
            for m in members:
                print("  " + m.get("name", "?") + " -> " + m.get("stateStr", "?"))
            client.close()
            break
        except Exception as e:
            print("  Tentative " + str(attempt+1) + ": " + str(e))
            time.sleep(3)
    else:
        print("ERREUR: impossible de se connecter au replica set")
        exit(1)

    results = []
    for mode_name, w in MODES.items():
        r = run_benchmark(mode_name, w)
        if r:
            results.append(r)

    if not results:
        print("Aucun resultat")
        exit(1)

    # Overhead vs async
    async_mean = next(r['mean_ms'] for r in results if r['mode'] == 'async')
    for r in results:
        r['overhead_pct'] = round(((r['mean_ms'] - async_mean) / async_mean) * 100, 1)

    with open('mg_latency_results.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    with open('mg_latency_results.json', 'w') as f:
        json.dump(results, f, indent=2)

    print("\n========== FINAL RESULTS ==========")
    print("Mode         Mean(ms)   P95(ms)   P99(ms)   Overhead")
    print("-" * 55)
    for r in results:
        print(
            r['mode'].ljust(12) +
            str(r['mean_ms']).rjust(10) +
            str(r['p95_ms']).rjust(10) +
            str(r['p99_ms']).rjust(10) +
            (str(r['overhead_pct']) + "%").rjust(10)
        )

    print("\nResultats: mg_latency_results.csv / .json")
