import pymongo
import time
import uuid
import statistics
import json
import csv
import subprocess
from datetime import datetime

NAMESPACE = "habench"
N_TESTS   = 30
TIMEOUT   = 120

def get_primary_pod():
    cmd = ["kubectl", "get", "pods", "-n", NAMESPACE, "-l", "app=mongodb",
           "-o", "jsonpath={.items[*].metadata.name}"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    pods = result.stdout.strip().split()
    for pod in pods:
        try:
            r = subprocess.run(
                ["kubectl", "exec", pod, "-n", NAMESPACE, "--",
                 "mongosh", "--quiet", "--eval",
                 "db.runCommand({isMaster:1}).ismaster"],
                capture_output=True, text=True, timeout=10
            )
            if "true" in r.stdout.lower():
                return pod
        except:
            pass
    return None

def get_mongo_client():
    return pymongo.MongoClient(
        "mongodb://mongodb-0.mongodb.habench.svc.cluster.local:27017,"
        "mongodb-1.mongodb.habench.svc.cluster.local:27017,"
        "mongodb-2.mongodb.habench.svc.cluster.local:27017/"
        "?replicaSet=rs0",
        serverSelectionTimeoutMS=5000
    )

def write_probe():
    try:
        client = get_mongo_client()
        col = client["habench_db"]["bench_rto"]
        col.with_options(
            write_concern=pymongo.WriteConcern(w="majority", wtimeout=5000)
        ).insert_one({"uuid": str(uuid.uuid4()), "ts": datetime.utcnow()})
        client.close()
        return True
    except:
        return False

def count_docs():
    try:
        client = get_mongo_client()
        count = client["habench_db"]["bench_rto"].count_documents({})
        client.close()
        return count
    except:
        return None

def wait_recovery(t_start):
    while time.perf_counter() - t_start < TIMEOUT:
        if write_probe():
            return time.perf_counter() - t_start
        time.sleep(0.5)
    return None

def run_test(test_id):
    print("\n  Test " + str(test_id) + "/" + str(N_TESTS))

    for _ in range(5):
        write_probe()
    count_pre = count_docs()
    print("  Docs avant: " + str(count_pre))

    primary = get_primary_pod()
    if not primary:
        print("  ERREUR: primary introuvable")
        return None
    print("  Primary: " + primary)

    # Supprimer le primary
    t0 = time.perf_counter()
    subprocess.run(
        ["kubectl", "delete", "pod", primary, "-n", NAMESPACE,
         "--grace-period=0", "--force"],
        capture_output=True
    )
    print("  Pod supprime, attente recuperation...")

    time.sleep(2)
    rto = wait_recovery(t0)

    if rto is None:
        print("  TIMEOUT")
        return {"test_id": test_id, "rto_seconds": None, "rpo": None, "success": False}

    count_after = count_docs()
    rpo = max(0, (count_pre or 0) - (count_after or 0))
    new_primary = get_primary_pod()

    print("  RTO = " + str(round(rto, 2)) + "s")
    print("  RPO = " + str(rpo))
    print("  Nouveau primary: " + str(new_primary))

    result = {"test_id": test_id, "primary_deleted": primary,
              "new_primary": new_primary, "rto_seconds": round(rto, 3),
              "rpo": rpo, "success": True}

    print("  Stabilisation 30s...")
    time.sleep(30)
    return result

if __name__ == "__main__":
    print("HABench -- MongoDB RTO/RPO Benchmark")
    print("N tests: " + str(N_TESTS))
    print("Started: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    print("\nTest connexion...")
    for i in range(10):
        if write_probe():
            print("Connexion OK!")
            break
        time.sleep(3)
    else:
        print("ERREUR connexion")
        exit(1)

    results = []
    for i in range(1, N_TESTS + 1):
        r = run_test(i)
        if r:
            results.append(r)

    successful = [r for r in results if r["success"] and r["rto_seconds"]]
    rtos = [r["rto_seconds"] for r in successful]
    rpos = [r["rpo"] for r in successful]

    if len(rtos) > 1:
        import numpy as np
        from scipy import stats as st
        mean_rto = statistics.mean(rtos)
        std_rto  = statistics.stdev(rtos)
        ci = st.t.interval(0.95, df=len(rtos)-1, loc=mean_rto, scale=st.sem(rtos))
        summary = {
            "db": "mongodb", "n_tests": N_TESTS,
            "n_success": len(successful),
            "mean_rto": round(mean_rto, 3),
            "std_rto": round(std_rto, 3),
            "min_rto": round(min(rtos), 3),
            "max_rto": round(max(rtos), 3),
            "ci_low_95": round(ci[0], 3),
            "ci_high_95": round(ci[1], 3),
            "mean_rpo": round(statistics.mean(rpos), 2),
            "max_rpo": max(rpos),
            "success_rate": str(len(successful)) + "/" + str(N_TESTS)
        }
    else:
        summary = {"error": "not enough data"}

    with open("/tmp/mg_rto_raw.csv", "w", newline="") as f:
        if results:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)

    with open("/tmp/mg_rto_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("\n========== RTO SUMMARY ==========")
    for k, v in summary.items():
        print("  " + str(k).ljust(15) + ": " + str(v))
    print("\nResultats: /tmp/mg_rto_raw.csv / /tmp/mg_rto_summary.json")
