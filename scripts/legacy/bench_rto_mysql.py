import mysql.connector
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

BASE_CONN = {
    'port': 3306,
    'database': 'habench_db',
    'user': 'root',
    'password': 'habench2024'
}

PODS = [
    'mysql-0.mysql.habench.svc.cluster.local',
    'mysql-1.mysql.habench.svc.cluster.local',
    'mysql-2.mysql.habench.svc.cluster.local'
]

def get_primary_host():
    for host in PODS:
        try:
            conn = mysql.connector.connect(**BASE_CONN, host=host, connection_timeout=5)
            cursor = conn.cursor()
            cursor.execute("SELECT MEMBER_ROLE FROM performance_schema.replication_group_members WHERE MEMBER_HOST=@@report_host")
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            if row and row[0] == 'PRIMARY':
                return host
        except:
            pass
    return None

def get_primary_pod():
    cmd = ["kubectl", "get", "pods", "-n", NAMESPACE, "-l", "app=mysql",
           "-o", "jsonpath={.items[*].metadata.name}"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    pods = result.stdout.strip().split()
    for pod in pods:
        try:
            r = subprocess.run(
                ["kubectl", "exec", pod, "-n", NAMESPACE, "--",
                 "mysql", "-uroot", "-phabench2024", "-sNe",
                 "SELECT MEMBER_ROLE FROM performance_schema.replication_group_members WHERE MEMBER_HOST=@@report_host"],
                capture_output=True, text=True, timeout=10
            )
            if "PRIMARY" in r.stdout:
                return pod
        except:
            pass
    return None

def restart_gr_on_pod(pod):
    try:
        subprocess.run(
            ["kubectl", "exec", pod, "-n", NAMESPACE, "--",
             "mysql", "-uroot", "-phabench2024", "-e",
             "SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF; STOP GROUP_REPLICATION; RESET MASTER; START GROUP_REPLICATION;"],
            capture_output=True, timeout=30
        )
        return True
    except:
        return False

def write_probe(host):
    try:
        conn = mysql.connector.connect(**BASE_CONN, host=host, connection_timeout=5)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO bench_writes (test_uuid, payload, mode) VALUES (%s, %s, %s)",
            (str(uuid.uuid4()), 'x' * 200, 'rto_probe')
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except:
        return False

def count_rows(host):
    try:
        conn = mysql.connector.connect(**BASE_CONN, host=host, connection_timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM bench_writes")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except:
        return None

def wait_recovery(t_start):
    while time.perf_counter() - t_start < TIMEOUT:
        host = get_primary_host()
        if host and write_probe(host):
            return time.perf_counter() - t_start, host
        time.sleep(0.5)
    return None, None

def run_test(test_id):
    print("\n  Test " + str(test_id) + "/" + str(N_TESTS))

    primary_host = get_primary_host()
    if not primary_host:
        print("  ERREUR: primary introuvable")
        return None

    for _ in range(5):
        write_probe(primary_host)
    count_pre = count_rows(primary_host)
    primary_pod = get_primary_pod()
    print("  Primary pod: " + str(primary_pod) + " (" + primary_host + ")")
    print("  Rows avant: " + str(count_pre))

    t0 = time.perf_counter()
    subprocess.run(
        ["kubectl", "delete", "pod", primary_pod, "-n", NAMESPACE,
         "--grace-period=0", "--force"],
        capture_output=True
    )
    print("  Pod supprime, attente recuperation...")

    time.sleep(2)
    rto, new_host = wait_recovery(t0)

    if rto is None:
        print("  TIMEOUT — tentative restart GR sur tous les pods...")
        pods_result = subprocess.run(
            ["kubectl", "get", "pods", "-n", NAMESPACE, "-l", "app=mysql",
             "-o", "jsonpath={.items[*].metadata.name}"],
            capture_output=True, text=True
        )
        for pod in pods_result.stdout.strip().split():
            if pod != primary_pod:
                restart_gr_on_pod(pod)
        time.sleep(20)
        rto, new_host = wait_recovery(t0)

    if rto is None:
        print("  TIMEOUT definitif")
        return {"test_id": test_id, "rto_seconds": None, "rpo": None, "success": False}

    count_after = count_rows(new_host)
    rpo = max(0, (count_pre or 0) - (count_after or 0))

    print("  RTO = " + str(round(rto, 2)) + "s")
    print("  RPO = " + str(rpo))
    print("  Nouveau primary: " + str(new_host))

    result = {"test_id": test_id, "primary_deleted": primary_pod,
              "new_primary": new_host, "rto_seconds": round(rto, 3),
              "rpo": rpo, "success": True}

    # Attendre que le pod supprimé revienne et rejoigne GR
    print("  Attente rejointe du pod redemarré (30s)...")
    time.sleep(30)

    # Restart GR sur le pod revenu si nécessaire
    pods_result = subprocess.run(
        ["kubectl", "get", "pods", "-n", NAMESPACE, "-l", "app=mysql",
         "-o", "jsonpath={.items[*].metadata.name}"],
        capture_output=True, text=True
    )
    for pod in pods_result.stdout.strip().split():
        try:
            r = subprocess.run(
                ["kubectl", "exec", pod, "-n", NAMESPACE, "--",
                 "mysql", "-uroot", "-phabench2024", "-sNe",
                 "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_HOST=@@report_host"],
                capture_output=True, text=True, timeout=10
            )
            if "ONLINE" not in r.stdout and "RECOVERING" not in r.stdout:
                print("  Restart GR sur " + pod)
                restart_gr_on_pod(pod)
                time.sleep(15)
        except:
            pass

    return result

if __name__ == "__main__":
    print("HABench -- MySQL GR RTO/RPO Benchmark")
    print("N tests: " + str(N_TESTS))
    print("Started: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    print("\nTest connexion...")
    primary_host = get_primary_host()
    if not primary_host:
        print("ERREUR: pas de primary trouvé")
        exit(1)
    print("Connexion OK! Primary: " + primary_host)

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
            "db": "mysql_gr", "n_tests": N_TESTS,
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
        summary = {"error": "not enough data", "rtos": rtos}

    with open("/tmp/mysql_rto_raw.csv", "w", newline="") as f:
        if results:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)

    with open("/tmp/mysql_rto_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("\n========== RTO SUMMARY ==========")
    for k, v in summary.items():
        print("  " + str(k).ljust(15) + ": " + str(v))
    print("\nResultats: /tmp/mysql_rto_raw.csv / /tmp/mysql_rto_summary.json")
