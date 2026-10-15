import psycopg2
import subprocess
import time
import uuid
import statistics
import json
import csv
import os
import signal
from datetime import datetime

CONN_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'app',
    'user': 'app',
    'password': 'iUGFuqCXQ9hAr5wb1CS3akB7GLLt4TcYMBRiyE99zOtqnS2YKPwaNB8xLcJHNBwS'
}

NAMESPACE = "habench"
CLUSTER   = "pg-habench"
N_TESTS   = 30
TIMEOUT   = 120

pf_process = None


def start_portforward():
    global pf_process
    stop_portforward()
    time.sleep(2)
    pf_process = subprocess.Popen(
        ["kubectl", "port-forward", "svc/pg-habench-rw", "5432:5432", "-n", NAMESPACE],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    time.sleep(3)
    print("  [PF] Port-forward redemarre (PID=" + str(pf_process.pid) + ")")


def stop_portforward():
    global pf_process
    if pf_process:
        try:
            pf_process.terminate()
            pf_process.wait(timeout=5)
        except Exception:
            pass
        pf_process = None


def get_primary_pod():
    cmd = ["kubectl", "get", "cluster", CLUSTER, "-n", NAMESPACE,
           "-o", "jsonpath={.status.currentPrimary}"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout.strip()


def wait_cluster_healthy():
    print("  Attente cluster healthy...")
    for _ in range(60):
        cmd = ["kubectl", "get", "cluster", CLUSTER, "-n", NAMESPACE,
               "-o", "jsonpath={.status.phase}"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if "Cluster in healthy state" in result.stdout or result.stdout.strip() == "":
            time.sleep(3)
            break
        time.sleep(2)


def write_probe():
    try:
        conn = psycopg2.connect(**CONN_PARAMS, connect_timeout=5)
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO bench_writes (test_uuid, payload, mode) VALUES (%s, %s, %s)",
                (str(uuid.uuid4()), 'x' * 200, 'rto_probe')
            )
        conn.commit()
        conn.close()
        return True
    except Exception:
        return False


def count_rows():
    try:
        conn = psycopg2.connect(**CONN_PARAMS, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bench_writes")
            count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return None


def wait_for_recovery(t_start):
    while time.perf_counter() - t_start < TIMEOUT:
        if write_probe():
            return time.perf_counter() - t_start
        time.sleep(0.5)
    return None


def run_rto_test(test_id):
    print("\n  Test " + str(test_id) + "/" + str(N_TESTS))

    # Ecrire quelques lignes avant failover
    for _ in range(5):
        write_probe()
    count_pre = count_rows()
    print("  Transactions avant: " + str(count_pre))

    # Identifier le primary
    primary = get_primary_pod()
    if not primary:
        print("  ERREUR: primary introuvable")
        return None
    print("  Primary: " + primary)

    # Supprimer le primary + stopper le port-forward
    t_delete = time.perf_counter()
    stop_portforward()
    subprocess.run(
        ["kubectl", "delete", "pod", primary, "-n", NAMESPACE,
         "--grace-period=0", "--force"],
        capture_output=True
    )
    print("  Pod supprime, attente election nouveau primary...")

    # Attendre que Kubernetes elise un nouveau primary (15-20s)
    time.sleep(15)

    # Relancer le port-forward vers le nouveau primary
    start_portforward()

    # Mesurer RTO depuis t_delete
    rto = wait_for_recovery(t_delete)

    if rto is None:
        print("  TIMEOUT apres " + str(TIMEOUT) + "s")
        return {
            'test_id': test_id,
            'primary_deleted': primary,
            'new_primary': '',
            'rto_seconds': None,
            'rpo_transactions': None,
            'success': False
        }

    count_after = count_rows()
    rpo = max(0, (count_pre or 0) - (count_after or 0))
    new_primary = get_primary_pod()

    print("  RTO = " + str(round(rto, 2)) + "s")
    print("  RPO = " + str(rpo) + " transactions perdues")
    print("  Nouveau primary: " + str(new_primary))

    result = {
        'test_id':          test_id,
        'primary_deleted':  primary,
        'new_primary':      new_primary,
        'rto_seconds':      round(rto, 3),
        'rpo_transactions': rpo,
        'success':          True
    }

    # Stabilisation
    print("  Stabilisation 30s...")
    time.sleep(30)
    return result


if __name__ == '__main__':
    print("HABench -- PostgreSQL RTO/RPO Benchmark")
    print("N tests: " + str(N_TESTS))
    print("Started: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("Duree estimee: ~" + str(N_TESTS * 50 // 60) + " minutes")

    # Demarrer le port-forward initial
    start_portforward()

    # Attendre connexion initiale
    print("\nTest connexion initiale...")
    for _ in range(10):
        if write_probe():
            print("Connexion OK!")
            break
        time.sleep(2)
    else:
        print("ERREUR: impossible de se connecter. Verifier le cluster.")
        stop_portforward()
        exit(1)

    results = []
    try:
        for i in range(1, N_TESTS + 1):
            r = run_rto_test(i)
            if r:
                results.append(r)
    finally:
        stop_portforward()

    # Statistiques
    successful = [r for r in results if r['success'] and r['rto_seconds'] is not None]
    rtos = [r['rto_seconds'] for r in successful]
    rpos = [r['rpo_transactions'] for r in successful]

    if len(rtos) > 1:
        import numpy as np
        from scipy import stats as st
        mean_rto = statistics.mean(rtos)
        std_rto  = statistics.stdev(rtos)
        ci = st.t.interval(0.95, df=len(rtos)-1, loc=mean_rto, scale=st.sem(rtos))
        summary = {
            'db':           'postgresql',
            'n_tests':      N_TESTS,
            'n_success':    len(successful),
            'mean_rto':     round(mean_rto, 3),
            'std_rto':      round(std_rto, 3),
            'min_rto':      round(min(rtos), 3),
            'max_rto':      round(max(rtos), 3),
            'ci_low_95':    round(ci[0], 3),
            'ci_high_95':   round(ci[1], 3),
            'mean_rpo':     round(statistics.mean(rpos), 2),
            'max_rpo':      max(rpos),
            'success_rate': str(len(successful)) + "/" + str(N_TESTS)
        }
    else:
        summary = {'error': 'not enough data', 'rtos': rtos}

    with open('pg_rto_raw.csv', 'w', newline='') as f:
        if results:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)

    with open('pg_rto_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)

    print("\n========== RTO SUMMARY ==========")
    for k, v in summary.items():
        print("  " + str(k).ljust(15) + ": " + str(v))
    print("\nResultats: pg_rto_raw.csv / pg_rto_summary.json")