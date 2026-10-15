import psycopg2
import time
import uuid
import statistics
import json
import csv
from datetime import datetime

CONN_PARAMS = {
    'host': 'pg-habench-rw.habench.svc.cluster.local',
    'port': 5432,
    'database': 'app',
    'user': 'app',
    'password': 'FPB1Q51OfjcvyAO9FesEoqzS837LyjLpI7PNPRCtDJNAwmVKW6MGoFzqf3sRXWUE'
}

N_TESTS = 30
TIMEOUT = 120

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

    for _ in range(5):
        write_probe()
    count_pre = count_rows()
    print("  Rows avant: " + str(count_pre))

    # Attendre que l'utilisateur supprime le pod primary
    print("  >>> SUPPRIMER LE PRIMARY MAINTENANT dans l'autre terminal <<<")
    print("  >>> kubectl delete pod $(kubectl get cluster pg-habench -n habench -o jsonpath='{.status.currentPrimary}') -n habench --grace-period=0 --force <<<")
    print("  En attente de perte de connexion...")

    # Detecter la perte de connexion
    t_start = time.perf_counter()
    lost = False
    while time.perf_counter() - t_start < 60:
        if not write_probe():
            lost = True
            break
        time.sleep(0.3)

    if not lost:
        print("  SKIP: connexion jamais perdue (pod pas supprime?)")
        return None

    t_lost = time.perf_counter()
    print("  Connexion perdue! Mesure RTO...")

    # Mesurer le temps jusqu'a recuperation
    rto = wait_for_recovery(t_start)

    if rto is None:
        print("  TIMEOUT: service non restaure apres " + str(TIMEOUT) + "s")
        return {'test_id': test_id, 'rto_seconds': None,
                'rpo_transactions': None, 'success': False}

    count_after = count_rows()
    rpo = max(0, (count_pre or 0) - (count_after or 0))

    print("  RTO = " + str(round(rto, 2)) + "s")
    print("  RPO = " + str(rpo) + " transactions perdues")

    result = {
        'test_id': test_id,
        'rto_seconds': round(rto, 3),
        'rpo_transactions': rpo,
        'success': True
    }

    print("  Stabilisation 30s...")
    time.sleep(30)
    return result

if __name__ == '__main__':
    print("HABench -- PostgreSQL RTO/RPO Benchmark (in-pod)")
    print("N tests: " + str(N_TESTS))
    print("Started: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("")
    print("INSTRUCTIONS:")
    print("  Garder ce terminal ouvert.")
    print("  Dans un 2eme terminal PowerShell, supprimer le primary quand demande:")
    print("  kubectl delete pod <primary-pod> -n habench --grace-period=0 --force")
    print("")

    conn = psycopg2.connect(**CONN_PARAMS, connect_timeout=10)
    conn.close()
    print("Connexion OK! Debut des tests...\n")

    results = []
    for i in range(1, N_TESTS + 1):
        r = run_rto_test(i)
        if r:
            results.append(r)

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
            'db': 'postgresql', 'n_tests': N_TESTS,
            'n_success': len(successful),
            'mean_rto': round(mean_rto, 3),
            'std_rto': round(std_rto, 3),
            'min_rto': round(min(rtos), 3),
            'max_rto': round(max(rtos), 3),
            'ci_low_95': round(ci[0], 3),
            'ci_high_95': round(ci[1], 3),
            'mean_rpo': round(statistics.mean(rpos), 2),
            'max_rpo': max(rpos),
            'success_rate': str(len(successful)) + "/" + str(N_TESTS)
        }
    else:
        summary = {'error': 'not enough data', 'rtos': rtos}

    with open('/tmp/pg_rto_raw.csv', 'w', newline='') as f:
        if results:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)

    with open('/tmp/pg_rto_summary.json', 'w') as f:
        json.dump(summary, f, indent=2)

    print("\n========== RTO SUMMARY ==========")
    for k, v in summary.items():
        print("  " + str(k).ljust(15) + ": " + str(v))
    print("\nResultats: /tmp/pg_rto_raw.csv / /tmp/pg_rto_summary.json")