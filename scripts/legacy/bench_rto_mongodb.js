// HABench - MongoDB RTO/RPO Benchmark
// Usage: kubectl exec -it mongodb-0 -n habench -- mongosh /tmp/bench_rto.js
// IMPORTANT: Lancer depuis un pod DANS le cluster, pas depuis l'exterieur

const N_TESTS = 30;
const DB_NAME = "habench_db";
const COL_NAME = "bench_writes_rto";

print("=== HABench MongoDB RTO/RPO Benchmark ===");
print("N tests: " + N_TESTS);
print("Duree estimee: ~" + Math.round(N_TESTS * 60 / 60) + " minutes");

let habenchDb = db.getSiblingDB(DB_NAME);

// Fonction pour compter les documents
function countDocs() {
    return habenchDb.getCollection(COL_NAME).countDocuments();
}

// Fonction pour inserer un document
function insertDoc(mode) {
    return habenchDb.runCommand({
        insert: COL_NAME,
        documents: [{uuid: UUID().toString(), payload: "x".repeat(100), mode: mode, ts: new Date()}],
        writeConcern: {w: "majority", wtimeout: 5000}
    });
}

// Verifier qu'on est PRIMARY
let isMaster = db.runCommand({isMaster: 1});
if (!isMaster.ismaster) {
    print("ERREUR: Lancer depuis le PRIMARY!");
    quit(1);
}
print("Node: PRIMARY ✓\n");

let results = [];

for (let test = 1; test <= N_TESTS; test++) {
    print("Test " + test + "/" + N_TESTS);

    // Ecrire avant failover
    for (let i = 0; i < 5; i++) insertDoc("pre_failover");
    let countBefore = countDocs();
    print("  Docs avant: " + countBefore);

    // Noter le temps de début
    let t0 = new Date().getTime();
    print("  Failover simulate a t=0 (supprimer le pod PRIMARY depuis un autre terminal)");
    print("  Commande: kubectl delete pod mongodb-0 -n habench --grace-period=0 --force");
    print("  En attente de perte de connexion...");

    // Attendre que la connexion soit perdue (simulation du failover externe)
    // Dans ce script on mesure le temps de recuperation depuis t0
    // Le pod doit etre supprime manuellement depuis un autre terminal

    // Pour automatiser: on utilise une boucle d'attente
    let lost = false;
    let t_lost = null;
    let maxWait = 30000; // 30s max pour detecter la perte

    // Tenter des insertions jusqu'a echec
    let attempts = 0;
    while (attempts < 60) {
        try {
            let r = habenchDb.runCommand({
                insert: COL_NAME,
                documents: [{uuid: UUID().toString(), payload: "probe", mode: "probe", ts: new Date()}],
                writeConcern: {w: "majority", wtimeout: 2000}
            });
            if (r.ok !== 1) {
                t_lost = new Date().getTime();
                lost = true;
                break;
            }
        } catch(e) {
            t_lost = new Date().getTime();
            lost = true;
            break;
        }
        sleep(500);
        attempts++;
    }

    if (!lost) {
        print("  SKIP: pas de failover detecte (supprimer le pod manuellement)");
        continue;
    }

    print("  Connexion perdue a t=" + (t_lost - t0) + "ms");

    // Attendre recuperation
    let recovered = false;
    let t_recovered = null;
    for (let i = 0; i < 240; i++) { // 120s max
        try {
            let r = habenchDb.runCommand({
                insert: COL_NAME,
                documents: [{uuid: UUID().toString(), payload: "recovery", mode: "recovery", ts: new Date()}],
                writeConcern: {w: 1, wtimeout: 2000}
            });
            if (r.ok === 1) {
                t_recovered = new Date().getTime();
                recovered = true;
                break;
            }
        } catch(e) {}
        sleep(500);
    }

    if (!recovered) {
        print("  TIMEOUT: service non restaure");
        results.push({test: test, success: false, rto: null, rpo: null});
        continue;
    }

    let rto = (t_recovered - t0) / 1000;
    let countAfter = countDocs();
    let rpo = Math.max(0, countBefore - countAfter);

    print("  RTO = " + rto.toFixed(2) + "s");
    print("  RPO = " + rpo + " docs perdus");

    results.push({test: test, success: true, rto_seconds: rto, rpo: rpo});

    // Stabilisation
    print("  Stabilisation 30s...");
    sleep(30000);
}

// Statistiques
let successful = results.filter(r => r.success && r.rto_seconds != null);
let rtos = successful.map(r => r.rto_seconds);

if (rtos.length > 0) {
    let mean = rtos.reduce((a,b) => a+b, 0) / rtos.length;
    let sorted = [...rtos].sort((a,b) => a-b);
    let variance = rtos.reduce((s,x) => s + (x-mean)**2, 0) / (rtos.length-1);
    let std = Math.sqrt(variance);

    print("\n========== RTO SUMMARY ==========");
    print("n_success   : " + rtos.length + "/" + N_TESTS);
    print("mean_rto    : " + mean.toFixed(3) + "s");
    print("std_rto     : " + std.toFixed(3) + "s");
    print("min_rto     : " + sorted[0].toFixed(3) + "s");
    print("max_rto     : " + sorted[sorted.length-1].toFixed(3) + "s");
    print("mean_rpo    : " + (successful.reduce((s,r) => s+r.rpo, 0) / successful.length).toFixed(2));

    // Sauvegarder
    db.getSiblingDB(DB_NAME).bench_results.insertOne({
        type: "rto", db: "mongodb", ts: new Date(),
        summary: {n: rtos.length, mean_rto: mean, std_rto: std, min_rto: sorted[0], max_rto: sorted[sorted.length-1]},
        raw: results
    });
    print("Resultats sauvegardes dans habench_db.bench_results");
}
