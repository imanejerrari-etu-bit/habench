// HABench - MongoDB Latency Benchmark (mongosh compatible)
const N_INSERTIONS = 1000;
const N_RUNS = 10;
const WARMUP = 50;
const DB_NAME = "habench_db";
const COL_NAME = "bench_writes";

function runBench(modeName, w) {
    print("\n[" + modeName.toUpperCase() + "] w=" + w);
    let allLats = [];
    let habenchDb = db.getSiblingDB(DB_NAME);

    for (let run = 0; run < N_RUNS; run++) {
        // Warmup
        for (let i = 0; i < WARMUP; i++) {
            habenchDb.runCommand({
                insert: COL_NAME,
                documents: [{uuid: UUID().toString(), payload: "x".repeat(200), mode: modeName, ts: new Date()}],
                writeConcern: {w: w, wtimeout: 30000}
            });
        }
        // Mesures
        let runLats = [];
        for (let i = 0; i < N_INSERTIONS; i++) {
            let t0 = new Date().getTime();
            habenchDb.runCommand({
                insert: COL_NAME,
                documents: [{uuid: UUID().toString(), payload: "x".repeat(200), mode: modeName, ts: new Date()}],
                writeConcern: {w: w, wtimeout: 30000}
            });
            runLats.push(new Date().getTime() - t0);
        }
        allLats = allLats.concat(runLats);
        let mean = runLats.reduce((a,b) => a+b, 0) / runLats.length;
        print("  Run " + (run+1) + "/" + N_RUNS + ": mean=" + mean.toFixed(2) + "ms");
    }

    allLats.sort((a,b) => a-b);
    let n = allLats.length;
    let mean = allLats.reduce((a,b) => a+b, 0) / n;
    let variance = allLats.reduce((s,x) => s + (x-mean)**2, 0) / (n-1);

    let result = {
        db: "mongodb", mode: modeName, w: String(w), n: n,
        mean_ms: Math.round(mean * 1000) / 1000,
        std_ms:  Math.round(Math.sqrt(variance) * 1000) / 1000,
        p50_ms:  allLats[Math.floor(n * 0.50)],
        p95_ms:  allLats[Math.floor(n * 0.95)],
        p99_ms:  allLats[Math.floor(n * 0.99)],
    };
    print("  RESULT: mean=" + result.mean_ms + "ms P95=" + result.p95_ms + "ms P99=" + result.p99_ms + "ms");
    return result;
}

print("=== HABench MongoDB Latency Benchmark ===");
print("Insertions: " + N_INSERTIONS + " x " + N_RUNS + " runs per mode");

// Verifier PRIMARY
let isMaster = db.runCommand({isMaster: 1});
if (!isMaster.ismaster) {
    print("ERREUR: Ce noeud n'est pas PRIMARY! Primary = " + isMaster.primary);
    quit(1);
}
print("Node role: PRIMARY");
rs.status().members.forEach(m => print("  " + m.name + " -> " + m.stateStr));

let modes = [
    {name: "async",     w: 1},
    {name: "semi_sync", w: 2},
    {name: "sync",      w: "majority"}
];

let results = [];
modes.forEach(m => results.push(runBench(m.name, m.w)));

let asyncMean = results.find(r => r.mode === "async").mean_ms;
results.forEach(r => {
    r.overhead_pct = Math.round(((r.mean_ms - asyncMean) / asyncMean) * 1000) / 10;
});

print("\n========== FINAL RESULTS ==========");
print("Mode         Mean(ms)   P95(ms)   P99(ms)   Overhead");
print("-".repeat(55));
results.forEach(r => {
    print(r.mode.padEnd(12) + String(r.mean_ms).padStart(10) +
          String(r.p95_ms).padStart(10) + String(r.p99_ms).padStart(10) +
          (String(r.overhead_pct) + "%").padStart(10));
});

db.getSiblingDB(DB_NAME).bench_results.insertOne({
    type: "latency", db: "mongodb", ts: new Date(), results: results
});
print("\nJSON:" + JSON.stringify(results));
