import mysql.connector, time, uuid, statistics, json, csv, subprocess
from datetime import datetime
NS='habench'
N=30
PODS=['mysql-0','mysql-1','mysql-2']
C={'port':3306,'database':'habench_db','user':'root','password':'habench2024'}

def qry(pod, sql, sk=False, to=15):
    pw = ['--skip-password'] if sk else ['-phabench2024']
    cmd=['kubectl','exec',pod,'-n',NS,'--','mysql','-uroot']+pw+['-sNe',sql]
    try:
        r=subprocess.run(cmd,capture_output=True,text=True,timeout=to)
        return r.stdout.strip()
    except: return None

def get_role(pod):
    for sk in [False,True]:
        o=qry(pod,'SELECT MEMBER_STATE,MEMBER_ROLE FROM performance_schema.replication_group_members WHERE MEMBER_HOST=@@report_host',sk,8)
        if o:
            p=o.split()
            if len(p)>=2: return p[0],p[1]
    return None,None

def get_primary():
    cmd=['kubectl','get','pods','-n',NS,'-l','app=mysql','-o','jsonpath={.items[*].metadata.name}']
    r=subprocess.run(cmd,capture_output=True,text=True)
    for p in r.stdout.strip().split():
        s,ro=get_role(p)
        if ro=='PRIMARY' and s=='ONLINE': return p
    return None

def get_running():
    cmd=['kubectl','get','pods','-n',NS,'-l','app=mysql','-o','jsonpath={.items[*].metadata.name}']
    r=subprocess.run(cmd,capture_output=True,text=True)
    return r.stdout.strip().split()

def probe(pod):
    sql="INSERT INTO habench_db.bench_writes(test_uuid,payload,mode) VALUES('"+str(uuid.uuid4())+"','xx','rto')"
    return qry(pod,sql,to=8) is not None

def cnt(pod):
    o=qry(pod,'SELECT COUNT(*) FROM habench_db.bench_writes',to=8)
    try: return int(o)
    except: return None

def bootstrap(pod):
    print('  Bootstrap '+pod)
    for sk in [False,True]:
        qry(pod,'SET GLOBAL super_read_only=OFF',sk,10)
        qry(pod,'SET GLOBAL read_only=OFF',sk,10)
        qry(pod,'STOP GROUP_REPLICATION',sk,15)
        qry(pod,'SET GLOBAL group_replication_bootstrap_group=ON',sk,10)
        qry(pod,'START GROUP_REPLICATION',sk,30)
        qry(pod,'SET GLOBAL group_replication_bootstrap_group=OFF',sk,10)
        time.sleep(7)
        s,ro=get_role(pod)
        if ro=='PRIMARY': print('  Bootstrap OK '+pod); return True
    return False

def rejoin(pod):
    print('  Rejoin '+pod)
    for sk in [False,True]:
        qry(pod,'SET GLOBAL super_read_only=OFF',sk,10)
        qry(pod,'SET GLOBAL read_only=OFF',sk,10)
        qry(pod,'STOP GROUP_REPLICATION',sk,15)
        qry(pod,'RESET MASTER',sk,10)
        qry(pod,"CHANGE MASTER TO MASTER_USER='repl',MASTER_PASSWORD='repl2024' FOR CHANNEL 'group_replication_recovery'",sk,10)
        qry(pod,'START GROUP_REPLICATION',sk,30)
        time.sleep(8)
        s,ro=get_role(pod)
        if s in('ONLINE','RECOVERING'): return True
    return False

def run_test(tid):
    print('\nTest '+str(tid)+'/'+str(N))
    pri=get_primary()
    if not pri:
        bootstrap('mysql-0'); time.sleep(12); pri=get_primary()
    if not pri: return None
    for _ in range(5): probe(pri)
    pre=cnt(pri)
    surv=[p for p in PODS if p!=pri]
    print('  Primary:'+pri+' Rows:'+str(pre))
    t0=time.perf_counter()
    subprocess.run(['kubectl','delete','pod',pri,'-n',NS,'--grace-period=0','--force'],capture_output=True,timeout=15)
    time.sleep(5)
    np2=None; rto=None
    for att in range(25):
        running=get_running()
        for p in surv:
            if p in running:
                s,ro=get_role(p)
                if ro=='PRIMARY' and s=='ONLINE' and probe(p):
                    rto=time.perf_counter()-t0; np2=p; break
        if np2: break
        if att==3:
            for p in surv:
                if p in get_running(): bootstrap(p); break
        time.sleep(3)
    if not np2: print('  TIMEOUT'); return {'test_id':tid,'success':False,'rto_seconds':None,'rpo':None}
    post=cnt(np2); rpo=max(0,(pre or 0)-(post or 0))
    print('  RTO='+str(round(rto,3))+'s RPO='+str(rpo)+' new='+np2)
    time.sleep(45)
    if pri in get_running(): rejoin(pri); time.sleep(15)
    return {'test_id':tid,'primary_deleted':pri,'new_primary':np2,'rto_seconds':round(rto,3),'rpo':rpo,'success':True}

print('HABench MySQL GR RTO v2 -- '+datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
p=get_primary()
if not p: bootstrap('mysql-0'); time.sleep(15); p=get_primary()
if not p: print('ERR cluster'); exit(1)
print('Primary: '+p)
qry(p,'CREATE TABLE IF NOT EXISTS habench_db.bench_writes(id INT AUTO_INCREMENT PRIMARY KEY,test_uuid VARCHAR(36),payload TEXT,mode VARCHAR(50),ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
res=[]
for i in range(1,N+1):
    r=run_test(i)
    if r: res.append(r)
ok=[r for r in res if r.get('success') and r.get('rto_seconds')]
rtos=[r['rto_seconds'] for r in ok]
rpos=[r['rpo'] for r in ok]
if len(rtos)>=2:
    from scipy import stats as st
    m=statistics.mean(rtos); s=statistics.stdev(rtos)
    ci=st.t.interval(0.95,df=len(rtos)-1,loc=m,scale=st.sem(rtos))
    sm={'db':'mysql_gr','n_success':len(ok),'mean_rto':round(m,3),'std_rto':round(s,3),'min_rto':round(min(rtos),3),'max_rto':round(max(rtos),3),'ci_low':round(ci[0],3),'ci_high':round(ci[1],3),'mean_rpo':round(statistics.mean(rpos),2),'max_rpo':max(rpos),'rate':str(len(ok))+'/'+str(N)}
else: sm={'err':'not enough data','n':len(ok),'rtos':rtos}
json.dump(sm,open('/tmp/mysql_rto_summary.json','w'),indent=2)
if res:
    w=csv.DictWriter(open('/tmp/mysql_rto_raw.csv','w',newline=''),fieldnames=res[0].keys())
    w.writeheader(); w.writerows(res)
print('\n===SUMMARY===')
for k,v in sm.items(): print('  '+str(k)+': '+str(v))
