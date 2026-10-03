// Linux live journal polling with an external lease owner and slow HTTP readers.
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, writeFile, appendFile, readFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
const report = resolve(process.env.QCG_LIVE_BENCHMARK_REPORT ?? 'docs/live-sse-benchmark.json');
const binary = resolve(process.env.QCG_HTTP_BINARY ?? 'target/release/qcg');
const root = await mkdtemp(join(tmpdir(), 'qcg-live-benchmark-'));
const rows = [];
const counts = process.env.QCG_LIVE_SMOKE ? [100] : [100, 1000, 10000];
const sizes = process.env.QCG_LIVE_SMOKE ? [1] : [1, 16, 64];
const readers = process.env.QCG_LIVE_SMOKE ? [8] : [1, 8, 64];
const modes = ['exclusive', 'shared-filesystem'];
let server, owner;
const event = (id, seq, kind, data = {}) => ({run_id:id,seq,t:kind,ts:'2026-10-03T00:00:00Z',trace_id:'a'.repeat(32),span_id:seq.toString(16).padStart(16,'0'),...data});
const started = id => event(id,1,'run_started',{generator:'bench',generator_path:'bench',contract_sha256:'a'.repeat(64),inputs:{},resource_hashes:[],schema_version:1});
const encode = value => JSON.stringify(value)+'\n';
const percentile = (values,p) => [...values].sort((a,b)=>a-b)[Math.min(values.length-1,Math.floor(values.length*p))];
async function counters() {
  const status = await readFile(`/proc/${server.pid}/status`,'utf8');
  const info = await readFile(`/proc/${server.pid}/stat`,'utf8');
  const fields = info.slice(info.lastIndexOf(')')+2).split(' ');
  const io = await readFile(`/proc/${server.pid}/io`,'utf8');
  return {rss_kib:Number(status.match(/^VmHWM:\s+(\d+)/m)[1]),cpu_ticks:Number(fields[11])+Number(fields[12]),read_bytes:Number(io.match(/^rchar:\s+(\d+)/m)[1])};
}
async function stop(child) {
  if (!child || child.exitCode !== null) return;
  const completion = once(child,'exit');
  child.kill('SIGTERM');
  const timeout = setTimeout(()=>child.kill('SIGKILL'),10000);
  try {await completion;} finally {clearTimeout(timeout);}
}
async function consume(base, id, expected) {
  let cursor = 0, events = 0, reconnects = 0, bytes = 0, terminal = false;
  const begin = performance.now();
  const deadline = begin + 180000;
  while (!terminal) {
    if (performance.now() > deadline) throw new Error(`slow reader deadline: seq=${cursor}, reconnects=${reconnects}`);
    const response = await fetch(`${base}/api/runs/${id}/events`,{headers:{'Last-Event-ID':String(cursor)},signal:AbortSignal.timeout(180000)});
    if (!response.ok) throw new Error(`subscription failed ${response.status}`);
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '', retry = false;
    try {
      for (;;) {
        const {done,value} = await reader.read();
        if (done) break;
        bytes += value.length;
        buffer += decoder.decode(value,{stream:true});
        let end;
        while ((end = buffer.indexOf('\n\n')) >= 0) {
          const frame = buffer.slice(0,end);buffer=buffer.slice(end+2);
          const text = frame.split('\n').filter(line=>line.startsWith('data:')).map(line=>line.slice(5).replace(/^ /,'')).join('\n');
          if (!text) continue;
          const value = JSON.parse(text);
          if (value.kind === 'lagged') {retry = true;break;}
          if (value.kind === 'stream_error' || value.kind === 'shutdown') throw new Error(`unexpected control: ${text}`);
          if (value.seq <= cursor) continue;
          if (value.seq !== cursor+1) throw new Error(`event gap: ${cursor} -> ${value.seq}`);
          cursor=value.seq;events++;
          terminal ||= value.kind === 'run_finished';
        }
        if (retry) break;
        // The producer outruns each intentionally delayed consumer.
        await delay(10);
      }
      if (!retry && !terminal) throw new Error(`premature EOF at ${cursor}`);
      if (buffer && !retry) throw new Error('partial SSE frame at EOF');
    } finally {await reader.cancel().catch(()=>{});reader.releaseLock();}
    if (retry) {reconnects++;await delay(10);}
  }
  if (cursor !== expected.seq || events !== expected.seq) throw new Error(`missing events: ${events}/${expected.seq}`);
  return {ms:performance.now()-begin,bytes,events,reconnects};
}
try {
  for (const mode of modes) for (const runs of counts) {
    const directory = join(root,`${mode}-${runs}`);await mkdir(directory);
    let output='';
    server=spawn(binary,['serve','--port','0','--run-store',mode,'--runs-dir',directory,'--max-tracked-runs','10000'],{stdio:['ignore','pipe','pipe'],env:{...process.env,READ_CACHE_MAX_BYTES:String(64*1024*1024),LIVE_EVENT_CHANNEL_CAPACITY:'64',JOURNAL_POLL_INTERVAL_MS:'50',GC_KEEP:'10001',GC_KEEP_FAILED:'10001',GC_INTERVAL_SECS:'3600'}});
    server.stdout.on('data',chunk=>output+=chunk);server.stderr.on('data',chunk=>output+=chunk);
    const startup=performance.now()+30000;
    let base;
    while (!(base=output.match(/listening on (http:\/\/\S+)/)?.[1])) {if(server.exitCode!==null||performance.now()>startup)throw new Error(output);await delay(20);}
    for(let index=0;index<runs-1;index++) {
      const id=`completed-${index}`;const meta=join(directory,id,'meta');await mkdir(meta,{recursive:true});
      await writeFile(join(meta,'journal.jsonl'),encode(started(id))+encode(event(id,2,'run_finished',{status:'success',metrics:{}})));
    }
    for(const mib of sizes) for(const subscribers of readers) {
      const id='bench-live';const meta=join(directory,id,'meta');await mkdir(meta,{recursive:true});
      owner=spawn('python3',['-c','import fcntl,sys; f=open(sys.argv[1],"a+"); fcntl.flock(f,fcntl.LOCK_EX); print("ready",flush=True); sys.stdin.read()',join(meta,'execution.lock')],{stdio:['pipe','pipe','pipe']});
      await Promise.race([once(owner.stdout,'data'),delay(10000).then(()=>{throw new Error('lease acquisition timed out');})]);
      const journal=join(meta,'journal.jsonl');let bytes=encode(started(id)).length;await writeFile(journal,encode(started(id)));
      let seq=2;
      const chunks=[];
      while(bytes+encode(event(id,seq,'run_finished',{status:'success',metrics:{}})).length<mib*1024*1024) {
        const empty=event(id,seq,'budget_charged',{amount:0,node:'bench',padding:''});
        const available=mib*1024*1024-bytes-encode(event(id,seq+1,'run_finished',{status:'success',metrics:{}})).length-encode(empty).length;
        if(available<0)throw new Error('fixture boundary invalid');
        const text=encode({...empty,padding:'x'.repeat(Math.min(available,512*1024))});chunks.push(text);bytes+=text.length;seq++;
      }
      const terminal=encode(event(id,seq,'run_finished',{status:'success',metrics:{}}));
      if(bytes+terminal.length!==mib*1024*1024)throw new Error('journal size mismatch');
      const before=await counters();
      const expected={seq};
      const pending=Array.from({length:subscribers},()=>consume(base,id,expected));
      // Subscribers connect to an open run before data is appended.
      await delay(200);
      for(const text of chunks) {await appendFile(journal,text);await delay(5);}
      await appendFile(journal,terminal);
      const result=await Promise.all(pending);
      const after=await counters();
      const memoryBoundKiB=512*1024;
      if(after.rss_kib>memoryBoundKiB)throw new Error(`live stream memory exceeds measurement bound: ${after.rss_kib}`);
      const snapshot=await (await fetch(`${base}/api/runs/${id}`,{signal:AbortSignal.timeout(15000)})).json();
      if(snapshot.state!=='succeeded')throw new Error(JSON.stringify(snapshot));
      rows.push({mode,runs,journal_mib:mib,subscribers,consumer_delay_ms:10,producer_delay_ms:5,channel_capacity:64,cache_limit_bytes:64*1024*1024,process_rss_bound_kib:memoryBoundKiB,p50_ms:percentile(result.map(row=>row.ms),.5),p95_ms:percentile(result.map(row=>row.ms),.95),reconnects:result.reduce((sum,row)=>sum+row.reconnects,0),event_seq:seq,wire_bytes:result.reduce((sum,row)=>sum+row.bytes,0),read_bytes:after.read_bytes-before.read_bytes,cpu_ticks:after.cpu_ticks-before.cpu_ticks,process_max_rss_kib:after.rss_kib});
      await writeFile(report,JSON.stringify(rows,null,2)+'\n');
      console.log(`${mode}/${runs}/${mib} MiB/${subscribers}: completed`);
      await stop(owner);owner=null;
      await rm(join(directory,id),{recursive:true});
    }
    await stop(server);server=null;
  }
} finally {await stop(owner);await stop(server);await rm(root,{recursive:true,force:true});}
