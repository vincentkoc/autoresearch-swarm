// Real Chrome, synthetic graph only. Node 22+, no npm/CDN dependencies.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {mkdtemp, readFile, writeFile, mkdir, rm} from 'node:fs/promises';
import {join, resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {pathToFileURL} from 'node:url';
import {setTimeout as delay} from 'node:timers/promises';

const output=resolve(process.env.GRAPH_OUTPUT_DIR || 'artifacts/experiment-graph');
const profile=await mkdtemp(join(tmpdir(),'swarm-force-graph-'));
let chrome,cdp,exited;
const pending=new Map(),errors=[];
let seq=0;
async function wait(check,label) {
  const deadline=Date.now()+15000;
  while(Date.now()<deadline) {const result=await check();if(result)return result;await delay(80);}
  throw new Error('Timeout: '+label);
}
async function send(method,params={}) {
  const id=++seq;
  let timer;
  try {
    return await Promise.race([new Promise((resolve,reject)=>{
      pending.set(id,{resolve,reject});cdp.send(JSON.stringify({id,method,params}));
    }),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error(method+' timed out')),10000);})]);
  } finally {clearTimeout(timer);pending.delete(id);}
}
async function evaluate(expression) {
  const result=await send('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});
  assert.equal(result.exceptionDetails,undefined,JSON.stringify(result.exceptionDetails));
  return result.result.value;
}
async function screenshot(name) {
  const {data}=await send('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});
  await writeFile(join(output,name),Buffer.from(data,'base64'));
}
async function load(name,width=1440,height=900) {
  await send('Emulation.setDeviceMetricsOverride',{width,height,deviceScaleFactor:1,mobile:width<500});
  await send('Page.navigate',{url:pathToFileURL(join(output,name)).href});
  await wait(()=>evaluate('document.readyState==="complete" && !!document.getElementById("count")?.textContent'),
    'graph render');
  await evaluate('new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)))');
}
async function click(selector) {return evaluate('document.querySelector('+JSON.stringify(selector)+').click()');}
try {
  await mkdir(output,{recursive:true});
  // The existing synthetic test fixture exercises 5,000 independently captured nodes.
  const fixture=spawn('.venv/bin/python',['-c',
    'import runpy; from pathlib import Path; from research_swarm.control.experiment_graph import render_experiment_graph_html; '+
    'fixture=runpy.run_path("tests/control/test_experiment_graph.py")["dataset"]; '+
    'out=Path('+JSON.stringify(output)+'); '+
    '[(out/f"synthetic-{n}.html").write_text(render_experiment_graph_html(fixture(n)), encoding="utf-8") for n in (0,240,5000)]; isolated=fixture(12); isolated["edges"]=isolated["edges"][:1]; (out/"synthetic-separated.html").write_text(render_experiment_graph_html(isolated))'
  ],{stdio:'inherit'});
  assert.equal(await new Promise((r,j)=>{fixture.on('exit',r);fixture.on('error',j);}),0);
  const atlas=spawn('.venv/bin/python',['examples/graph_inventory/build.py','--output',join(output,'atlas')],{stdio:'inherit'});
  assert.equal(await new Promise((r,j)=>{atlas.on('exit',r);atlas.on('error',j);}),0);
  chrome=spawn(process.env.CHROME_BIN || 'google-chrome',[
    '--headless','--disable-gpu','--no-first-run','--no-default-browser-check',
    '--disable-background-networking','--remote-debugging-address=127.0.0.1',
    '--remote-debugging-port=0','--user-data-dir='+profile,'about:blank'
  ],{stdio:['ignore','ignore','pipe']});
  let stderr='',startupError;
  chrome.on('error',e=>{startupError=e;});
  chrome.stderr.on('data',c=>{stderr=(stderr+c.toString()).slice(-4000);});
  exited=new Promise(r=>chrome.on('close',r));
  const port=await wait(async()=>{
    if(startupError)throw startupError;
    if(chrome.exitCode!==null)throw new Error(stderr);
    try{return Number((await readFile(join(profile,'DevToolsActivePort'),'utf8')).split('\n')[0]);}
    catch(e){if(e.code!=='ENOENT')throw e;return false;}
  },'Chrome startup');
  const targets=await (await fetch('http://127.0.0.1:'+port+'/json/list')).json();
  cdp=new WebSocket(targets.find(t=>t.type==='page').webSocketDebuggerUrl);
  await new Promise((r,j)=>{cdp.addEventListener('open',r,{once:true});cdp.addEventListener('error',j,{once:true});});
  cdp.addEventListener('message',({data})=>{
    const message=JSON.parse(data);
    if(message.id && pending.has(message.id)){
      const p=pending.get(message.id);message.error?p.reject(message.error):p.resolve(message.result);
    } else if(message.method==='Runtime.exceptionThrown')errors.push(message.params);
    else if(message.method==='Log.entryAdded' && message.params.entry.level==='error')errors.push(message.params);
  });
  await send('Page.enable');await send('Runtime.enable');await send('Log.enable');
  // Observe drawn node centers, without adding a production debug API.
  await send('Page.addScriptToEvaluateOnNewDocument',{source:
    'window.__dots=[];window.__pageErrors=[];window.__drawCount=0;window.__workers=0;const NativeWorker=Worker;window.Worker=class extends NativeWorker{constructor(...args){super(...args);__workers++;}};'+
    'addEventListener("securitypolicyviolation", e=>__pageErrors.push(e.violatedDirective));'+
    'const clear=CanvasRenderingContext2D.prototype.clearRect,arc=CanvasRenderingContext2D.prototype.arc;'+
    'CanvasRenderingContext2D.prototype.clearRect=function(...a){window.__dots=[];window.__drawCount++;return clear.apply(this,a)};'+
    'CanvasRenderingContext2D.prototype.arc=function(x,y,...a){const t=this.getTransform();'+
    'window.__dots.push({x:t.a*x+t.e,y:t.d*y+t.f});return arc.call(this,x,y,...a)};'
  });
  await load('synthetic-240.html');
  assert.equal(await evaluate('window.__workers'),1,'One offline layout worker');
  assert.equal(await evaluate('document.getElementById("layout").value'),'topology');
  assert.equal(await evaluate('getComputedStyle(document.body).backgroundColor'),'rgb(250, 250, 248)');
  assert.equal(await evaluate('document.querySelectorAll(".node-item").length'),100);
  assert.match(await evaluate('document.getElementById("count").textContent'),/240 \/ 240/);
  await click('#pause');
  assert.equal(await evaluate('document.getElementById("pause").textContent'),'Resume');
  await click('[data-id="experiment-9"]');
  assert.match(await evaluate('document.getElementById("detail").textContent'),/1 causal ancestors/);
  assert.match(await evaluate('document.getElementById("detail").textContent'),/Parent: Experiment 8/);
  await click('[data-id="experiment-8"]');
  assert.match(await evaluate('document.getElementById("detail").textContent'),/0 causal ancestors/);
  assert.match(await evaluate('document.getElementById("detail").textContent'),/Source reference: Experiment 7/);
  // Keep the inspected node selected while search/category visibility changes.
  const connections=()=>evaluate('Array.from(document.querySelectorAll("#detail .connection"), b=>b.textContent)');
  const initialConnections=await connections();
  assert.deepEqual(initialConnections,['Source reference: Experiment 7','Child: Experiment 9']);
  for(const [id,value,event] of [
    ['search','experiment-8','input'],['cluster',JSON.stringify('Category 2'),'change']
  ]) {
    await evaluate('document.getElementById('+JSON.stringify(id)+').value='+JSON.stringify(value)+
      ';document.getElementById('+JSON.stringify(id)+').dispatchEvent(new Event('+JSON.stringify(event)+'))');
    assert.deepEqual(await connections(),initialConnections.map(text=>text+' (filtered)'),
      id+' immediately marks hidden connections');
    assert.equal(await evaluate('document.querySelector("#detail h3").textContent'),'Experiment 8');
    await evaluate('document.getElementById('+JSON.stringify(id)+').value="";'+
      'document.getElementById('+JSON.stringify(id)+').dispatchEvent(new Event('+JSON.stringify(event)+'))');
    assert.deepEqual(await connections(),initialConnections,id+' clearing removes filtered annotations');
  }
  await click('#more');
  assert.equal(await evaluate('document.querySelectorAll(".node-item").length'),200);
  await evaluate('document.getElementById("search").value="experiment-239";document.getElementById("search").dispatchEvent(new Event("input"))');
  assert.match(await evaluate('document.getElementById("count").textContent'),/1 \/ 240/);
  await click('.node-item');
  assert.match(await evaluate('document.getElementById("detail").textContent'),/Experiment 239/);
  await evaluate('document.getElementById("search").value="not present";document.getElementById("search").dispatchEvent(new Event("input"))');
  assert.equal(await evaluate('document.getElementById("empty").hidden'),false);
  await evaluate('document.getElementById("search").value="";document.getElementById("search").dispatchEvent(new Event("input"));document.getElementById("cluster").value=JSON.stringify("Category 2");document.getElementById("cluster").dispatchEvent(new Event("change"))');
  assert.match(await evaluate('document.getElementById("count").textContent'),/40 \/ 240/);
  await evaluate('document.getElementById("cluster").value="";document.getElementById("cluster").dispatchEvent(new Event("change"))');
  await click('#reset');await click('#pause');await click('#fit');
  await evaluate('new Promise(r=>requestAnimationFrame(r))');
  const before=await evaluate('window.__dots[0]');
  const rect=await evaluate('(()=>{const r=document.getElementById("graph").getBoundingClientRect();return {x:r.x,y:r.y}})()');
  const p={x:rect.x+before.x,y:rect.y+before.y};
  await send('Input.dispatchMouseEvent',{type:'mouseMoved',...p});
  assert.equal(await evaluate('document.getElementById("tooltip").hidden'),false);
  await send('Input.dispatchMouseEvent',{type:'mousePressed',...p,button:'left',clickCount:1});
  await send('Input.dispatchMouseEvent',{type:'mouseMoved',x:p.x+30,y:p.y+20,button:'left',buttons:1});
  await send('Input.dispatchMouseEvent',{type:'mouseReleased',x:p.x+30,y:p.y+20,button:'left',clickCount:1});
  await evaluate('new Promise(r=>requestAnimationFrame(r))');
  const after=await evaluate('window.__dots[0]');
  assert.ok(Math.abs(after.x-before.x-30)<1,'Dragging moves node 30 screen pixels');
  assert.match(await evaluate('document.getElementById("detail").textContent'),/Experiment 0/);
  await click('#zoom-in');await evaluate('new Promise(r=>requestAnimationFrame(r))');
  assert.notDeepEqual(await evaluate('window.__dots[0]'),after,'Zoom changes screen position');
  await click('#fit');
  await screenshot('graph-desktop.png');
  // Public README uses the populated synthetic atlas, never a private capture.
  await load('atlas/index.html');
  assert.match(await evaluate('document.getElementById("count").textContent'),/600 \/ 600 records · 2,028 links/);
  await evaluate('document.getElementById("layout").value="ring";document.getElementById("layout").dispatchEvent(new Event("change"))');
  await wait(()=>evaluate('document.getElementById("motion").textContent==="Layout settled"'),'README ring settles');
  await evaluate('new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)))');
  await click('[data-id="area-0-035"]');
  assert.match(await evaluate('document.getElementById("detail").textContent'),/Illustrative supporting record/);
  await evaluate('document.getElementById("graph").focus()');
  await send('Input.dispatchKeyEvent',{type:'keyDown',key:'Escape',code:'Escape'});
  await send('Input.dispatchKeyEvent',{type:'keyUp',key:'Escape',code:'Escape'});
  await evaluate('new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)))');
  await screenshot('graph-ring-example.png');
  await load('synthetic-240.html');
  await evaluate('document.getElementById("layout").value="topology";document.getElementById("layout").dispatchEvent(new Event("change"))');
  await send('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});
  await evaluate('new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)))');
  assert.ok(await evaluate('document.documentElement.scrollWidth<=innerWidth'),'No mobile overflow');
  await screenshot('graph-mobile.png');
  await load('synthetic-0.html');
  assert.equal(await evaluate('document.getElementById("empty").hidden'),false);
  await load('synthetic-separated.html');
  await wait(()=>evaluate('document.getElementById("motion").textContent==="Layout settled"'),'separate components settle');
  await evaluate('new Promise(r=>requestAnimationFrame(r))');
  assert.ok(await evaluate('Math.min(...__dots.slice(2).map(p=>p.x))>Math.max(...__dots.slice(0,2).map(p=>p.x))'),
    'Disconnected components packed outside linked component bounds, not into an orbit');
  const idleDraws=await evaluate('__drawCount');
  await delay(150);
  assert.equal(await evaluate('__drawCount'),idleDraws,'Settled graph performs no idle redraws');
  await evaluate('document.getElementById("layout").value="category";document.getElementById("layout").dispatchEvent(new Event("change"))');
  await wait(()=>evaluate('document.getElementById("motion").textContent==="Layout settled"'),'optional category layout');
  await evaluate('document.getElementById("layout").value="ring";document.getElementById("layout").dispatchEvent(new Event("change"))');
  await wait(()=>evaluate('document.getElementById("motion").textContent==="Layout settled"'),'ring settles');
  await evaluate('new Promise(r=>requestAnimationFrame(r))');
  const ring=await evaluate('__dots');
  assert.equal(ring.length,12,'Ring keeps every record');
  const center=ring.reduce((c,p)=>({x:c.x+p.x/12,y:c.y+p.y/12}),{x:0,y:0});
  const radii=ring.map(p=>Math.hypot(p.x-center.x,p.y-center.y));
  assert.ok(Math.max(...radii)-Math.min(...radii)<.01,'Equal-radius symmetric ring');
  await screenshot('graph-ring.png');
  await click('#pause');await click('#pause');
  await wait(()=>evaluate('document.getElementById("motion").textContent==="Layout settled"'),'ring resumes without a simulation');
  await send('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});
  await evaluate('new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)))');
  assert.ok(await evaluate('document.documentElement.scrollWidth<=innerWidth'),'No mobile ring overflow');
  await screenshot('graph-ring-mobile.png');
  await send('Emulation.setDeviceMetricsOverride',{width:1440,height:900,deviceScaleFactor:1,mobile:false});

  await click('[data-id="experiment-1"]');
  assert.match(await evaluate('document.getElementById("detail").textContent'),/1 causal ancestors/);
  await evaluate('document.getElementById("search").value="not present";document.getElementById("search").dispatchEvent(new Event("input"))');
  assert.equal(await evaluate('document.getElementById("empty").hidden'),false);
  await evaluate('document.getElementById("search").value="";document.getElementById("search").dispatchEvent(new Event("input"));document.getElementById("layout").value="topology";document.getElementById("layout").dispatchEvent(new Event("change"))');
  await wait(()=>evaluate('document.getElementById("motion").textContent==="Layout settled"'),'switch back to connected clusters');
  await evaluate('new Promise(r=>requestAnimationFrame(r))');
  assert.ok(await evaluate('Math.min(...__dots.slice(2).map(p=>p.x))>Math.max(...__dots.slice(0,2).map(p=>p.x))'),
    'Returning from ring restores component packing');
  await evaluate('document.getElementById("layout").value="spread";document.getElementById("layout").dispatchEvent(new Event("change"))');
  await wait(()=>evaluate('document.getElementById("motion").textContent==="Layout settled"'),'spread-out force settles');
  await evaluate('new Promise(r=>requestAnimationFrame(r))');
  assert.equal(await evaluate('__dots.length'),12,'Spread view keeps every record');
  assert.ok(await evaluate('__dots.every(p=>Number.isFinite(p.x)&&Number.isFinite(p.y))'));
  assert.equal(await evaluate('document.querySelector("#detail h3").textContent'),'Experiment 1','Layout keeps selection');
  await screenshot('graph-spread.png');
  await send('Emulation.setEmulatedMedia',{features:[{name:'prefers-reduced-motion',value:'reduce'}]});
  await load('synthetic-240.html');
  assert.equal(await evaluate('document.getElementById("pause").textContent'),'Resume');
  await load('synthetic-separated.html');
  await wait(()=>evaluate('Math.min(...__dots.slice(2).map(p=>p.x))>Math.max(...__dots.slice(0,2).map(p=>p.x))'),
    'reduced-motion initial component layout is applied');
  await evaluate('document.getElementById("layout").value="ring";document.getElementById("layout").dispatchEvent(new Event("change"))');
  const isRing='(()=>{if(__dots.length!==12)return false;const c=__dots.reduce((a,p)=>({x:a.x+p.x/12,y:a.y+p.y/12}),{x:0,y:0});const r=__dots.map(p=>Math.hypot(p.x-c.x,p.y-c.y));return Math.max(...r)-Math.min(...r)<.01})()';
  await wait(()=>evaluate(isRing),'reduced-motion ring applies without Resume');
  assert.equal(await evaluate('document.getElementById("pause").textContent'),'Resume');
  const pausedDraws=await evaluate('__drawCount');
  await delay(150);
  assert.equal(await evaluate('__drawCount'),pausedDraws,'Reduced motion does not animate after layout');
  await evaluate('document.getElementById("search").value="not present";document.getElementById("search").dispatchEvent(new Event("input"))');
  await wait(()=>evaluate('__dots.length===0'),'paused filter clears positions');
  await evaluate('document.getElementById("search").value="";document.getElementById("search").dispatchEvent(new Event("input"))');
  await wait(()=>evaluate(isRing),'paused filter restoration applies new ring');
  await send('Emulation.setEmulatedMedia',{features:[]});
  const start=Date.now();
  await load('synthetic-5000.html');
  await wait(()=>evaluate('window.__dots.length===5000'),'5,000 drawn nodes');
  assert.match(await evaluate('document.getElementById("count").textContent'),/5,000 \/ 5,000/);
  const timings=await evaluate('new Promise(resolve=>{const t=[];let prev=performance.now();function f(){const now=performance.now();t.push(now-prev);prev=now;if(t.length===12)resolve(t);else requestAnimationFrame(f)}requestAnimationFrame(f)})');
  await click('#pause');await click('#fit');await screenshot('graph-5000-desktop.png');
  assert.deepEqual(await evaluate('window.__pageErrors'),[]);
  assert.deepEqual(errors,[]);
  console.log(JSON.stringify({result:'PASS',nodes:5000,loadMs:Date.now()-start,
    meanFrameMs:Math.round(timings.reduce((a,b)=>a+b,0)/timings.length),output}));
} finally {
  cdp?.close();
  if(chrome && exited){
    chrome.kill('SIGTERM');
    if(!await Promise.race([exited.then(()=>true),delay(3000).then(()=>false)])){
      chrome.kill('SIGKILL');
      assert.ok(await Promise.race([exited.then(()=>true),delay(3000).then(()=>false)]),'Chrome exit');
    }
  }
  await rm(profile,{recursive:true,force:true});
}
