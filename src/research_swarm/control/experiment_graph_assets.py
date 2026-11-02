"""Self-contained browser assets for the offline experiment inventory."""

STYLE = """
:root{color-scheme:light;--paper:#fafaf8;--ink:#21272a;--muted:#697677;--line:#e5e8e3;
--teal:#23877d}*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);
font:12px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}header{height:52px;
padding:0 20px;display:flex;align-items:center;justify-content:space-between;border-bottom:
1px solid var(--line);gap:16px}h1{font-size:16px;font-weight:600;margin:0}h2{font-size:10px;
letter-spacing:.1em;text-transform:uppercase;margin:0 0 12px}h3{font-size:16px;line-height:1.4;
margin:8px 0;overflow-wrap:anywhere}p{margin:8px 0}.muted,dt,footer{color:var(--muted)}
main{height:calc(100dvh - 52px);display:grid;grid-template-columns:240px minmax(0,1fr) 290px}
aside{overflow:auto;min-width:0}.inventory{border-right:1px solid var(--line)}
.details{border-left:1px solid var(--line)}section>section,aside>section{padding:20px;
border-bottom:1px solid var(--line)}label{display:block;font-size:11px;margin:12px 0 4px}
input,select,button{font:inherit;color:inherit;border:1px solid var(--line);border-radius:5px;
background:#fff;padding:8px;max-width:100%}input[type=search],select{width:100%}
button{cursor:pointer}button:hover{background:#edf3ef}button:focus-visible,input:focus-visible,
select:focus-visible,canvas:focus-visible{outline:2px solid var(--teal);outline-offset:-2px}
.check{display:flex;align-items:center;gap:7px}.check input{margin:0}.graph{min-width:0;
display:flex;flex-direction:column;overflow:hidden}nav{display:flex;gap:6px;align-items:center;
padding:12px;flex-wrap:wrap;border-bottom:1px solid var(--line)}nav button{padding:5px 10px}
#motion{margin-left:auto;color:var(--muted);font-size:10px}.viewport{flex:1;min-height:0;
position:relative;overflow:hidden}canvas{display:block;width:100%;height:100%;touch-action:none;
cursor:grab}canvas:active{cursor:grabbing}footer{padding:10px 16px;border-top:1px solid var(--line);
font-size:10px}footer p{margin:4px 0 0}.causal{color:#23877d}.reference{color:#8a9497;
padding-left:12px}.node-item{display:block;text-align:left;width:100%;border:0;border-radius:0;
border-bottom:1px solid var(--line);padding:10px 20px;background:transparent}
.node-item[aria-pressed=true]{background:#e5f0eb;box-shadow:inset 3px 0 var(--teal)}
.node-item strong{display:block;font-size:11px;font-weight:600;overflow:hidden;text-overflow:
ellipsis;white-space:nowrap}.node-item small{display:block;color:var(--muted);
overflow-wrap:anywhere}
#more{margin:12px}dl{margin:12px 0}dt{font-size:10px;margin-top:10px}dd{margin:2px 0;
overflow-wrap:anywhere}a{color:var(--teal);text-decoration:none}a:hover{text-decoration:underline}
#detail p{white-space:pre-wrap;overflow-wrap:anywhere}.connection{display:block;text-align:left;
padding:6px;margin:5px 0;width:100%;overflow-wrap:anywhere}#tooltip{position:absolute;
pointer-events:none;left:12px;bottom:12px;max-width:calc(100% - 24px);padding:10px 14px;
background:#fffefa;box-shadow:0 2px 14px #20332920;border:1px solid var(--line);
border-radius:6px;white-space:pre-wrap;overflow-wrap:anywhere;font-size:11px}
#empty{position:absolute;top:40%;left:10%;right:10%;text-align:center;color:var(--muted)}
[hidden]{display:none!important}@media(max-width:1050px){main{grid-template-columns:200px
minmax(0,1fr) 240px}}@media(max-width:760px){header{height:auto;min-height:52px;padding:10px 14px}
h1{font-size:14px}main{height:auto;grid-template-columns:minmax(0,1fr)}
.graph{grid-row:1;height:65dvh;min-height:380px}.inventory{grid-row:2;max-height:380px;
border-top:1px solid var(--line);border-right:0}.details{grid-row:3;border-left:0;
border-top:1px solid var(--line)}#motion{margin-left:0}}
"""

SCRIPT = r"""
(() => {
'use strict';
const data = JSON.parse(document.getElementById('graph-data').textContent);
const $ = id => document.getElementById(id);
const canvas = $('graph'), ctx = canvas.getContext('2d');
const palette = ['#308b80','#768dba','#c38b58','#9b80ac','#7f9b5a','#b97478',
  '#5b9eae','#a79750','#798c91','#aa7c62'];
const categories = [...new Set(data.nodes.map(n => n.cluster))].sort();
const color = new Map(categories.map((c,i) => [c,palette[i % palette.length]]));
const nodes = data.nodes.map(n => ({...n,x:0,y:0,vx:0,vy:0,pinned:false}));
const byId = new Map(nodes.map(n => [n.id,n]));
const edges = data.edges.map(e => ({...e,a:byId.get(e.source),b:byId.get(e.target)}));
const incoming = new Map(nodes.map(n => [n.id,[]]));
const adjacent = new Map(nodes.map(n => [n.id,[]]));
for (const e of edges) {
  adjacent.get(e.source).push(e); adjacent.get(e.target).push(e);
  if(e.kind === 'causal') incoming.get(e.target).push(e);
}
let visible = nodes, visibleEdges = edges, shown = new Set(nodes.map(n => n.id));
let selected = null, neighbors = new Set(), ancestry = new Set(), ancestryEdges = new Set();
let width = 1, height = 1, camera = {x:0,y:0,k:1}, alpha = 1, paused = false;
let frame = 0, dirty = true, drag = null, hover = null, listLimit = 100, autoFit = true;
const reduced = matchMedia('(prefers-reduced-motion: reduce)').matches;
function hash(text) {
  let value = 2166136261;
  for(const c of text) value = Math.imul(value ^ c.charCodeAt(0),16777619);
  return value >>> 0;
}
function targets() {
  const grouped = $('group').checked;
  const counts = new Map(categories.map(c => [c,0]));
  for(const n of nodes) counts.set(n.cluster,counts.get(n.cluster)+1);
  const radius = Math.max(120,Math.sqrt(nodes.length)*15);
  const centers = new Map(categories.map((c,i) => {
    const angle = i * 2 * Math.PI / Math.max(1,categories.length);
    return [c,{x:grouped && categories.length>1 ? Math.cos(angle)*radius : 0,
      y:grouped && categories.length>1 ? Math.sin(angle)*radius : 0,
      radius:Math.max(35,Math.sqrt(grouped ? counts.get(c) : nodes.length)*16)}];
  }));
  for(const n of nodes) n.center = centers.get(n.cluster);
}
function reset() {
  targets();
  for(const n of nodes) {
    const angle = hash(n.id) / 4294967296 * Math.PI * 2;
    const r = Math.sqrt(hash(n.id+'radius') / 4294967296) * n.center.radius;
    n.x = n.center.x + Math.cos(angle)*r; n.y = n.center.y + Math.sin(angle)*r;
    n.vx=0; n.vy=0; n.pinned=false;
  }
  alpha=1; frame=0; autoFit=true; paused=reduced; syncPause(); fit(); dirty=true;
}
function fit() {
  if(!visible.length) {camera={x:width/2,y:height/2,k:1}; dirty=true; return;}
  let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
  for(const n of visible) {
    minX=Math.min(minX,n.x);maxX=Math.max(maxX,n.x);
    minY=Math.min(minY,n.y);maxY=Math.max(maxY,n.y);
  }
  camera.k=Math.max(.01,Math.min(2.5,(width-60)/Math.max(80,maxX-minX),
    (height-60)/Math.max(80,maxY-minY)));
  camera.x=width/2-(minX+maxX)/2*camera.k;
  camera.y=height/2-(minY+maxY)/2*camera.k; dirty=true;
}
function step() {
  // Bounded local repulsion: a spatial grid, not an O(n²) all-pairs pass.
  // Sample at most 20 members per neighboring cell even for dense captures.
  const cells = new Map(), size=65;
  for(const n of visible) {
    const key=Math.floor(n.x/size)+','+Math.floor(n.y/size);
    if(!cells.has(key)) cells.set(key,[]);
    cells.get(key).push(n);
  }
  for(const n of visible) {
    if(n.pinned) continue;
    n.vx+=(n.center.x-n.x)*.0018*alpha;
    n.vy+=(n.center.y-n.y)*.0018*alpha;
    const cx=Math.floor(n.x/size),cy=Math.floor(n.y/size);
    for(let x=cx-1;x<=cx+1;x++) for(let y=cy-1;y<=cy+1;y++) {
      const bucket=cells.get(x+','+y);
      if(!bucket) continue;
      const stride=Math.max(1,Math.ceil(bucket.length/20));
      for(let j=frame%stride;j<bucket.length;j+=stride) {
        const other=bucket[j]; if(other===n) continue;
        let dx=n.x-other.x,dy=n.y-other.y;
        if(dx===0 && dy===0) {dx=.1;dy=.1;}
        const d2=Math.max(36,dx*dx+dy*dy),force=Math.min(2,90*alpha/d2);
        n.vx+=dx*force; n.vy+=dy*force;
      }
    }
  }
  for(const e of visibleEdges) {
    const dx=e.b.x-e.a.x,dy=e.b.y-e.a.y,dist=Math.max(1,Math.hypot(dx,dy));
    const weight=e.kind==='causal' ? .012 : .003 /
      Math.sqrt(Math.max(1,adjacent.get(e.source).length,adjacent.get(e.target).length));
    const force=(dist-65)/dist*weight*alpha;
    if(!e.a.pinned) {e.a.vx+=dx*force;e.a.vy+=dy*force;}
    if(!e.b.pinned) {e.b.vx-=dx*force;e.b.vy-=dy*force;}
  }
  for(const n of visible) if(!n.pinned) {
    n.vx=Math.max(-10,Math.min(10,n.vx*.72)); n.vy=Math.max(-10,Math.min(10,n.vy*.72));
    n.x+=n.vx; n.y+=n.vy;
  }
  alpha*=.982; frame++; dirty=true;
  if(autoFit && (frame%20===0 || alpha<=.005)) fit();
}
function draw() {
  ctx.clearRect(0,0,width,height);
  ctx.save();ctx.translate(camera.x,camera.y);ctx.scale(camera.k,camera.k);
  for(const e of visibleEdges) {
    const causal=e.kind==='causal',active=ancestryEdges.has(e);
    const direct=selected && (e.source===selected.id || e.target===selected.id);
    ctx.globalAlpha=selected ? (active? .95 : (direct ? .6:.06)):(causal?.3:.1);
    ctx.strokeStyle=active?'#237f72':causal?'#7a9890':'#8c949b';
    ctx.lineWidth=(active?1.7:.8)/camera.k;
    ctx.setLineDash(causal?[]:[4/camera.k,4/camera.k]);
    ctx.beginPath();ctx.moveTo(e.a.x,e.a.y);ctx.lineTo(e.b.x,e.b.y);ctx.stroke();
    if(causal && (camera.k>.35 || active)) {
      const a=Math.atan2(e.b.y-e.a.y,e.b.x-e.a.x),r=5/camera.k;
      const x=e.b.x-Math.cos(a)*r,y=e.b.y-Math.sin(a)*r;
      ctx.beginPath();ctx.moveTo(x,y);
      ctx.lineTo(x-Math.cos(a-.45)*5/camera.k,y-Math.sin(a-.45)*5/camera.k);
      ctx.lineTo(x-Math.cos(a+.45)*5/camera.k,y-Math.sin(a+.45)*5/camera.k);
      ctx.closePath();ctx.fillStyle=ctx.strokeStyle;ctx.fill();
    }
  }
  ctx.setLineDash([]);
  for(const n of visible) {
    const active=n===selected,related=neighbors.has(n.id)||ancestry.has(n.id);
    ctx.globalAlpha=selected && !active && !related?.18:1;
    const r=(active?7:n===hover?6:3.5)/Math.sqrt(camera.k);
    ctx.beginPath();ctx.arc(n.x,n.y,r,0,Math.PI*2);ctx.fillStyle=color.get(n.cluster);ctx.fill();
    if(active || n.pinned) {
      ctx.strokeStyle=active?'#24443b':'#fff';ctx.lineWidth=2/camera.k;ctx.stroke();
    }
    if(active || n===hover || (visible.length<180 && camera.k>.6) ||
      (related && camera.k>1.3)) {
      ctx.font=(11/camera.k)+'px system-ui';ctx.fillStyle='#475450';
      ctx.fillText(n.label.slice(0,55),n.x+r+4/camera.k,n.y+3/camera.k);
    }
  }
  ctx.restore();ctx.globalAlpha=1;
}
function renderList() {
  const fragment=document.createDocumentFragment();
  for(const n of visible.slice(0,listLimit)) {
    const button=document.createElement('button');button.className='node-item';
    button.setAttribute('aria-pressed',String(n===selected));button.dataset.id=n.id;
    const strong=document.createElement('strong');strong.textContent=n.label;
    const small=document.createElement('small');small.textContent=n.cluster+' · '+n.status;
    button.append(strong,small);button.onclick=()=>select(n);fragment.append(button);
  }
  $('node-list').replaceChildren(fragment);$('more').hidden=visible.length<=listLimit;
  $('count').textContent=visible.length.toLocaleString()+' / '+nodes.length.toLocaleString()+
    ' records · '+visibleEdges.length.toLocaleString()+' links';
}
function element(tag,text) {const el=document.createElement(tag);el.textContent=text;return el;}
function select(n) {
  selected=n;neighbors=new Set();ancestry=new Set();ancestryEdges=new Set();dirty=true;
  const detail=$('detail');detail.replaceChildren();
  for(const button of $('node-list').children)
    button.setAttribute('aria-pressed',String(n && button.dataset.id===n.id));
  if(!n) {detail.append(element('p','Select a node or inventory item.'));return;}
  const links=adjacent.get(n.id);
  for(const e of links) neighbors.add(e.source===n.id?e.target:e.source);
  // Only explicit incoming causal edges participate; cycles terminate safely.
  const stack=[n.id],visited=new Set([n.id]);
  while(stack.length) for(const e of incoming.get(stack.pop())) {
    ancestryEdges.add(e);
    if(!visited.has(e.source)) {visited.add(e.source);ancestry.add(e.source);stack.push(e.source);}
  }
  detail.append(element('h3',n.label),element('p',n.summary));
  const dl=document.createElement('dl');
  for(const [label,value] of [['ID',n.id],['Category',n.cluster],['Status',n.status],
    ['Source score',n.score===null?'Not recorded':String(n.score)]]) {
    dl.append(element('dt',label),element('dd',value));
  }
  if(n.node_type) dl.append(element('dt','Record type'),element('dd',n.node_type));
  detail.append(dl);
  if(n.source_url) {
    const link=element('a','Open source on GitHub ↗');link.href=n.source_url;
    link.target='_blank';link.rel='noopener noreferrer';detail.append(link);
  } else detail.append(element('p','No GitHub source link recorded.'));
  detail.append(element('p',neighbors.size+' direct neighbors · '+ancestry.size+
    ' causal ancestors (including filtered nodes).'));
  const release=element('button','Release pinned node');
  release.onclick=()=>{n.pinned=false;alpha=1;dirty=true;};detail.append(release);
  detail.append(element('h2','Recorded connections'));
  for(const e of links.slice(0,100)) {
    const other=e.source===n.id?e.b:e.a;
    const direction=e.kind==='causal'?(e.target===n.id?'Parent: ':'Child: '):'Source reference: ';
    const button=element('button',direction+other.label+(e.label?' — '+e.label:'')+
      (shown.has(other.id)?'':' (filtered)'));
    button.className='connection';button.onclick=()=>select(other);detail.append(button);
  }
  if(links.length>100) detail.append(element('p','Showing 100 of '+links.length+
    ' connections. All captured connections remain in the graph.'));
}
function filter() {
  const term=$('search').value.toLocaleLowerCase(),category=$('cluster').value;
  visible=nodes.filter(n=>(!category || JSON.stringify(n.cluster)===category) &&
    (!term || [n.id,n.label,n.summary,n.status,n.cluster].join(' ')
      .toLocaleLowerCase().includes(term)));
  shown=new Set(visible.map(n=>n.id));
  visibleEdges=edges.filter(e=>shown.has(e.source)&&shown.has(e.target));
  listLimit=100;renderList();$('empty').hidden=visible.length>0;alpha=1;autoFit=true;fit();
}
function syncPause() {
  $('pause').textContent=paused?'Resume':'Pause';
  $('pause').setAttribute('aria-pressed',String(paused));
}
function zoom(factor,x=width/2,y=height/2) {
  autoFit=false;
  const k=Math.min(8,Math.max(.01,camera.k*factor)),ratio=k/camera.k;
  camera.x=x-(x-camera.x)*ratio;camera.y=y-(y-camera.y)*ratio;camera.k=k;dirty=true;
}
function point(event) {
  const r=canvas.getBoundingClientRect();return {x:event.clientX-r.left,y:event.clientY-r.top};
}
function hit(p) {
  const x=(p.x-camera.x)/camera.k,y=(p.y-camera.y)/camera.k;
  let best=null,dist=100/camera.k/camera.k;
  for(const n of visible) {const d=(n.x-x)**2+(n.y-y)**2;if(d<dist){dist=d;best=n;}}
  return best;
}
canvas.addEventListener('pointerdown',e=>{
  if(e.button!==0) return;
  autoFit=false;
  const p=point(e),n=hit(p);canvas.setPointerCapture(e.pointerId);
  drag={id:e.pointerId,n,p,moved:false};$('tooltip').hidden=true;
  if(n) {n.pinned=true;n.vx=0;n.vy=0;select(n);}
});
canvas.addEventListener('pointermove',e=>{
  const p=point(e);
  if(drag && drag.id===e.pointerId) {
    const dx=p.x-drag.p.x,dy=p.y-drag.p.y;drag.moved ||= Math.abs(dx)+Math.abs(dy)>2;
    if(drag.n) {
      drag.n.x+=dx/camera.k;drag.n.y+=dy/camera.k;alpha=Math.max(alpha,.4);
    } else {camera.x+=dx;camera.y+=dy;}
    drag.p=p;dirty=true;return;
  }
  hover=hit(p);dirty=true;
  $('tooltip').hidden=!hover;
  if(hover) $('tooltip').textContent=hover.label+'\n'+hover.cluster+' · '+hover.status+
    (hover.score===null?'':' · source score '+hover.score)+'\n'+hover.summary.slice(0,250);
});
function endDrag(e) {
  if(!drag || drag.id!==e.pointerId)return;
  if(!drag.n && !drag.moved && e.type!=='pointercancel')select(null);
  drag=null;
}
canvas.addEventListener('pointerup',endDrag);canvas.addEventListener('pointercancel',endDrag);
canvas.addEventListener('lostpointercapture',()=>{drag=null;});
canvas.addEventListener('pointerleave',()=>{hover=null;$('tooltip').hidden=true;dirty=true;});
canvas.addEventListener('dblclick',e=>{
  const n=hit(point(e));if(n){n.pinned=false;alpha=1;dirty=true;}});
canvas.addEventListener('wheel',e=>{e.preventDefault();const p=point(e);
  zoom(Math.exp(-Math.max(-200,Math.min(200,e.deltaY))*.003),p.x,p.y);},{passive:false});
canvas.addEventListener('keydown',e=>{
  if(e.key==='Escape')select(null);
  else if(e.key.toLowerCase()==='f')fit();
  else if(e.key==='+' || e.key==='=')zoom(1.25);
  else if(e.key==='-')zoom(.8);
  else if(e.key.startsWith('Arrow')) {
    autoFit=false;
    camera.x+=e.key==='ArrowLeft'?30:e.key==='ArrowRight'?-30:0;
    camera.y+=e.key==='ArrowUp'?30:e.key==='ArrowDown'?-30:0;dirty=true;
  } else return;
  e.preventDefault();
});
for(const category of categories) {
  const option=element('option',category || '(Uncategorized)');
  option.value=JSON.stringify(category);
  $('cluster').append(option);
}
$('search').addEventListener('input',filter);$('cluster').addEventListener('change',filter);
$('more').onclick=()=>{listLimit+=100;renderList();};
$('fit').onclick=()=>{autoFit=false;fit();};
$('zoom-in').onclick=()=>zoom(1.25);$('zoom-out').onclick=()=>zoom(.8);
$('pause').onclick=()=>{paused=!paused;if(!paused && alpha<.005)alpha=.4;syncPause();};
$('reset').onclick=reset;$('group').onchange=reset;
new ResizeObserver(()=>{
  const rect=canvas.getBoundingClientRect(),dpr=Math.min(2,devicePixelRatio||1);
  width=rect.width;height=rect.height;canvas.width=Math.round(width*dpr);
  canvas.height=Math.round(height*dpr);ctx.setTransform(dpr,0,0,dpr,0,0);fit();
}).observe(canvas);
reset();filter();
function animate() {
  if(!paused && alpha>.005 && visible.length)step();
  if(dirty) {draw();dirty=false;}
  const state=paused?'Layout paused':alpha>.005 && visible.length?'Arranging…':'Layout settled';
  if($('motion').textContent!==state)$('motion').textContent=state;
  requestAnimationFrame(animate);
}
requestAnimationFrame(animate);
})();
"""
