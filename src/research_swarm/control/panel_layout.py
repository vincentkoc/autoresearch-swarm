"""Shared local-only rail and section sizing for offline research views."""

PANEL_STYLE = """
main.layout-workspace{--left-width:18%;--right-width:28.5%;display:grid;
grid-template-columns:var(--left-width) 8px minmax(0,1fr) 8px var(--right-width)}
.layout-left{grid-column:1}.layout-center{grid-column:3}.layout-right{grid-column:5}
main.layout-workspace>.splitter[data-resize=left]{grid-column:2}
main.layout-workspace>.splitter[data-resize=right]{grid-column:4}
.layout-rail{display:flex;flex-direction:column;overflow:hidden;min-height:0}
.layout-rail>.layout-section{flex:1 1 0;min-height:72px;overflow:auto}
.layout-rail>.layout-section.right-section{overflow:hidden}
.layout-workspace .splitter{padding:0;border:0;background:#f1f3ef;position:relative;
touch-action:none;user-select:none;cursor:col-resize;min-width:8px}
.layout-workspace .section-splitter{flex:0 0 8px;min-height:8px;cursor:row-resize}
.layout-workspace .splitter:hover{background:#c4d7d0}
.layout-workspace .splitter:focus-visible{outline:2px solid #23877d;outline-offset:-2px}
.layout-workspace[data-left-collapsed=true]{--left-width:0px!important}
.layout-workspace[data-right-collapsed=true]{--right-width:0px!important}
.layout-workspace[data-left-collapsed=true]>.splitter[data-resize=left],
.layout-workspace[data-right-collapsed=true]>.splitter[data-resize=right]{visibility:hidden}
.layout-workspace [hidden]{display:none!important}
.layout-toggles{display:flex;gap:6px;align-items:center}
.layout-toggles button{font:inherit;background:#fff;color:inherit;border:1px solid #e5e8e3;
border-radius:5px;padding:5px 8px;cursor:pointer}
body.layout-resizing{user-select:none}
@media(max-width:760px){body>header{height:auto;min-height:52px;flex-wrap:wrap}
.layout-toggles{flex-wrap:wrap;max-width:100%}
main.layout-workspace{display:flex;flex-direction:column;height:auto;
min-height:0}.layout-center{order:0}.layout-right{order:1}.layout-left{order:2}
main.layout-workspace>.splitter,.layout-workspace .section-splitter{display:none}
.layout-workspace .layout-rail{height:auto;overflow:visible}
.layout-workspace .layout-section{flex:none!important;max-height:420px;min-height:72px;
overflow:auto}.layout-workspace .layout-section.right-section{height:300px}}
"""

PANEL_SCRIPT = r"""
(() => {
'use strict';
const root=document.querySelector('main');
if(!root)return;
const left=root.querySelector(':scope > .sidebar, :scope > .inventory');
const right=root.querySelector(':scope > .right, :scope > .details');
const center=root.querySelector(':scope > .graph-panel, :scope > .graph');
if(!left||!right||!center)return;
root.classList.add('layout-workspace');left.classList.add('layout-left','layout-rail');
right.classList.add('layout-right','layout-rail');center.classList.add('layout-center');
const narrow=()=>matchMedia('(max-width:760px)').matches;
const key='swarm-panel-layout-v1';
let saved={};
try{saved=JSON.parse(localStorage.getItem(key)||'{}')||{};}catch{}
const persist=()=>{try{localStorage.setItem(key,JSON.stringify(saved));}catch{}};
const rails={left,right};
let toggles=document.querySelector('.header-actions');
if(!toggles){toggles=document.createElement('div');toggles.className='layout-toggles';
  document.querySelector('header').append(toggles);}
const sync=()=>{
  for(const side of ['left','right']){
    const handle=root.querySelector(':scope > [data-resize="'+side+'"]');
    const width=Math.round(rails[side].getBoundingClientRect().width);
    handle.setAttribute('aria-valuenow',String(width));
    handle.setAttribute('aria-valuemax',String(Math.round(root.clientWidth*.45)));
    handle.setAttribute('aria-valuetext',rails[side].hidden?'Collapsed':width+' pixels');
  }
};
function resize(side,width,remember=true){
  if(narrow())return;
  const other=rails[side==='left'?'right':'left'].getBoundingClientRect().width;
  const maximum=Math.max(0,Math.min(root.clientWidth*.45,root.clientWidth-other-296));
  const value=Math.round(Math.max(Math.min(160,maximum),Math.min(maximum,width)));
  root.style.setProperty('--'+side+'-width',value+'px');
  if(remember){saved[side]=value;persist();}sync();
}
function collapse(side,hidden){
  root.dataset[side+'Collapsed']=String(hidden);rails[side].hidden=hidden;
  toggles.querySelector('[data-toggle="'+side+'"]').setAttribute('aria-expanded',String(!hidden));
  if(!hidden)resize(side,saved[side]||rails[side].getBoundingClientRect().width,false);
  sync();
}
for(const side of ['left','right']){
  let button=toggles.querySelector('[data-toggle="'+side+'"]');
  if(!button){button=document.createElement('button');button.dataset.toggle=side;
    button.textContent=side==='left'?'Inventory':'Details';toggles.append(button);}
  button.setAttribute('aria-expanded','true');
  if(!rails[side].id)rails[side].id='layout-'+side;
  button.setAttribute('aria-controls',rails[side].id);
  button.addEventListener('click',()=>collapse(side,!rails[side].hidden));
  let handle=root.querySelector(':scope > [data-resize="'+side+'"]');
  if(!handle){handle=document.createElement('div');handle.dataset.resize=side;
    root.insertBefore(handle,side==='left'?center:right);}
  handle.classList.add('splitter');handle.tabIndex=0;handle.setAttribute('role','separator');
  handle.setAttribute('aria-orientation','vertical');
  handle.setAttribute('aria-label','Resize '+(side==='left'?'left sidebar':'right sidebar'));
  handle.setAttribute('aria-controls',rails[side].id);handle.setAttribute('aria-valuemin','0');
  let drag=null;
  handle.addEventListener('pointerdown',e=>{
    if(narrow()||e.button!==0)return;e.preventDefault();
    drag={x:e.clientX,width:rails[side].getBoundingClientRect().width};
    handle.setPointerCapture(e.pointerId);document.body.classList.add('layout-resizing');
  });
  handle.addEventListener('pointermove',e=>{
    if(drag)resize(side,drag.width+(side==='left'?1:-1)*(e.clientX-drag.x));
  });
  const stop=()=>{drag=null;document.body.classList.remove('layout-resizing');};
  for(const event of ['pointerup','pointercancel','lostpointercapture'])
    handle.addEventListener(event,stop);
  handle.addEventListener('keydown',e=>{
    if(e.key==='Enter'){e.preventDefault();collapse(side,!rails[side].hidden);}
    else if(['ArrowLeft','ArrowRight'].includes(e.key)){
      e.preventDefault();resize(side,rails[side].getBoundingClientRect().width+
        (e.key==='ArrowRight'?20:-20)*(side==='left'?1:-1));
    }
  });
}
const list=left.querySelector(':scope > #node-list');
if(list){const section=document.createElement('section');section.className='inventory-list';
  left.insertBefore(section,list);section.append(list);
  const more=left.querySelector(':scope > #more');if(more)section.append(more);}
const sectionResets=[];
for(const side of ['left','right']){
  const rail=rails[side];
  const sections=[...rail.children].filter(el=>el.matches('section,.sidebar-section'));
  const ratioKey=side+'Sections';
  let ratios=Array.isArray(saved[ratioKey])&&saved[ratioKey].length===sections.length?
    saved[ratioKey]:sections.map(()=>1);
  if(!ratios.every(v=>Number.isFinite(v)&&v>0))ratios=sections.map(()=>1);
  const apply=()=>sections.forEach((section,i)=>{
    section.classList.add('layout-section');section.style.flex=ratios[i]+' 1 0px';});
  apply();
  sectionResets.push(()=>{ratios=sections.map(()=>1);apply();});
  for(let i=0;i<sections.length-1;i++){
    const handle=document.createElement('div');handle.className='splitter section-splitter';
    handle.tabIndex=0;handle.setAttribute('role','separator');
    handle.setAttribute('aria-orientation','horizontal');
    handle.setAttribute('aria-label','Resize '+side+' sidebar sections');
    handle.setAttribute('aria-valuemin','72');rail.insertBefore(handle,sections[i+1]);
    let drag=null;
    function change(delta,initial){
      const total=initial.a+initial.b;
      const minimum=Math.min(72,total/2);
      const value=Math.max(minimum,Math.min(total-minimum,initial.a+delta));
      const share=initial.ra+initial.rb;
      const extra=section=>{
        const css=getComputedStyle(section);
        return ['paddingTop','paddingBottom','borderTopWidth','borderBottomWidth']
          .reduce((sum,key)=>sum+(parseFloat(css[key])||0),0);
      };
      const firstExtra=extra(sections[i]),secondExtra=extra(sections[i+1]);
      const space=Math.max(1,total-firstExtra-secondExtra);
      ratios[i]=share*Math.max(.001,(value-firstExtra)/space);
      ratios[i+1]=share*Math.max(.001,(total-value-secondExtra)/space);
      apply();saved[ratioKey]=ratios;persist();
      handle.setAttribute('aria-valuenow',String(Math.round(value)));
      handle.setAttribute('aria-valuemax',String(Math.round(total-72)));
    }
    const initial=()=>({a:sections[i].getBoundingClientRect().height,
      b:sections[i+1].getBoundingClientRect().height,ra:ratios[i],rb:ratios[i+1]});
    handle.addEventListener('pointerdown',e=>{
      if(narrow()||e.button!==0)return;e.preventDefault();drag={y:e.clientY,...initial()};
      handle.setPointerCapture(e.pointerId);document.body.classList.add('layout-resizing');
    });
    handle.addEventListener('pointermove',e=>{if(drag)change(e.clientY-drag.y,drag);});
    const stop=()=>{drag=null;document.body.classList.remove('layout-resizing');};
    for(const event of ['pointerup','pointercancel','lostpointercapture'])
    handle.addEventListener(event,stop);
    handle.addEventListener('keydown',e=>{
      if(narrow())return;
      if(['ArrowUp','ArrowDown'].includes(e.key)){e.preventDefault();change(e.key==='ArrowDown'?20:-20,initial());}
    });
  }
}
const reset=document.createElement('button');reset.textContent='Reset panels';
reset.dataset.layoutReset='';toggles.append(reset);
reset.addEventListener('click',()=>{
  saved={};persist();sectionResets.forEach(fn=>fn());
  for(const side of ['left','right']){
    root.style.removeProperty('--'+side+'-width');root.dataset[side+'Collapsed']='false';
    rails[side].hidden=false;
    toggles.querySelector('[data-toggle="'+side+'"]').setAttribute('aria-expanded','true');
  }
  sync();
});
for(const side of ['left','right'])if(Number.isFinite(saved[side]))resize(side,saved[side],false);
addEventListener('resize',()=>{
  root.style.removeProperty('--left-width');root.style.removeProperty('--right-width');
  for(const side of ['left','right'])if(Number.isFinite(saved[side]))resize(side,saved[side],false);
  sync();
});
sync();
})();
"""
