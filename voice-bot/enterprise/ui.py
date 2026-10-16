"""Operator pages: callback inbox and human-call review. Self-contained (no external assets).
Auth is the existing dashboard key cookie/Bearer header; every value is rendered with textContent, never innerHTML."""
CSP = "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"

_STYLE = """<style>body{font:15px/1.5 Arial,sans-serif;margin:0;background:#eef2f7;color:#17253a}header{background:#17253a;color:#fff;padding:12px 20px}
header a{color:#cfe3ff;margin-right:16px;text-decoration:none}main{max-width:1100px;margin:20px auto;padding:0 16px}
.card{background:#fff;border-radius:10px;padding:14px 16px;margin:10px 0;box-shadow:0 1px 3px #0002}.row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
.pill{border-radius:99px;padding:2px 10px;font-size:12px;background:#dde6f3}.bad{background:#ffd9d6;color:#8a1b12}.ok{background:#d8f2df;color:#14532d}.warn{background:#fef3c7;color:#92400e}
button{background:#1f5fbf;color:#fff;border:0;border-radius:6px;padding:7px 12px;cursor:pointer}button.alt{background:#5b6b82}input,select,textarea{padding:6px;border:1px solid #b7c3d6;border-radius:6px;font:inherit}
.turn{padding:6px 10px;margin:4px 0;border-radius:8px;background:#f3f6fb}.turn.caller{background:#e2f1ff;border-left:4px solid #1f5fbf}.turn.rep{background:#f6f0e4;border-left:4px solid #b7791f}
.muted{color:#5b6b82;font-size:13px}.err{color:#8a1b12}</style>"""

_NAV = '<header><a href="/enterprise/callbacks/ui">Callback inbox</a><a href="/enterprise/review/ui">Human-call review</a><a href="/dashboard">Dashboard</a></header>'

_COMMON_JS = """
const $=(s)=>document.querySelector(s);
function el(tag,attrs,...kids){const e=document.createElement(tag);for(const[k,v]of Object.entries(attrs||{})){if(k==='class')e.className=v;else if(k.startsWith('on'))e.addEventListener(k.slice(2),v);else e.setAttribute(k,v)}
 for(const c of kids)e.append(c instanceof Node?c:document.createTextNode(c==null?'':String(c)));return e}
async function api(path,opts){const r=await fetch(path,{credentials:'same-origin',headers:{'Content-Type':'application/json'},...opts});
 if(r.status===401){throw new Error('Not signed in: open /dashboard and enter the dashboard key first')}
 const t=await r.text();let j={};try{j=JSON.parse(t)}catch(e){}
 if(!r.ok)throw new Error((j&&j.detail&&(typeof j.detail==='string'?j.detail:JSON.stringify(j.detail)))||('HTTP '+r.status));return j}
function fmt(ts){return ts?new Date(ts*1000).toLocaleString():''}
function showErr(e){const b=$('#err');b.textContent=e.message||String(e)}
"""


def _page(title, body, script):
    return (f'<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            f'<title>{title}</title>{_STYLE}{_NAV}<main><h2>{title}</h2><div id="err" class="err"></div>{body}</main><script>{_COMMON_JS}{script}</script></html>')


CALLBACKS_HTML = _page('Callback inbox', '<div class="row"><select id="f"><option value="open,assigned">Open</option><option value="done">Done</option><option value="open,assigned,done,cancelled">All</option></select><button id="r">Refresh</button></div><div id="list"></div>', """
async function load(){try{$('#err').textContent='';const d=await api('/enterprise/callbacks?status='+encodeURIComponent($('#f').value));const box=$('#list');box.replaceChildren();
 if(!d.callbacks.length)box.append(el('p',{class:'muted'},'Nothing here.'));
 for(const c of d.callbacks){
  const who=el('input',{placeholder:'Assign to (rep name)',value:c.assignee||''});
  const mins=el('input',{type:'number',min:'1',max:'1440',placeholder:'due in min',style:'width:100px'});
  const out=el('select',{},...['reached','no_answer','wrong_number','not_interested','visit_scheduled','other'].map(o=>el('option',{value:o},o)));
  const note=el('input',{placeholder:'Notes',style:'width:240px'});
  const card=el('div',{class:'card'},
   el('div',{class:'row'},el('strong',{},c.phone||'(no number)'),el('span',{class:'pill'},c.status),c.overdue?el('span',{class:'pill bad'},'OVERDUE'):'',el('span',{class:'pill'},c.reason),
     el('a',{href:'tel:'+(c.phone||'')},'Call')),
   el('div',{class:'muted'},'Raised '+fmt(c.created)+' | due '+fmt(c.due_at)+(c.assignee?' | owner '+c.assignee:'')+(c.outcome?' | last outcome '+c.outcome:'')+(c.notes?' | '+c.notes:'')),
   c.status==='done'?'':el('div',{class:'row'},who,mins,el('button',{onclick:async()=>{try{await api('/enterprise/callbacks/'+c.id+'/assign',{method:'POST',body:JSON.stringify({assignee:who.value,due_minutes:mins.value?Number(mins.value):null})});load()}catch(e){showErr(e)}}},'Assign'),
     out,note,el('button',{class:'alt',onclick:async()=>{try{await api('/enterprise/callbacks/'+c.id+'/complete',{method:'POST',body:JSON.stringify({outcome:out.value,notes:note.value})});load()}catch(e){showErr(e)}}},'Mark done')));
  box.append(card)}}catch(e){showErr(e)}}
$('#r').addEventListener('click',load);$('#f').addEventListener('change',load);load();setInterval(load,60000);
""")

REVIEW_HTML = _page('Call Review & Ground Truth', """
<div class="card" style="margin-bottom:20px">
 <div class="row" style="justify-content:space-between">
  <h3 style="margin:0">AI Call Quality & Ground Truth (Coval Active-Learning)</h3>
  <button id="refresh-evals" class="alt" style="padding:4px 8px;font-size:12px">Refresh Calls</button>
 </div>
 <p class="muted" style="margin:4px 0 10px 0">Review AI verdicts and override ground truth. Overrides update regression fixtures and log reviewer identity.</p>
 <div id="eval-list"></div>
 <div id="eval-override-box" style="margin-top:12px"></div>
</div>

<div class="card">
 <h3 style="margin:0 0 10px 0">Lead Qualification Review (Human Calls)</h3>
 <div id="list"></div>
 <div id="detail"></div>
</div>
""", """
async function loadEvalCalls(){try{const d=await api('/enterprise/eval/calls');const box=$('#eval-list');box.replaceChildren();
 if(!d.calls||!d.calls.length){box.append(el('p',{class:'muted'},'No AI eval call reports recorded yet.'));return}
 for(const c of d.calls){
  const ov=c.human_override;
  const aiPill=el('span',{class:'pill '+(c.ai_verdict==='PASS'?'ok':'bad')},'AI: '+c.ai_verdict);
  const ovPill=ov?el('span',{class:'pill warn'},'OVERRIDE: '+ov.verdict.toUpperCase()+' (by '+ov.by+')'):'';
  const fails=(c.failed_checks&&c.failed_checks.length)?el('span',{class:'muted'},'Failed: '+c.failed_checks.join(', ')):'';
  const when=c.scored_at?el('span',{class:'muted'},c.scored_at.slice(0,19).replace('T',' ')):'';
  const btn=el('button',{class:ov?'alt':'',onclick:()=>openEvalOverride(c)},ov?'Edit Override':'Override Verdict');
  const card=el('div',{class:'card row',style:'margin:6px 0;justify-content:space-between'},el('div',{class:'row'},el('strong',{},c.call_id),aiPill,ovPill,when,fails),btn);
  if(ov&&ov.reason){card.append(el('div',{class:'muted',style:'width:100%;margin-top:4px'},'Ground truth reason: '+ov.reason))}
  box.append(card)}}catch(e){showErr(e)}}

function openEvalOverride(c){
 const box=$('#eval-override-box');box.replaceChildren();
 const ov=c.human_override||{};
 const currentV=ov.verdict?ov.verdict.toLowerCase():(c.ai_verdict==='PASS'?'fail':'pass');
 const sel=el('select',{},el('option',{value:'pass',...(currentV==='pass'?{selected:''}:{})},'PASS (Mark as Valid / Good)'),el('option',{value:'fail',...(currentV==='fail'?{selected:''}:{})},'FAIL (Mark as Defect / Regression)'));
 const reason=el('input',{placeholder:'Mandatory one-line reason for ground truth override',style:'flex:1;min-width:300px',value:ov.reason||''});
 const saveBtn=el('button',{onclick:async()=>{try{
  if(!reason.value||reason.value.trim().length<3){throw new Error('Please enter a clear one-line reason for the ground truth override.')}
  await api('/enterprise/eval/override/'+encodeURIComponent(c.call_id),{method:'POST',body:JSON.stringify({verdict:sel.value,reason:reason.value.trim()})});
  box.replaceChildren();
  showErr({message:'Override recorded as ground truth and synced to regression fixtures for '+c.call_id});
  loadEvalCalls()}catch(e){showErr(e)}}},'Save Override');
 const cancelBtn=el('button',{class:'alt',onclick:()=>box.replaceChildren()},'Cancel');
 const formCard=el('div',{class:'card',style:'border:2px solid #1f5fbf;background:#f9fbff'},el('h4',{style:'margin:0 0 8px 0'},'Override Ground Truth: '+c.call_id),
  el('div',{class:'muted',style:'margin-bottom:8px'},'AI verdict: '+c.ai_verdict+'. Action logs reviewer identity and updates permanent regression test fixtures.'),
  el('div',{class:'row'},sel,reason,saveBtn,cancelBtn));
 box.append(formCard)}

$('#refresh-evals').addEventListener('click',loadEvalCalls);
loadEvalCalls();

async function loadList(){try{const d=await api('/enterprise/human-reviews');const box=$('#list');box.replaceChildren();
 if(!d.items.length)box.append(el('p',{class:'muted'},'No transcribed human calls yet.'));
 for(const i of d.items){box.append(el('div',{class:'card row'},el('strong',{},i.name||i.phone),el('span',{class:'pill '+(i.reviewed?'ok':'bad')},i.reviewed?'reviewed: '+i.tier:'needs review'),
   el('span',{class:'muted'},i.when),el('button',{onclick:()=>openCall(i.lead_id,i.call_id)},i.reviewed?'Open':'Review')))}}catch(e){showErr(e)}}
async function openCall(lead,call){try{$('#err').textContent='';const d=await api('/enterprise/human-review/'+lead+'/'+call);const box=$('#detail');box.replaceChildren();
 const speakers=[...new Set(d.entries.map(x=>x.speaker))];const pick=el('select',{},el('option',{value:''},'Select the CALLER speaker'),...speakers.map(s=>el('option',{value:s,...(d.caller_speaker===s?{selected:''}:{})},'Speaker '+s)));
 const t=el('div',{});
 function paint(){t.replaceChildren();if(!d.entries.length){t.append(el('div',{class:'turn'},d.transcript||''));return}
  for(const x of d.entries){t.append(el('div',{class:'turn '+(pick.value&&x.speaker===pick.value?'caller':(pick.value?'rep':''))},el('strong',{},'Speaker '+x.speaker+': '),x.text))}}
 pick.addEventListener('change',paint);paint();
 const f={name:el('input',{placeholder:'Caller name',value:d.lead.name||''}),bmin:el('input',{type:'number',step:'0.1',min:'0',placeholder:'Budget min (lakhs)'}),bmax:el('input',{type:'number',step:'0.1',min:'0',placeholder:'Budget max (lakhs)'}),
  tl:el('input',{type:'number',min:'0',placeholder:'Timeline (months)'}),cfg:el('input',{placeholder:'Config e.g. 3BHK'}),
  visit:el('select',{},...['none','maybe','requested','booked'].map(o=>el('option',{value:o},'Visit: '+o))),chk:el('input',{type:'checkbox'})};
 const num=(x)=>x.value===''?null:Number(x.value);
 box.append(el('div',{class:'card'},el('h3',{},(d.lead.name||d.lead.phone)+' | '+d.lead.phone),el('div',{class:'muted'},'Pick which speaker is the caller. Only the caller\\'s own statements count for budget, timeline and visit.'),
  el('div',{class:'row'},pick),t),
  el('div',{class:'card'},el('div',{class:'row'},f.name,f.bmin,f.bmax,f.tl,f.cfg,f.visit),
   el('label',{},f.chk,' I reviewed the caller\\'s own statements (not the rep\\'s)'),
   el('div',{class:'row'},el('button',{onclick:async()=>{try{
     const r=await api('/enterprise/human-review/'+lead+'/'+call,{method:'POST',body:JSON.stringify({caller_evidence_reviewed:f.chk.checked,caller_speaker:pick.value||null,caller_name:f.name.value||null,
       budget_min_lakhs:num(f.bmin),budget_max_lakhs:num(f.bmax),timeline_months:num(f.tl),configuration:f.cfg.value||null,visit_intent:f.visit.value})});
     showErr({message:'Saved: '+r.tier.toUpperCase()+' ('+r.score+') - '+r.reason});loadList()}catch(e){showErr(e)}}},'Submit review'))))
 }catch(e){showErr(e)}}
loadList();
""")
