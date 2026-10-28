// Client timing/state regressions without starting Spark, Docker or a browser.
'use strict';
const {readFileSync}=require('node:fs');
const {join}=require('node:path');
const vm=require('node:vm');
const assert=require('node:assert/strict');
const ui=join(__dirname,'../dashboard/presentation_ui');
const nodes=new Map(), frames=new Map(), messages=[], events={};
let frameNumber=0,now=0;
class Element {
 constructor(){this.textContent='';this.innerHTML='';this.dataset={};this.style={};this.attributes={};this.children=[];this.scrollTop=0;this.clientHeight=200;this.scrollHeight=200;this.parentElement=null;this.writes=0;this.classes=new Set();this.classList={toggle:(name,on)=>on?this.classes.add(name):this.classes.delete(name)};}
 setAttribute(name,value){this.attributes[name]=value;}
 appendChild(node){node.parentElement=this;this.children.push(node);}
 replaceChildren(...children){this.children=children;this.writes++;}
 getBoundingClientRect(){return {height:650};}
 getAnimations(){return [];}
 animate(){return {cancel(){}};}
}
const html=readFileSync(join(ui,'index.html'),'utf8');
for(const [,id] of html.matchAll(/id="([^"]+)"/g))nodes.set(id,new Element());
nodes.get('timeline-fill').parentElement=new Element();
const preference={matches:false,addEventListener(type,callback){this.changed=callback;}};
const document={hidden:false,documentElement:{dataset:{}},body:new Element(),getElementById:id=>nodes.get(id),createElement:()=>new Element(),querySelectorAll:selector=>selector==='.check'?nodes.get('checks').children:nodes.get('chapters').children,addEventListener:(type,callback)=>{events[type]=callback;}};
const parent={postMessage:message=>messages.push(message)};
const context=vm.createContext({document,window:{parent,addEventListener:(type,callback)=>{events[type]=callback;}},matchMedia:()=>preference,performance:{now:()=>now},requestAnimationFrame:callback=>{frames.set(++frameNumber,callback);return frameNumber;},cancelAnimationFrame:id=>frames.delete(id),ResizeObserver:class{observe(){}},console});
vm.runInContext(readFileSync(join(ui,'presentation.js'),'utf8'),context);
function flush(time=now){now=time;const pending=[...frames];frames.clear();for(const [,callback] of pending)callback(now);}
function run(code){return vm.runInContext(code,context);}
flush();
assert.equal(frames.size,0,'Paused narration must not keep a 60fps JS loop running');
const initialHeightMessages=messages.filter(m=>m.type==='streamlit:setFrameHeight').length;
for(let i=0;i<20;i++)context.receive({});
flush();
assert.equal(messages.filter(m=>m.type==='streamlit:setFrameHeight').length,initialHeightMessages,'Unchanged polls must not resend iframe height');
context.selectChapter(2);
assert.match(nodes.get('scene').innerHTML,/ĐÃ LƯU/);
const fixture={live:{session_id:'fixture',status:'success',started_at:'2026-10-20T00:00:00Z',steps:{etl:{state:'success',result:{raw_count:3,clean_count:2,duplicate_removed:1,elapsed_seconds:1,completed_at:'2026-10-20T00:00:01Z',output_hdfs:'fixture'}}}}};
context.receive(fixture);
assert.match(nodes.get('scene').innerHTML,/3 → 2/);
fixture.live.steps.etl.result.clean_count=1;
context.receive(fixture);
assert.match(nodes.get('scene').innerHTML,/3 → 1/,'A changed result in the same session must redraw');
context.selectChapter(3);
fixture.live.steps.etl.result.sql_sources=[{source:'<script>fixture</script>',articles:1}];
context.receive(fixture);
assert.match(nodes.get('scene').innerHTML,/&lt;script&gt;/);
assert.doesNotMatch(nodes.get('scene').innerHTML,/<script>/);
assert.match(run('box(0,0,100,65,"<script>","<&>")'), /&lt;script&gt;/);
context.selectChapter(1);
flush();
assert.equal(nodes.get('timeline-fill').parentElement.attributes['aria-valuenow'],'12');
context.togglePlay(true);flush(100);assert.equal(frames.size,1);
context.togglePlay(false);assert.equal(frames.size,0);
context.togglePlay(true);document.hidden=true;events.visibilitychange();
assert.equal(frames.size,0,'Hidden page must stop JS animation work');
now=100000;document.hidden=false;events.visibilitychange();flush(100010);
assert.ok(run('elapsed')<13,'Returning to a tab must not skip narration chapters');
context.togglePlay(false);
preference.matches=true;preference.changed();
assert.equal(nodes.get('motion').attributes['aria-pressed'],'false');
assert.ok(document.body.classes.has('motion-off'));
context.setMotion(true);
assert.equal(nodes.get('motion').attributes['aria-pressed'],'false','OS reduced-motion preference wins');
for(let i=0;i<6;i++){context.selectChapter(i);assert.equal(nodes.get('operations').children.length,3);}
assert.equal(run('TOTAL_DURATION'),105);
assert.ok(run('TOTAL_DURATION')<120);
assert.doesNotMatch(html,/LỜI DẪN/);
assert.match(html,/aria-valuemax="105"/);
preference.matches=false;preference.changed();
nodes.get('terminal-choice').checked=true;
run('elapsed=TOTAL_DURATION');
nodes.get('play').onclick();
assert.equal(run('elapsed'),0,'Replay restarts the short scenario');
assert.equal(messages.at(-1).value.action,'terminal','Replay reopens logs without starting another worker');
assert.equal(messages.at(-1).value.session_id,'fixture');
nodes.get('play').onclick();
nodes.get('terminal-open').onclick();
assert.equal(messages.at(-1).value.action,'terminal');
context.selectChapter(0);context.togglePlay(true);
for(let i=0;i<1060;i++)flush(now+100);
assert.equal(run('elapsed'),105);
assert.equal(run('playing'),false,'Scenario finishes automatically in 105 seconds');
console.log('PASS: idle/resize, result refresh, escaping, seek/play/pause, tab visibility, reduced motion, six scenes');
