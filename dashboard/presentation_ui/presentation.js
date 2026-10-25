/* Persistent component: only text/data change on Streamlit ticks; no iframe reset. */
'use strict';
const CHAPTERS=[
  {name:'Bài toán',label:'BÀI TOÁN & PHẠM VI',title:'Tin tức nhiều. Góc nhìn cần rõ.',duration:25,key:'health',badge:'MINH HỌA KIẾN TRÚC',take:'Theo dõi nguồn, chủ đề và biến động từ RSS. Mỗi số liệu truy ngược về bài gốc.',cue:'Nhóm xây dựng hệ thống thu thập và phân tích tin tức tiếng Việt. Đây là đường đi của dữ liệu, từ RSS đến dashboard; không phân tích cảm xúc độc giả.'},
  {name:'Thu thập',label:'NGUỒN TIN → HDFS',title:'Một lần thu, giữ nguyên dấu vết.',duration:30,key:'health',badge:'LIVE HDFS + INPUT ĐÃ LƯU',take:'Raw RSS đã lưu từ 08/10. Phiên này đọc lại HDFS, không chờ mạng báo và không gọi đó là crawl mới.',cue:'15 feed từ ba nguồn báo. Dữ liệu gốc được giữ trên HDFS; bên phải đang kiểm tra NameNode/DataNode thật. Một poll được chọn để xử lý trực tiếp.'},
  {name:'Làm sạch',label:'SPARK ETL / CHẠY TRỰC TIẾP',title:'Bớt bản trùng. Giữ đúng bài.',duration:45,key:'etl',badge:'ETL LIVE · OUTPUT RIÊNG',take:'Dùng đúng hàm chuẩn hóa và khử trùng của hệ thống. Đếm đầu vào, bài giữ lại, bản trùng và cách ly để đối soát.',cue:'Spark đang đọc RSS từ HDFS, chuẩn hóa và khử trùng theo khóa bài. Khi job hoàn tất, số live xuất hiện kèm thời gian và lệnh thực tế; không dùng animation làm tiến độ giả.'},
  {name:'Phân tích',label:'SPARK SQL & MODEL',title:'Từ bản ghi đến góc nhìn.',duration:40,key:'etl',badge:'SQL LIVE / MODEL ĐÃ LƯU',take:'SQL tổng hợp nguồn trên output vừa làm sạch. Model sentiment và phân cụm có bằng chứng riêng, vẫn là thử nghiệm.',cue:'Nhóm dùng Spark SQL để tổng hợp theo nguồn. Với full snapshot, TF-IDF/PCA và K-Means đã chạy; sentiment là thông tin bổ sung, chưa công bố độ chính xác độc lập.'},
  {name:'Điều phối',label:'KAFKA & AIRFLOW',title:'Dây chuyền có trạng thái, có log.',duration:35,key:'airflow',badge:'KIỂM TRA LIVE / RUN ĐÃ LƯU',take:'Topic Kafka và trạng thái Airflow được đọc trực tiếp. Chín task full-snapshot đã chạy trước, không đánh dấu lại theo đồng hồ kịch bản.',cue:'Kafka truyền dữ liệu, Spark ghi HDFS và Airflow điều phối chín task. Bên phải là lệnh kiểm tra thật; các task lịch sử ghi rõ ngày chạy, không phải chín task vừa chạy trong ba phút.'},
  {name:'Kết quả',label:'DASHBOARD & ĐỐI SOÁT',title:'Đọc được kết quả. Giải thích được số.',duration:35,key:'sqlite',badge:'SQLITE LIVE / SNAPSHOT ĐÃ LƯU',take:'Dashboard phục vụ snapshot đã kiểm tra. Integrity, count và unique keys được đọc lại; model thử nghiệm và coverage thiếu được giữ rõ.',cue:'Kết quả chính là dashboard có bộ lọc và bài gốc để xác minh. Snapshot có hơn 13 nghìn bài; demo live là một poll riêng. Hạn chế còn lại là nhãn kiểm tra độc lập và coverage.'}
];
let data={},chapter=0,elapsed=0,playing=false,receivedAt=0,lastFrame=0,sessionId=null,selectedCheck=null;
document.documentElement.dataset.instance=String(Date.now());
document.documentElement.dataset.updates='0';
const $=id=>document.getElementById(id);
const number=value=>typeof value==='number'?value.toLocaleString('vi-VN'):'—';
const local=value=>value?new Date(value).toLocaleString('vi-VN',{timeZone:'Asia/Ho_Chi_Minh',hour12:false}):'chưa có';
const text=(id,value)=>{if($(id).textContent!==String(value))$(id).textContent=String(value);};
const post=(type,extra={})=>window.parent.postMessage({isStreamlitMessage:true,type,...extra},'*');
const height=()=>post('streamlit:setFrameHeight',{height:Math.ceil(document.documentElement.scrollHeight)});
const stateNames={queued:'Chờ',running:'Đang chạy',success:'Đã xong',failed:'Lỗi'};
const checkNames={health:'HDFS',etl:'Spark ETL',kafka:'Kafka',airflow:'Airflow',sqlite:'SQLite'};
const box=(x,y,w,h,label,sub='')=>`<g><rect x="${x}" y="${y}" width="${w}" height="${h}" rx="12" fill="#f0f7f8" stroke="#9ec8ce"/><text x="${x+w/2}" y="${y+28}" text-anchor="middle" font-weight="700">${label}</text><text x="${x+w/2}" y="${y+46}" text-anchor="middle" class="label">${sub}</text></g>`;
const link=(x,y,w=110)=>`<path d="M${x} ${y}h${w}" stroke="#92b5c0" stroke-width="2" stroke-dasharray="4 6"/><g transform="translate(${x},${y-4})"><rect class="packet" width="11" height="8" rx="3" fill="#0c8f92"/><rect class="packet two" width="11" height="8" rx="3" fill="#4b82bd"/></g>`;
const papers=(x,y)=>`<g transform="translate(${x},${y})"><rect x="6" y="-6" width="60" height="78" rx="6" fill="#d5e9ef"/><rect width="60" height="78" rx="6" fill="white" stroke="#98b7c8"/><path d="M10 15h25M10 28h40M10 38h40M10 48h26" stroke="#94adbc" stroke-width="3"/><text x="10" y="67" class="micro">RSS / JSON</text></g>`;
function liveResult(){return data.live?.steps?.etl?.state==='success'?data.live.steps.etl.result:null;}
function sceneMarkup(){
 const a=data.archive,r=liveResult(),poll=a?.poll;
 let inside='';
 if(chapter===0){inside=box(12,68,110,65,'RSS','15 feed / 3 báo')+link(122,100,100)+box(228,68,110,65,'HDFS','Raw → processed')+link(338,100,100)+box(444,68,110,65,'Dashboard','SQL / insight')+`<text x="282" y="174" text-anchor="middle" class="big">${number(a?.full_count)} bài</text><text x="282" y="195" text-anchor="middle" class="label">Snapshot đóng băng · 08/10/2026</text>`;}
 else if(chapter===1){inside=papers(28,60)+link(98,100,110)+box(218,58,132,84,'NameNode','Metadata / đường dẫn')+link(350,100,110)+[0,1,2].map(i=>`<g transform="translate(477,${27+i*59})"><rect width="76" height="47" rx="6" fill="#173c53"/><rect x="9" y="10" width="58" height="9" rx="2" fill="#46697e"/><circle class="live-lamp" cx="62" cy="30" r="3" fill="#64dfd0"/><text x="10" y="34" style="fill:#e6f5fa;font-size:9px">DN ${i+1}</text></g>`).join('')+`<text x="280" y="185" text-anchor="middle" class="big">${number(poll?.raw)} observations</text><text x="280" y="207" text-anchor="middle" class="label">Raw thật từ poll08/10 · không tải lại RSS khi báo cáo</text>`;}
 else if(chapter===2){const isLive=Boolean(r);inside=papers(24,63)+link(92,99,105)+`<path d="M211 48h143l-49 57v47l-44-20v-27z" fill="#dff2ee" stroke="#0c8b82" stroke-width="2"/><path d="M237 64h90M249 80h66" stroke="#86bdb7" stroke-width="3"/>`+link(354,99,100)+box(463,60,90,68,'Parquet','Ghi + đọc lại')+`<text x="280" y="181" text-anchor="middle" class="big">${number(isLive?r.raw_count:poll?.raw)} → ${number(isLive?r.clean_count:poll?.clean)}</text><text x="280" y="204" text-anchor="middle" class="label">${isLive?'LIVE phiên này':'ĐÃ LƯU08/10 - đang chờ live'} · ${number(isLive?r.duplicate_removed:poll?.duplicates)} observations trùng</text>`;}
 else if(chapter===3){const rows=r?.sql_sources;inside=`<rect x="15" y="25" width="257" height="168" rx="10" fill="#eef5f8" stroke="#bfd4df"/><text x="29" y="47" font-weight="700">SPARK SQL ${rows?'· LIVE':'· CHƯA CHẠY'}</text>`+(rows?rows.map((row,i)=>`<text x="29" y="${76+i*38}" class="label">${escapeXML(row.source)}</text><rect x="114" y="${63+i*38}" width="${115*row.articles/Math.max(...rows.map(v=>v.articles))}" height="17" rx="3" fill="#087e83"/><text x="235" y="${76+i*38}" class="micro">${number(row.articles)}</text>`).join(''):`<text x="29" y="87" class="label">GROUP BY source</text><text x="29" y="112" class="label">Số liệu hiện khi ETL thành công</text>`)+box(301,25,247,68,'TF-IDF → PCA → K-Means','Kết quả full-run đã lưu16/10')+`<text x="425" y="137" text-anchor="middle" class="big">${a?Number(a.silhouette).toFixed(3):'—'}</text><text x="425" y="157" text-anchor="middle" class="label">silhouette · không phải accuracy</text><text x="425" y="181" text-anchor="middle" class="label">Sentiment: model thử nghiệm</text>`;}
 else if(chapter===4){inside=box(12,27,109,65,'Producer','Canonical replay')+link(122,60,100)+box(228,27,110,65,'Kafka','3 partitions')+link(339,60,100)+box(444,27,110,65,'HDFS','Stream checkpoint')+`<text x="282" y="123" text-anchor="middle" class="label">AIRFLOW · CHÍN TASK ĐÃ LƯU - KHÔNG PHẢI TIẾN ĐỘ LIVE</text>`+Array.from({length:9},(_,i)=>`<g transform="translate(${21+i*60},143)"><rect width="42" height="34" rx="7" fill="${a?'#def0e8':'#edf1f4'}" stroke="${a?'#8fbca9':'#c4cfd6'}"/><text x="21" y="22" text-anchor="middle" font-weight="700">${String(i+1).padStart(2,'0')}</text></g>`).join('')+`<text x="282" y="206" text-anchor="middle" class="label">${a?'9/9 task success · nguồn full_2a5e… · 16/10/2026':'Không đọc được bằng chứng'}</text>`;}
 else {const samples=r?.samples||a?.samples||[];inside=`<rect x="15" y="20" width="334" height="181" rx="12" fill="#fff" stroke="#a6c4d3"/><rect x="15" y="20" width="334" height="29" rx="12" fill="#14384f"/><circle cx="31" cy="35" r="3" fill="#6de0d0"/><text x="45" y="38" style="fill:#e8f3f8;font-size:10px">BÀI SAU XỬ LÝ ${r?'· LIVE':'· ĐÃ LƯU'}</text>`+samples.slice(0,3).map((v,i)=>`<text x="30" y="${73+i*42}" style="font-size:10px">${escapeXML(v.title.length>50?v.title.slice(0,47)+'…':v.title)}</text><text x="30" y="${89+i*42}" class="micro">${escapeXML(v.source)} / ${escapeXML(v.category)}</text>`).join('')+box(368,28,180,66,'Integrity / unique keys','Kiểm tra SQLite live')+box(368,113,180,72,'Giữ giới hạn thật','Coverage '+(a?.coverage||'unavailable'));}
 return `<svg viewBox="0 0 570 224" role="img" aria-label="${CHAPTERS[chapter].title} - sơ đồ minh họa">${inside}</svg>`;
}
function escapeXML(s){return String(s??'').replace(/[<>&"']/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&#39;'}[c]));}
function selectChapter(index,seek=true){
 chapter=Math.max(0,Math.min(5,index));selectedCheck=null;
 if(seek)elapsed=CHAPTERS.slice(0,chapter).reduce((s,c)=>s+c.duration,0);
 const c=CHAPTERS[chapter];
 text('chapter-label',`${String(chapter+1).padStart(2,'0')} / ${c.label}`);text('stage-title',c.title);text('evidence-badge',c.badge);text('takeaway',c.take);text('cue',c.cue);
 $('evidence-badge').classList.toggle('live',chapter===2&&Boolean(liveResult()));
 text('cue-time',formatTime(CHAPTERS.slice(0,chapter).reduce((s,c)=>s+c.duration,0))+'–'+formatTime(CHAPTERS.slice(0,chapter+1).reduce((s,c)=>s+c.duration,0)));
 document.querySelectorAll('#chapters button').forEach((b,i)=>{b.classList.toggle('active',i===chapter);b.setAttribute('aria-current',i===chapter?'step':'false');});
 $('scene').innerHTML=sceneMarkup();updateSource();renderConsole();height();
}
function updateSource(){
 const a=data.archive,r=liveResult();
 text('source-line',chapter===2?(r?'LIVE: '+r.output_hdfs:a?'ĐÃ LƯU: '+a.poll.run:'Evidence unavailable'):a?'EVIDENCE: '+a.full_run+' · '+local(a.full_date):'Evidence unavailable');
}
function renderConsole(){
 const live=data.live,steps=live?.steps||{},key=selectedCheck||CHAPTERS[chapter].key;
 const step=steps[key];
 text('command-label',`${checkNames[key]} · ${stateNames[step?.state]||'Chưa chạy'}${step?.elapsed_seconds!=null?' · '+step.elapsed_seconds+' giây':''}`);
 const content=step?`$ ${step.command||'chờ lệnh'}\n\n${step.tail||step.error||(step.state==='running'?'Đang chạy lệnh thật… log đầy đủ trong terminal riêng.':'Chờ tới lượt.')}\n${step.exit_code!=null?'\n[exit='+step.exit_code+']':''}`:'$ chưa chạy phiên kiểm tra\n\nKết quả lịch sử ở bên trái.\nTerminal này chỉ hiển thị lệnh và output của phiên live.';
 if($('console').textContent!==content){const nearEnd=$('console').scrollTop+$('console').clientHeight>=$('console').scrollHeight-25;text('console',content);if(nearEnd)$('console').scrollTop=$('console').scrollHeight;}
 document.querySelectorAll('.check').forEach(b=>{b.dataset.state=steps[b.dataset.key]?.state||'queued';b.textContent=(steps[b.dataset.key]?.state==='success'?'✓ ':steps[b.dataset.key]?.state==='failed'?'! ':steps[b.dataset.key]?.state==='running'?'› ':'')+checkNames[b.dataset.key];b.setAttribute('aria-label',checkNames[b.dataset.key]+': '+(stateNames[steps[b.dataset.key]?.state]||'Chưa chạy'));});
}
function receive(next){
 document.documentElement.dataset.updates=String(Number(document.documentElement.dataset.updates)+1);
 const oldResult=liveResult();data=next||{};receivedAt=performance.now();
 const archive=data.archive,live=data.live,result=liveResult();
 text('full-count',number(archive?.full_count));text('poll-count',number(archive?.poll?.raw));
 text('poll-detail',archive?`${number(archive.poll.clean)} bài sau khử trùng · poll08/10`:'Thiếu evidence - không điền số');
 text('verified-count',archive?`${archive.stages.length}/9 task`:'Chưa xác minh');
 text('archive-date',archive?'Đã lưu '+local(archive.full_date):'Không có manifest hợp lệ');
 text('live-count',result?number(result.clean_count)+' bài':live?.status==='running'?'Đang xử lý':live?.status==='partial'?'Có bước lỗi':live?.status==='failed'?'Lỗi phiên':'Chưa chạy');
 text('live-detail',result?`${number(result.raw_count)} → ${number(result.clean_count)} · ${result.elapsed_seconds}s · ${local(result.completed_at)}`:'ETL riêng · không thay production');
 text('session-status',live?({queued:'Phiên đang khởi động',running:'Kiểm tra live đang chạy',success:'Phiên live hoàn tất',partial:'Live có bước chưa đạt',failed:'Phiên live lỗi'}[live.status]||live.status):'Sẵn sàng trình diễn');
 text('live-clock',live?local(live.started_at):'CHƯA CHẠY');
 text('session-id',live?.session_id||'← → CHUYỂN CẢNH · SPACE TẠM DỪNG');
 const busy=['queued','running'].includes(live?.status);$('start').disabled=busy;
 text('start',busy?'Đang chạy kiểm tra…':'▶ Bắt đầu phiên demo');
 const errors=[data.archive_error,data.live_error,live?.error].filter(Boolean);
 $('alert').hidden=errors.length===0;text('alert',errors.join(' · '));
 text('source-line',chapter===2?(result?'LIVE: '+result.output_hdfs:archive?'ĐÃ LƯU: '+archive.poll.run:'Evidence unavailable'):archive?'EVIDENCE: '+archive.full_run+' · '+local(archive.full_date):'Evidence unavailable');
 if(Boolean(oldResult)!==Boolean(result)||sessionId!==live?.session_id){sessionId=live?.session_id;selectChapter(chapter,false);}
 renderConsole();height();
}
function formatTime(seconds){return String(Math.floor(seconds/60)).padStart(2,'0')+':'+String(Math.floor(seconds%60)).padStart(2,'0');}
function togglePlay(force){playing=typeof force==='boolean'?force:!playing;$('play').setAttribute('aria-pressed',String(playing));text('play',playing?'Tạm dừng kịch bản':'Phát kịch bản');document.body.classList.toggle('paused',!playing);lastFrame=performance.now();}
function animate(now){
 if(playing&&!document.hidden){elapsed=Math.min(210,elapsed+Math.max(0,Math.min((now-lastFrame)/1000,.3)));let end=0,index=0;for(let i=0;i<6;i++){end+=CHAPTERS[i].duration;if(elapsed<end){index=i;break;}index=5;}if(index!==chapter)selectChapter(index,false);if(elapsed>=210)togglePlay(false);}
 lastFrame=now;text('timer',formatTime(elapsed)+' / 03:30');$('timeline-fill').style.transform=`scaleX(${elapsed/210})`;requestAnimationFrame(animate);
}
CHAPTERS.forEach((c,i)=>{const b=document.createElement('button');b.innerHTML=`<span>${String(i+1).padStart(2,'0')}</span>${c.name}`;b.onclick=()=>selectChapter(i);$('chapters').appendChild(b);});
Object.keys(checkNames).forEach(key=>{const b=document.createElement('button');b.className='check';b.dataset.key=key;b.onclick=()=>{selectedCheck=key;renderConsole();};$('checks').appendChild(b);});
$('start').onclick=()=>{elapsed=0;selectChapter(0);togglePlay(!matchMedia('(prefers-reduced-motion: reduce)').matches);post('streamlit:setComponentValue',{value:{action:'start',terminal:$('terminal-choice').checked,nonce:Date.now()+':'+Math.random()}});$('start').disabled=true;};
$('play').onclick=()=>togglePlay();$('prev').onclick=()=>selectChapter(chapter-1);$('next').onclick=()=>selectChapter(chapter+1);
$('fullscreen').onclick=async()=>{try{if(document.fullscreenElement)await document.exitFullscreen();else await document.documentElement.requestFullscreen();}catch(e){text('fullscreen','Bấm F11 để phóng to');}height();};
$('copy-log').onclick=async()=>{try{await navigator.clipboard.writeText($('console').textContent);text('copy-log','Đã copy');}catch(e){text('copy-log','Chọn log rồi Ctrl+C');}};
document.addEventListener('keydown',e=>{if(['INPUT','BUTTON','A','PRE'].includes(e.target.tagName))return;if(e.key==='ArrowRight'){e.preventDefault();selectChapter(chapter+1);}if(e.key==='ArrowLeft'){e.preventDefault();selectChapter(chapter-1);}if(e.code==='Space'){e.preventDefault();togglePlay();}});
document.addEventListener('visibilitychange',()=>{lastFrame=performance.now();});
window.addEventListener('message',event=>{if(event.source===window.parent&&event.data.type==='streamlit:render')receive(event.data.args.data);});
window.addEventListener('resize',height);document.addEventListener('fullscreenchange',height);
document.body.classList.add('paused');selectChapter(0);requestAnimationFrame(animate);post('streamlit:componentReady',{apiVersion:1});height();
