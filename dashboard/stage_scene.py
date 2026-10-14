"""Stage-specific 2D illustrations; motion is explanatory, not task telemetry."""
from html import escape
from dashboard.pipeline_visual import STATIONS, STATUS, result_text

DETAILS = {
 'prepare':('Khởi tạo vùng dữ liệu cho lượt demo','NameNode quản lý đường dẫn; các DataNode lưu block dữ liệu.','VÙNG HDFS','3 DATANODE'),
 'upload':('Đưa file RSS vào HDFS','Đọc manifest, đối chiếu checksum và sao file raw vào vùng của run mới.','FILE RSS','HDFS / RAW'),
 'read':('Spark đọc các bản ghi JSON','Áp dụng schema cho dữ liệu RSS và đếm observations để đối soát đầu vào.','JSON / RAW','SPARK READER'),
 'clean':('Chuẩn hóa và tách dòng trùng','Chuẩn hóa văn bản/thời gian; chọn nội dung mới nhất và giữ quarantine riêng.','DỮ LIỆU THÔ','CANONICAL'),
 'sql':('Tổng hợp dữ liệu bằng Spark SQL','Nhóm theo ngày Việt Nam; so hai cửa sổ 6 giờ và đối chiếu công thức DataFrame.','BÀI ĐÃ SẠCH','BẢNG TỔNG HỢP'),
 'cluster':('Biến văn bản thành vector rồi phân cụm','TF-IDF mô tả từ ngữ; K-Means gom các vector gần nhau, không xác nhận cùng sự kiện.','VECTOR TF-IDF','CỤM THỬ NGHIỆM'),
 'save':('Dự đoán sentiment và lưu kết quả','Tải model sentiment sẵn có để inference; ghi Parquet/gold và SQLite của run riêng.','BÀI ĐÃ SẠCH','PARQUET + SQLITE'),
 'kafka':('Truyền observations qua Kafka','Producer gửi bản ghi; Spark đọc các partition và phục hồi offset qua checkpoint.','PRODUCER','SPARK CONSUMER'),
 'verify':('Đối soát dữ liệu sau xử lý','Kiểm tra integrity SQLite, khóa bài duy nhất, stage evidence và hash raw nguồn.','OUTPUT CỦA RUN','KẾT QUẢ ĐỐI SOÁT'),
}


def focus_stage(rows):
    states={r['task_id']:r.get('state') for r in rows}
    keys=[s[0] for s in STATIONS]
    for group in (('failed',),('running',),('queued','scheduled','up_for_retry')):
        for key in keys:
            if states.get(key) in group:
                return key
    return next((k for k in keys if states.get(k)!='success'),'verify')


def document(x, y, tag='RSS', color='#60a5fa', extra=''):
    return (f'<g transform="translate({x},{y})"><g class="{extra}">'
        '<rect x="5" y="-5" width="50" height="68" rx="6" fill="#223c57"/>'
        '<rect width="50" height="68" rx="6" fill="#f5f9ff" stroke="#bfd5e9"/>'
        f'<rect x="8" y="9" width="25" height="5" rx="2" fill="{color}"/>'
        '<path d="M8 24h32M8 31h32M8 38h22" stroke="#a1b4ca" stroke-width="2"/>'
        f'<text x="8" y="56" class="doc-tag">{escape(tag)}</text></g></g>')


TECH = {
    'prepare':('HDFS CLUSTER','NameNode → block storage','Đường dẫn riêng cho mỗi lượt demo'),
    'upload':('RAW INGEST','Manifest → checksum → copy','File nguồn được giữ nguyên'),
    'read':('SPARK READER','JSON → schema → DataFrame','Đối soát số observations'),
    'clean':('CANONICAL ENGINE','Normalize → deduplicate','Nội dung mới nhất · category riêng'),
    'sql':('SPARK SQL','GROUP BY · completed windows','Ngày Việt Nam · hai cửa sổ 6 giờ'),
    'cluster':('TEXT CLUSTERING','TF-IDF → K-Means','Vị trí điểm chỉ để minh họa'),
    'save':('INFERENCE & STORAGE','Model có sẵn → dự đoán → lưu','Không huấn luyện lại sentiment'),
    'kafka':('KAFKA BROKER','Producer → partitions → consumer','Offset được lưu trong checkpoint'),
    'verify':('QUALITY GATE','Integrity → unique keys → hashes','Đối soát output của chính lượt chạy'),
}


def illustration(key):
    # Schematic shapes only; no per-record telemetry or model coordinates.
    if key=='prepare':
        racks=''.join(f'<g transform="translate({x},145)"><rect width="86" height="94" rx="8" fill="#203c55" stroke="#52718c"/>'+''.join(f'<rect x="9" y="{10+j*25}" width="68" height="17" rx="3" fill="#102438"/><circle class="lamp l{i}" cx="64" cy="{18+j*25}" r="3" fill="#5eead4"/>' for j in range(3))+f'<text x="43" y="116" text-anchor="middle" class="svg-muted">DN {i+1}</text></g>' for i,x in enumerate((325,437,549)))
        return '<path d="M367 138v-9h224v9M480 120v9" stroke="#5eead4" fill="none"/>'+racks
    if key=='upload':
        return ('<rect x="323" y="152" width="316" height="76" rx="12" fill="#102438" stroke="#446681"/>'
            '<path d="M339 192h281" stroke="#4b718e" stroke-dasharray="4 8"/>'
            +''.join(f'<g class="paper p{i}"><rect x="342" y="174" width="30" height="36" rx="5" fill="#eaf7ff"/><path d="M349 184h16M349 191h16M349 198h10" stroke="#28849a" stroke-width="2"/></g>' for i in range(3)))
    if key=='read':
        return ('<rect x="319" y="144" width="322" height="101" rx="8" fill="#0f2236" stroke="#42627d"/>'
            '<text x="335" y="167" class="code-line">{ title_raw, source, link_raw }</text>'
            '<text x="335" y="192" class="code-line">{ published_at_raw, crawled_at }</text>'
            '<text x="335" y="217" class="code-line">{ dataset_id, observation_id }</text>'
            '<rect class="scan" x="323" y="149" width="314" height="25" rx="4" fill="#5eead4" opacity=".18"/>')
    if key=='clean':
        return ('<path d="M303 183h96M505 177l80-29M505 189l80 42" stroke="#42627d" stroke-width="2" fill="none"/>'
            '<path d="M398 145h110l-39 48v36l-31-16v-20z" fill="#1b514f" stroke="#5eead4" stroke-width="2"/>'
            '<path d="M422 159h62M433 174h40" stroke="#81ddd0" stroke-width="2"/>'
            '<rect x="574" y="129" width="92" height="31" rx="7" fill="#174e48" stroke="#408477"/><text x="620" y="150" text-anchor="middle">Giữ lại</text>'
            '<rect x="574" y="218" width="92" height="31" rx="7" fill="#4b3826" stroke="#ad8051"/><text x="620" y="239" text-anchor="middle" class="svg-amber">Cách ly</text>'
            '<rect class="clean-paper" x="314" y="169" width="18" height="25" rx="3" fill="#5eead4"/>'
            '<rect class="reject-paper" x="314" y="173" width="17" height="23" rx="3" fill="#fbbf77"/>')
    if key=='sql':
        return ('<path d="M324 238h314M324 238V142" stroke="#52718c"/>'
            '<path d="M324 206h314M324 174h314" stroke="#29465f" stroke-dasharray="4 5"/>'
            +''.join(f'<rect class="bar b{i}" x="{352+i*62}" y="{238-h}" width="34" height="{h}" rx="5" fill="{color}"/>' for i,(h,color) in enumerate(((44,'#4ca5ae'),(68,'#5eead4'),(54,'#73a9ef'),(89,'#3b82f6')))))
    if key=='cluster':
        islands=''.join(f'<circle cx="{x}" cy="{y}" r="38" fill="{color}" fill-opacity=".09" stroke="{color}" stroke-opacity=".4" stroke-dasharray="4 5"/>' for x,y,color in ((365,185,'#5eead4'),(480,185,'#93c5fd'),(595,185,'#fbbf77')))
        dots=''.join(f'<g class="dot d{i}"><circle cx="{420+(i%4)*40}" cy="{155+(i//4)*30}" r="6" fill="{("#5eead4","#93c5fd","#fbbf77")[i%3]}"/></g>' for i in range(12))
        return islands+dots
    if key=='save':
        return ('<path d="M320 190h280" stroke="#52718c" stroke-dasharray="4 6"/>'
            '<rect x="327" y="144" width="106" height="91" rx="10" fill="#203c55" stroke="#52718c"/>'
            '<text x="380" y="181" text-anchor="middle">MODEL</text><text x="380" y="207" text-anchor="middle" class="svg-muted">Inference</text>'
            '<path d="M514 160v65c0 26 104 26 104 0v-65" fill="#154b49" stroke="#68c7b9"/>'
            '<ellipse cx="566" cy="160" rx="52" ry="15" fill="#25615b" stroke="#68c7b9"/>'
            '<path d="M514 190c4 21 100 21 104 0M514 215c4 21 100 21 104 0" stroke="#68c7b9" fill="none"/>'
            '<rect class="write-paper" x="437" y="177" width="20" height="27" rx="3" fill="#e0f2fe"/>')
    if key=='kafka':
        return ''.join(f'<g><rect x="323" y="{138+i*39}" width="316" height="30" rx="6" fill="#102438" stroke="#42627d"/><text x="335" y="{159+i*39}" class="code-line">P{i}</text><path d="M369 {153+i*39}h250" stroke="#355671" stroke-dasharray="3 7"/><rect class="kpacket kp{i}" x="376" y="{145+i*39}" width="24" height="16" rx="4" fill="#5eead4"/></g>' for i in range(3))
    return ''.join(f'<g><rect x="322" y="{132+i*30}" width="318" height="25" rx="5" fill="#152e40"/><path d="M337 {143+i*30}l4 5 8-10" class="check c{i}" stroke="#5eead4" stroke-width="2" fill="none"/><text x="366" y="{150+i*30}">{label}</text></g>' for i,label in enumerate(('Integrity SQLite','Article ID duy nhất','Hash raw không đổi','Đủ evidence các bước')))


SCENE_CSS='''<style>
.stage-scene{--scene-ink:#14283d;--scene-muted:#50647a;--scene-teal:#0f766e;
 border:1px solid #d8e3eb;border-radius:20px;background:#fff;color:var(--scene-ink);margin:8px 0 20px;
 box-shadow:0 8px 28px #14283d08;overflow:hidden;}
.stage-scene *{box-sizing:border-box;}
.stage-scene .scene-head{padding:24px 26px 20px;}
.stage-scene .scene-eyebrow{display:flex;align-items:center;gap:12px;justify-content:space-between;flex-wrap:wrap;}
.stage-scene .scene-kicker{font:700 11px system-ui,sans-serif;letter-spacing:.11em;color:var(--scene-teal);}
.stage-scene .scene-state{font-size:11px;font-weight:650;color:#115e59;background:#e2f5ef;border:1px solid #b6dfd1;padding:5px 10px;border-radius:20px;}
.stage-scene.failed .scene-state,.stage-scene.blocked .scene-state{background:#fff0ef;color:#991b1b;border-color:#f3c5bf;}
.stage-scene.active .scene-state{background:#eff6ff;color:#1e40af;border-color:#bfdbfe;}
.stage-scene.preview .scene-state{background:#edf6ff;color:#245581;border-color:#cadff2;}
.stage-scene h3{font:650 24px/1.35 system-ui,sans-serif;color:var(--scene-ink);margin:12px 0 8px;letter-spacing:-.025em;}
.stage-scene .scene-description{font:14px/1.6 system-ui,sans-serif;color:var(--scene-muted);margin:0;max-width:80ch;}
.stage-scene .scene-canvas{background:#101f33;padding:4px 10px 0;}
.stage-scene .scene-canvas svg{display:block;width:100%;height:auto;min-width:0;}
.stage-scene svg text{font:14px system-ui,sans-serif;fill:#e3eff9;}
.stage-scene svg .svg-muted{font-size:12px;fill:#b0c5d7;}
.stage-scene svg .svg-amber{fill:#ffcf91;}
.stage-scene svg .svg-kicker{font:600 10px ui-monospace,monospace;letter-spacing:1.4px;fill:#a7bed1;}
.stage-scene svg .tech-title{font:600 12px ui-monospace,monospace;letter-spacing:1px;fill:#5eead4;}
.stage-scene svg .endpoint-label{font-weight:650;font-size:14px;}
.stage-scene svg .doc-tag{font:700 9px ui-monospace,monospace;fill:#345471;}
.stage-scene svg .code-line{font:12px ui-monospace,monospace;fill:#c0d7e9;}
.stage-scene .scene-canvas-note{font:11px/1.5 system-ui,sans-serif;color:#b0c5d7;padding:0 16px 16px;}
.stage-scene .scene-footer{display:flex;align-items:center;justify-content:space-between;gap:18px;padding:18px 26px;background:#f7fafc;border-top:1px solid #dfe7ee;}
.stage-scene .scene-result-label{font:600 10px system-ui,sans-serif;color:#50647a;letter-spacing:.1em;margin-bottom:5px;}
.stage-scene .scene-result{font:600 14px/1.5 system-ui,sans-serif;color:var(--scene-teal);}
.stage-scene .scene-mode{font:12px/1.5 system-ui,sans-serif;color:#50647a;max-width:200px;text-align:right;}
.stage-scene .scene-mobile{display:none;}
.stage-scene .scene-rail{display:flex;gap:8px;border-top:1px solid #dfe7ee;padding:14px 26px;justify-content:space-between;}
.stage-scene .rail-stop{flex:1;text-align:center;border-top:2px solid #d8e3eb;padding-top:8px;font:11px/1.6 system-ui,sans-serif;color:#50647a;}
.stage-scene .rail-stop.done{border-color:#5baf98;}.stage-scene .rail-stop.chosen{border-color:#0f766e;color:#0f766e;font-weight:700;background:#f0faf6;border-radius:0 0 6px 6px;}
.stage-scene .rail-stop.failed,.stage-scene .rail-stop.blocked{border-color:#c76868;}
.stage-scene .lamp{animation:scene-lamp 1.8s ease-in-out infinite;}.stage-scene .l1{animation-delay:.3s;}.stage-scene .l2{animation-delay:.6s;}
.stage-scene .paper{animation:scene-transfer 2.7s linear infinite;}.stage-scene .p1{animation-delay:-.9s;}.stage-scene .p2{animation-delay:-1.8s;}
.stage-scene .scan{animation:scene-scan 2s ease-in-out infinite alternate;}
.stage-scene .clean-paper{animation:scene-clean 3.2s linear infinite;}.stage-scene .reject-paper{animation:scene-reject 3.2s linear infinite;animation-delay:-1.6s;}
.stage-scene .bar{transform-box:fill-box;transform-origin:bottom;animation:scene-bar 2.8s ease-in-out infinite alternate;}.stage-scene .b1{animation-delay:-.3s;}.stage-scene .b2{animation-delay:-.6s;}.stage-scene .b3{animation-delay:-.9s;}
.stage-scene .dot{animation:scene-cluster 3.4s ease-in-out infinite alternate;}
.stage-scene .d0{--dx:-55px;--dy:30px}.stage-scene .d3{--dx:-165px;--dy:25px}.stage-scene .d6{--dx:-128px;--dy:10px}.stage-scene .d9{--dx:-92px;--dy:-13px}
.stage-scene .d1{--dx:6px;--dy:12px}.stage-scene .d4{--dx:72px;--dy:0px}.stage-scene .d7{--dx:-71px;--dy:5px}.stage-scene .d10{--dx:-15px;--dy:-19px}
.stage-scene .d2{--dx:77px;--dy:24px}.stage-scene .d5{--dx:148px;--dy:-13px}.stage-scene .d8{--dx:168px;--dy:-20px}.stage-scene .d11{--dx:46px;--dy:-42px}
.stage-scene .write-paper{animation:scene-write 2.3s linear infinite;}
.stage-scene .kpacket{animation:scene-kafka 2.5s linear infinite;}.stage-scene .kp1{animation-delay:-.8s;}.stage-scene .kp2{animation-delay:-1.6s;}
.stage-scene .check{stroke-dasharray:30;animation:scene-check 3.2s ease-in-out infinite;}.stage-scene .c1{animation-delay:-.4s;}.stage-scene .c2{animation-delay:-.8s;}.stage-scene .c3{animation-delay:-1.2s;}
.stage-scene svg *{animation-play-state:paused;}.stage-scene.moving svg *{animation-play-state:running;}.stage-scene:not(.moving) svg *{animation:none;}
@keyframes scene-lamp{50%{opacity:.3}}@keyframes scene-transfer{0%{transform:translateX(0);opacity:0}15%{opacity:1}85%{opacity:1}100%{transform:translateX(230px);opacity:0}}
@keyframes scene-scan{to{transform:translateY(63px)}}
@keyframes scene-clean{0%{transform:translate(0,0);opacity:0}12%{opacity:1}46%{transform:translate(130px,0)}90%{opacity:1}100%{transform:translate(283px,-39px);opacity:0}}
@keyframes scene-reject{0%{transform:translate(0,0);opacity:0}12%{opacity:1}46%{transform:translate(130px,0)}90%{opacity:1}100%{transform:translate(283px,52px);opacity:0}}
@keyframes scene-bar{from{transform:scaleY(.45)}to{transform:scaleY(1)}}@keyframes scene-cluster{to{transform:translate(var(--dx),var(--dy))}}
@keyframes scene-write{to{transform:translateX(110px);opacity:0}}@keyframes scene-kafka{to{transform:translateX(225px)}}@keyframes scene-check{0%,20%{stroke-dashoffset:30}50%,100%{stroke-dashoffset:0}}
@media(prefers-reduced-motion:reduce){.stage-scene svg *{animation:none!important;}}
@media(max-width:600px){.stage-scene .scene-head{padding:18px}.stage-scene h3{font-size:20px}.stage-scene .scene-canvas svg{display:none;}
 .stage-scene .scene-mobile{display:grid;gap:10px;padding:18px;color:#e3eff9;font:14px/1.5 system-ui,sans-serif;}
 .stage-scene .scene-mobile div{padding:12px;border:1px solid #42627d;border-radius:8px;}.stage-scene .scene-mobile small{display:block;color:#b0c5d7;font-size:11px;}
 .stage-scene .scene-footer{padding:16px;display:block}.stage-scene .scene-mode{text-align:left;margin-top:8px;max-width:none;}
 .stage-scene .scene-rail{padding:10px;gap:3px;}.stage-scene .rail-stop{font-size:10px}.stage-scene .rail-name{display:none;}}
</style>'''


def render_scene(rows,outputs,animate=True,preview=None):
    key=preview or focus_stage(rows)
    if key not in DETAILS:
        raise ValueError('Unknown scene')
    index=next(i for i,s in enumerate(STATIONS,1) if s[0]==key)
    state=next((r.get('state') for r in rows if r['task_id']==key),None)
    kind,label=STATUS.get(state,('waiting','Chưa rõ trạng thái'))
    title,description,source,target=DETAILS[key]
    moving=animate and (preview is not None or state=='running')
    kicker=f'MINH HỌA CÔNG ĐOẠN {index:02} — KHÔNG CHẠY LẠI PIPELINE' if preview else f'CÔNG ĐOẠN {index:02}/09 · {label.upper()}'
    tech,operation,annotation=TECH[key]
    foot='Animation giải thích chức năng · không phải telemetry, tốc độ hoặc phần trăm trong task.'
    source_art=document(84,151,'PLAN' if key=='prepare' else 'JSON')
    output_art=(
        '<rect x="793" y="149" width="100" height="77" rx="6" fill="#162e43" stroke="#42627d"/>'
        '<path d="M793 168h100M793 187h100M793 206h100M826 149v77M859 149v77" stroke="#42627d"/>'
        '<path d="M801 160h16M834 160h16M868 160h16" stroke="#5eead4" stroke-width="3"/>'
    )
    scene=(f'<svg viewBox="0 0 960 340" role="img" aria-label="{escape(title,quote=True)}">'
        '<defs><pattern id="scene-grid" width="24" height="24" patternUnits="userSpaceOnUse"><circle cx="1" cy="1" r=".7" fill="#34516c"/></pattern></defs>'
        '<rect width="960" height="340" fill="url(#scene-grid)"/>'
        '<text x="24" y="33" class="svg-kicker">VIETNEWS / DATA WORKSHOP</text>'
        '<text x="936" y="33" text-anchor="end" class="svg-kicker">SƠ ĐỒ CHỨC NĂNG</text>'
        '<path d="M207 184h44M709 184h44" stroke="#52718c" stroke-width="2"/>'
        '<path d="M240 180l5 4-5 4M741 180l5 4-5 4" fill="none" stroke="#5eead4" stroke-width="2"/>'
        '<rect x="24" y="76" width="182" height="202" rx="14" fill="#192f46" stroke="#34516c"/>'
        '<rect x="754" y="76" width="182" height="202" rx="14" fill="#192f46" stroke="#34516c"/>'
        '<rect x="259" y="76" width="442" height="202" rx="14" fill="#172b43" stroke="#42627d"/>'
        '<text x="42" y="99" class="svg-kicker">01 / ĐẦU VÀO</text>'
        f'<text x="42" y="123" class="endpoint-label">{escape(source)}</text>{source_art}'
        '<text x="115" y="254" text-anchor="middle" class="svg-muted">Giữ dữ liệu gốc</text>'
        '<text x="772" y="99" class="svg-kicker">03 / ĐẦU RA</text>'
        f'<text x="772" y="123" class="endpoint-label">{escape(target)}</text>{output_art}'
        '<text x="845" y="254" text-anchor="middle" class="svg-muted">Output của công đoạn</text>'
        f'<text x="279" y="99" class="tech-title">02 / {escape(tech)}</text>'
        f'<text x="480" y="121" text-anchor="middle" class="svg-muted">{escape(operation)}</text>'
        f'{illustration(key)}<text x="480" y="309" text-anchor="middle" class="svg-muted">{escape(annotation)}</text></svg>')
    states={r['task_id']:r.get('state') for r in rows}
    short=('HDFS','RSS','Spark','ETL','SQL','ML','Lưu','Kafka','Audit')
    rail=''.join(f'<div class="rail-stop {STATUS.get(states.get(s[0]),("waiting",""))[0]}{" chosen" if s[0]==key else ""}" title="{escape(s[1])}: {STATUS.get(states.get(s[0]),("waiting","Chưa rõ"))[1]}" aria-label="{i+1}. {escape(s[1])}: {STATUS.get(states.get(s[0]),("waiting","Chưa rõ"))[1]}"><span>{i+1:02}</span><div class="rail-name">{name}</div></div>' for i,(s,name) in enumerate(zip(STATIONS,short)))
    mode='Minh họa được chọn thủ công' if preview else 'Đồng bộ trạng thái Airflow'
    badge='Chế độ minh họa' if preview else label
    mobile=f'<div class="scene-mobile"><div><small>ĐẦU VÀO</small>{escape(source)}</div><div><small>XỬ LÝ</small>{escape(operation)}</div><div><small>ĐẦU RA</small>{escape(target)}</div></div>'
    return (SCENE_CSS+f'<section class="stage-scene{" moving" if moving else ""} {kind}{" preview" if preview else ""}" data-scene="{key}">'
        f'<div class="scene-head"><div class="scene-eyebrow"><div class="scene-kicker">{escape(kicker)}</div><div class="scene-state">{escape(badge)}</div></div>'
        f'<h3>{escape(title)}</h3><p class="scene-description">{escape(description)}</p></div>'
        f'<div class="scene-canvas">{scene}{mobile}<div class="scene-canvas-note">{escape(foot)}</div></div>'
        f'<div class="scene-footer"><div><div class="scene-result-label">KẾT QUẢ TỪ LƯỢT ĐANG XEM</div><div class="scene-result">{escape(result_text(key,state,outputs))}</div></div><div class="scene-mode">{escape(mode)}</div></div>'
        f'<div class="scene-rail" aria-label="Thứ tự chín công đoạn">{rail}</div></section>')
