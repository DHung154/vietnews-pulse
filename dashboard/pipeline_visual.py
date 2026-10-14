"""Scoped 2D markup driven only by observed Airflow states and stage outputs."""
from html import escape

STATIONS = (
    ('prepare','HDFS sẵn sàng','NameNode + DataNode','M5 4h14v5H5z M5 15h14v5H5z M8 7h1 M8 18h1'),
    ('upload','Nạp RSS','Raw JSON → HDFS','M6 3h8l4 4v14H6z M14 3v5h4 M9 12h6 M9 16h6'),
    ('read','Spark đọc','Schema & số dòng','M13 2L4 14h7l-1 8 10-13h-7z'),
    ('clean','Làm sạch','Chuẩn hóa & khử trùng','M4 4h16l-6 7v7l-4 3V11z'),
    ('sql','Phân tích SQL','Ngày VN · cửa sổ 6 giờ','M4 20h16 M6 17v-5 M12 17V7 M18 17V3'),
    ('cluster','Nhóm văn bản','TF-IDF + K-Means','M7 6l10 6-10 6 M7 6v12 M4 6h6 M14 12h6 M4 18h6'),
    ('save','Dự đoán & lưu','Sentiment → SQLite','M4 4h13l3 3v13H4z M8 4v6h8V4 M8 20v-6h8v6'),
    ('kafka','Luồng Kafka','Replay & checkpoint','M5 6h14 M5 12h14 M5 18h14 M15 3l4 3-4 3 M9 9l-4 3 4 3 M15 15l4 3-4 3'),
    ('verify','Đối soát cuối','Airflow · kết thúc','M12 2l8 4v6c0 5-8 10-8 10S4 17 4 12V6z M8 12l3 3 5-6'),
)

STATUS = {
    None:('waiting','Chờ xử lý'), 'success':('done','Hoàn thành'),
    'running':('active','Đang xử lý'), 'queued':('queued','Trong hàng đợi'),
    'scheduled':('queued','Đã lên lượt'), 'failed':('failed','Có lỗi'),
    'upstream_failed':('blocked','Bị chặn'), 'up_for_retry':('queued','Chờ thử lại'),
    'skipped':('blocked','Bỏ qua'),
}


def result_text(key, state, outputs):
    if state != 'success':
        return 'Đang thực thi tác vụ' if state == 'running' else 'Chưa có kết quả bước này'
    data=outputs.get(key)
    if not data:
        return 'Task xong · chưa đọc được output'
    if key=='prepare':
        return f"{data['live_datanodes']} DataNode live lúc kiểm tra"
    if key in ('upload','read'):
        return f"{data['input_rows']:,} observations".replace(',','.')
    if key=='clean':
        return f"{data['clean_count']:,} bài · loại {data['duplicate_removed']} dòng trùng".replace(',','.')
    if key=='sql':
        return f"SQL so DataFrame: {data['reference_mismatches']} khác biệt"
    if key=='cluster':
        return f"{data['k']} cụm thử nghiệm · đã lưu model"
    if key=='save':
        return f"{data['serving']['article_count']:,} bài vào SQLite riêng".replace(',','.')
    if key=='kafka':
        return f"Replay: {data['replay'][1]:,} observations / {data['replay'][2]} bài".replace(',','.')
    return f"{data['article_count']:,} bài đã đối soát".replace(',','.')


CSS = '''<style>
.vn-flow{--ink:#0f172a;--muted:#475569;--line:#cbd5e1;--teal:#0f766e;
 color:var(--ink);padding:24px;border:1px solid #dbe4ed;border-radius:20px;
 background-color:#eef4f8;background-image:radial-gradient(#cbd5e1 .7px,transparent .7px);background-size:18px 18px;}
.vn-flow *{box-sizing:border-box;}
.vn-flow .flow-heading{display:flex;justify-content:space-between;gap:16px;align-items:center;margin-bottom:8px;}
.vn-flow h3{font-size:21px;margin:0;color:var(--ink);}
.vn-flow .flow-note{font-size:13px;color:var(--muted);line-height:1.5;margin:0 0 24px;}
.vn-flow .flow-key{display:flex;flex-wrap:wrap;gap:12px;font-size:12px;color:var(--muted);margin:0 0 22px;}
.vn-flow .flow-key span{background:#fff;padding:5px 9px;border:1px solid #dbe4ed;border-radius:8px;}
.vn-flow .flow-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:32px;}
.vn-flow .station{position:relative;min-width:0;background:#fff;border:1px solid #cbd5e1;
 border-radius:14px;padding:14px 16px;min-height:160px;border-top:4px solid #94a3b8;}
.vn-flow .station.done{border-color:#86bfb1;border-top-color:#0f766e;background:#f6fffb;}
.vn-flow .station.active{border-color:#60a5fa;border-top-color:#2563eb;background:#eff6ff;}
.vn-flow .station.queued{border-top-color:#b45309;}
.vn-flow .station.failed,.vn-flow .station.blocked{border-color:#f0b5b5;border-top-color:#b91c1c;background:#fff8f8;}
.vn-flow .station-top{display:flex;align-items:center;justify-content:space-between;gap:6px;margin-bottom:10px;}
.vn-flow .station-number{font:700 12px ui-monospace,monospace;color:#475569;}
.vn-flow .state{font-size:11px;font-weight:650;border-radius:6px;padding:3px 6px;background:#e2e8f0;color:#334155;}
.vn-flow .done .state{background:#ccf1e5;color:#115e59;}
.vn-flow .active .state{background:#dbeafe;color:#1e40af;}
.vn-flow .queued .state{background:#fef3c7;color:#92400e;}
.vn-flow .failed .state,.vn-flow .blocked .state{background:#fee2e2;color:#991b1b;}
.vn-flow .station-body{display:flex;gap:10px;align-items:center;}
.vn-flow .machine{flex:0 0 40px;width:40px;height:40px;border:1px solid #cbd5e1;border-radius:10px;
 display:flex;align-items:center;justify-content:center;background:#f1f5f9;color:#475569;}
.vn-flow .done .machine{color:#0f766e;background:#def5ed;border-color:#adddce;}
.vn-flow .active .machine{color:#1d4ed8;background:#dbeafe;border-color:#93c5fd;}
.vn-flow svg{width:25px;height:25px;display:block;}
.vn-flow h4{font-size:16px;line-height:1.3;margin:0;color:#0f172a;overflow-wrap:anywhere;}
.vn-flow .station-desc{font-size:12px;line-height:1.5;color:var(--muted);margin-top:3px;}
.vn-flow .station-result{font-size:12px;color:#334155;line-height:1.5;margin-top:11px;overflow-wrap:anywhere;}
.vn-flow .belt{position:absolute;width:32px;height:14px;right:-33px;top:75px;color:#94a3b8;}
.vn-flow .belt:before{content:"";position:absolute;top:6px;left:0;right:0;border-top:2px dashed currentColor;}
.vn-flow .belt:after{content:"";position:absolute;right:3px;top:3px;width:8px;height:8px;border-top:2px solid currentColor;border-right:2px solid currentColor;transform:rotate(45deg);}
.vn-flow .done .belt{color:#0f766e;}
.vn-flow .active .belt{color:#2563eb;}
.vn-flow .active .packet{position:absolute;width:6px;height:6px;background:#2563eb;top:4px;border-radius:2px;animation:vn-packet 1.7s linear infinite;}
.vn-flow .s3 .belt,.vn-flow .s6 .belt{top:auto;bottom:-24px;left:calc(50% - 16px);transform:rotate(90deg);}
.vn-flow .s4 .belt,.vn-flow .s5 .belt{right:auto;left:-33px;transform:rotate(180deg);}
.vn-flow .s4{grid-column:3;grid-row:2;}.vn-flow .s5{grid-column:2;grid-row:2;}.vn-flow .s6{grid-column:1;grid-row:2;}
.vn-flow .s7{grid-column:1;grid-row:3;}.vn-flow .s8{grid-column:2;grid-row:3;}.vn-flow .s9{grid-column:3;grid-row:3;}
.vn-flow .processing{height:3px;background:#bfdbfe;border-radius:4px;overflow:hidden;margin-top:10px;}
.vn-flow .processing i{display:block;width:35%;height:100%;background:#2563eb;animation:vn-process 1.7s ease-in-out infinite;}
.vn-flow .flow-footer{font-size:12px;color:#475569;line-height:1.6;margin-top:20px;}
@keyframes vn-packet{from{left:0;opacity:.3}to{left:24px;opacity:1}}
@keyframes vn-process{from{transform:translateX(-100%)}to{transform:translateX(400%)}}
.vn-flow.motion-off .packet,.vn-flow.motion-off .processing i{animation:none;}
@media(prefers-reduced-motion:reduce){.vn-flow .packet,.vn-flow .processing i{animation:none!important;}}
@media(max-width:700px){.vn-flow{padding:16px;}.vn-flow .flow-grid{gap:26px;grid-template-columns:1fr;}
 .vn-flow .station{grid-row:auto;grid-column:1;min-height:145px;}
 .vn-flow .station .belt{left:calc(50% - 16px);right:auto;top:auto;bottom:-21px;transform:rotate(90deg);}}
</style>'''


def render_pipeline(rows, outputs, animate=True):
    by_step={row['task_id']:row for row in rows}
    cards=[]
    for number,(key,title,subtitle,path) in enumerate(STATIONS,1):
        state=by_step.get(key,{}).get('state')
        kind,label=STATUS.get(state,('waiting','Chưa rõ trạng thái'))
        result=result_text(key,state,outputs)
        icon=f'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="{path}"/></svg>'
        processing='<div class="processing" aria-hidden="true"><i></i></div>' if kind=='active' else ''
        belt='<span class="belt" aria-hidden="true"><i class="packet"></i></span>' if number<9 else ''
        cards.append(f'<article class="station s{number} {kind}" data-stage="{key}"><div class="station-top"><span class="station-number">TRẠM {number:02}</span><span class="state">{escape(label)}</span></div><div class="station-body"><div class="machine">{icon}</div><div><h4>{escape(title)}</h4><div class="station-desc">{escape(subtitle)}</div></div></div><div class="station-result">{escape(result)}</div>{processing}{belt}</article>')
    motion='' if animate else ' motion-off'
    return CSS+f'<section class="vn-flow{motion}" aria-label="Dây chuyền xử lý dữ liệu"><div class="flow-heading"><h3>Dây chuyền dữ liệu</h3></div><p class="flow-note">Theo số trạm 01 → 09 · trạng thái lấy từ Airflow của lượt demo hiện tại</p><div class="flow-key"><span>Chờ</span><span>Đang xử lý</span><span>Hoàn thành</span><span>Lỗi / bị chặn</span></div><div class="flow-grid">'+''.join(cards)+'</div><div class="flow-footer">Chuyển động chỉ minh họa tác vụ đang chạy, không biểu thị tốc độ hoặc phần trăm xử lý trong tác vụ. Số liệu chỉ hiện khi có output của bước đã hoàn thành.</div></section>'
