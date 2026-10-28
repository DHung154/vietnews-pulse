"""Presentation console: immutable lab evidence, separate from live read checks."""
import json
import sqlite3
import subprocess
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
RUN = 'airflow_181ed4cba01a3cab'
WORK = ROOT / 'runtime/nine_steps' / RUN
HDFS = '/news/lab/' + RUN
STEPS = (
    ('prepare','Chuẩn bị HDFS','NameNode giữ metadata, ba DataNode giữ block; đây là ba node logic trên một máy.'),
    ('upload','Đưa RSS vào HDFS','Đối chiếu hash từng file RSS thật trước khi upload, không sửa raw nguồn.'),
    ('read','Spark đọc HDFS','Spark đọc JSON với schema có sẵn, đối soát observations với manifest.'),
    ('clean','Làm sạch và khử trùng','Giữ nội dung mới nhất, giải quyết category riêng; một article_id được đếm một lần.'),
    ('sql','Spark SQL','SQL tổng hợp theo ngày Việt Nam và hai cửa sổ 6 giờ đã kết thúc; đối chiếu DataFrame.'),
    ('cluster','TF-IDF và K-Means','Thay RFM khách hàng bằng phân cụm tin tức; cụm chưa phải sự kiện đã xác minh.'),
    ('save','Lưu kết quả và sentiment','Lưu Parquet/model trên HDFS, SQLite phục vụ riêng; chỉ inference model sentiment đang có.'),
    ('kafka','Kafka ingest và restart','Phát lại observations, đọc bằng Structured Streaming; checkpoint giữ offset, canonical chống trùng bài.'),
    ('verify','Airflow và đối soát cuối','Chín task chạy tuần tự; kiểm tra SQLite, khóa bài và raw không đổi.'))


def evidence(step, work=None):
    if step not in {s[0] for s in STEPS}:
        raise ValueError('Unknown stage')
    return json.loads(((WORK if work is None else work) / (step+'.json')).read_text(encoding='utf-8'))


def command(step):
    docker = ['docker','--config',str(ROOT/'runtime/docker-config')]
    if step == 'prepare':
        return docker+['compose','exec','-T','namenode','hdfs','dfsadmin','-report']
    if step == 'kafka':
        return docker+['compose','-f','compose.nine-steps.yaml','exec','-T','kafka',
            '/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:29092',
            '--describe','--topic','vietnews-lab-'+RUN]
    if step == 'verify':
        return docker+['compose','-f','compose.nine-steps.yaml','exec','-T','airflow',
            'airflow','tasks','states-for-dag-run','vietnews_nine_steps',
            'manual_review_20261008T100700Z','--output','json']
    paths = {'upload':'raw/rss','read':'raw/rss','clean':'clean','sql':'sql',
             'cluster':'clustering/model','save':'gold'}
    return docker+['compose','exec','-T','namenode','hdfs','dfs','-ls',HDFS+'/'+paths[step]]


def main():
    import streamlit as st
    st.set_page_config(page_title='VietNews | Demo Big Data',layout='wide',initial_sidebar_state='collapsed')
    st.markdown('''<style>.stApp {background:#f3f6fa;color:#14283d;}
    h1,h2,h3 {color:#14283d;} h1 {font-size:2rem;letter-spacing:-.035em;}
    .block-container {max-width:1280px;padding-top:1.6rem;}
    button[kind="primary"] {background:#0f766e;border-color:#0f766e;}
    button[kind="primary"]:hover {background:#115e59;border-color:#115e59;}
    button:focus-visible {outline:3px solid #5aa9b4;outline-offset:3px;}
    </style>''',unsafe_allow_html=True)
    st.sidebar.header('Demo đồ án')
    mode=st.sidebar.selectbox('Chế độ',['Demo nhanh · 1 phút 45 giây','Chạy toàn pipeline','Xem chi tiết 9 bước'])
    if mode=='Demo nhanh · 1 phút 45 giây':
        from dashboard.presentation import panel
        st.markdown('<style>.block-container{max-width:1400px;padding-top:3.7rem;padding-left:1.5rem;padding-right:1.5rem;padding-bottom:.5rem;}</style>',unsafe_allow_html=True)
        panel(st)
        return
    st.title('Demo đồ án · VietNews Pulse')
    st.caption('RSS → HDFS → Spark → phân tích → SQLite/dashboard | Kafka và Airflow lab')
    if mode=='Chạy toàn pipeline':
        pipeline_panel(st)
        return
    st.warning('Evidence là lượt chạy ngày 08/10/2026, không phải trạng thái live. Sentiment và grouping vẫn thử nghiệm.')
    index = st.sidebar.radio('Chọn chức năng',range(9),format_func=lambda i:f'{i+1}. {STEPS[i][1]}')
    st.sidebar.link_button('Dashboard tin tức','http://localhost:8502')
    st.sidebar.link_button('NameNode · máy trình diễn','http://localhost:9870')
    st.sidebar.link_button('Airflow · máy trình diễn','http://localhost:8088')
    st.sidebar.caption('Các liên kết localhost chỉ dành cho máy đang trình diễn. Không tự bật service hoặc chạy job ghi từ trang này.')
    loaded = {}
    for key,_,_ in STEPS:
        try:
            loaded[key] = evidence(key)
        except (OSError,ValueError) as exc:
            st.error(f'Thiếu hoặc lỗi evidence {key}: {exc}')
    if len(loaded) != 9:
        st.info('Không đủ evidence để trình bày trọn pipeline. Không điền số liệu thay thế.')
        return
    canonical = loaded['clean']
    kpi = st.columns(4)
    kpi[0].metric('Observations đầu vào',canonical['raw_count'])
    kpi[1].metric('Bài sau khử trùng',canonical['clean_count'])
    kpi[2].metric('Observations trùng',canonical['duplicate_removed'])
    kpi[3].metric('Stage có evidence',f'{len(loaded)}/9')
    key,title,explanation = STEPS[index]
    st.divider()
    st.subheader(f'{index+1}. {title}')
    st.write(explanation)
    data = loaded[key]
    stamp = datetime.fromisoformat(data['completed_at']).astimezone(ZoneInfo('Asia/Ho_Chi_Minh'))
    st.caption(f'Evidence ghi lúc {stamp:%d/%m/%Y %H:%M:%S} giờ Việt Nam · status={data["status"]} · Spark local[2]')
    left,right=st.columns([3,2])
    with left:
        st.markdown('#### Kết quả đã ghi nhận')
        if key == 'cluster':
            st.bar_chart({'Cụm':[str(r['topic_cluster']) for r in data['sizes']],
                'Số bài':[r['count'] for r in data['sizes']]},x='Cụm',y='Số bài',color='#0f766e')
            st.warning(f'Silhouette {data["silhouette"]:.4f}: chưa có cơ sở khẳng định cụm tốt; không phải accuracy sentiment.')
        if key == 'kafka':
            st.dataframe([{'Lượt':name,'Observations':data[name][1],'Canonical':data[name][2]}
                for name in ('first','replay','restart')],hide_index=True,use_container_width=True)
            st.caption('Restart ở đây là truy vấn cùng checkpoint, chưa phải tắt/bật broker hoặc crash khi ghi.')
        if key == 'save':
            st.write('Coverage:',data['serving']['coverage_status'])
            db = WORK/'serving'/('lab_'+RUN+'.sqlite')
            with sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True) as connection:
                rows=connection.execute('SELECT title,source,category,sentiment_label FROM articles ORDER BY article_id LIMIT 5').fetchall()
                st.dataframe([dict(zip(('Tiêu đề','Nguồn','Chủ đề','Dự đoán thử nghiệm'),r)) for r in rows],hide_index=True,use_container_width=True)
        with st.expander('Xem JSON evidence',expanded=key not in ('cluster','save','kafka')):
            st.json(data)
        st.caption(str(WORK/(key+'.json')))
    with right:
        st.markdown('#### Kiểm tra trực tiếp khi báo cáo')
        argv=command(key)
        st.code(subprocess.list2cmdline(argv),language='powershell')
        if st.button('Chạy kiểm tra chỉ đọc',type='primary',key='live_check'):
            now=datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).isoformat()
            try:
                with st.spinner('Đang kiểm tra, tối đa 25 giây…'):
                    result=subprocess.run(argv,cwd=ROOT,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=25)
                st.session_state['demo_live']=(key,now,result.returncode,(result.stdout+'\n'+result.stderr)[-16000:])
            except (OSError,subprocess.TimeoutExpired) as exc:
                st.session_state['demo_live']=(key,now,None,str(exc))
        live=st.session_state.get('demo_live')
        if live and live[0]==key:
            st.caption(f'Live {live[1]} · exit={live[2]}')
            if live[2] != 0:
                st.error('Kiểm tra live chưa thành công; evidence cũ không chứng minh service đang chạy.')
            st.code(live[3] or '(không có stdout)',language='text')
        else:
            st.info('Chưa chạy kiểm tra live cho bước này trong phiên trình diễn.')
    with st.expander('Chạy lại toàn pipeline và giới hạn'):
        st.write('Mở Airflow → vietnews_nine_steps → Trigger với conf trong docs/NINE_STEPS_RUNBOOK.md. Dùng run mới; không Clear task ghi đè output cũ. Trang này không tự train/publish.')
        st.write('Lab một poll, không toàn corpus; RFM không áp dụng tin tức. Chưa benchmark phân tán hoặc crash-during-write. Đánh giá sentiment cần human gold; không coi số có dự đoán là độ chính xác.')


def pipeline_panel(st):
    from dashboard.demo_runner import start,current,states,work_path
    from dashboard.pipeline_visual import render_pipeline
    from dashboard.stage_scene import render_scene
    st.caption('Xử lý lại RSS thật ngày 08/10 · Không crawl mới · Output riêng, không thay dashboard chính')
    viewing,motion=st.columns([3,2])
    scene_mode=viewing.radio('Cách xem công đoạn',['Theo lượt chạy thật','Xem minh họa từng công đoạn'],horizontal=True)
    animate=motion.checkbox('Bật chuyển động minh họa',value=True)
    preview=None
    if scene_mode=='Xem minh họa từng công đoạn':
        preview=st.selectbox('Chọn công đoạn để xem minh họa',[s[0] for s in STEPS],format_func=lambda k:next(s[1] for s in STEPS if s[0]==k))
        st.caption('Chỉ phát animation giải thích chức năng, không khởi chạy task và không thay trạng thái Airflow.')
    launch,refresh,dashboard_link=st.columns(3)
    clicked=launch.button('Chạy demo đồ án',type='primary',use_container_width=True)
    refresh.button('Cập nhật tiến trình',use_container_width=True)
    dashboard_link.link_button('Dashboard chính','http://localhost:8502',use_container_width=True)
    if clicked:
        try:
            with st.spinner('Gửi lượt chạy mới tới Airflow…'):
                start()
            st.success('Đã gửi pipeline. Có thể theo dõi bên dưới, không cần giữ terminal mở.')
        except (OSError,ValueError,RuntimeError,subprocess.TimeoutExpired) as exc:
            st.error(f'Chưa xác nhận khởi chạy: {exc}. Bấm cập nhật để kiểm tra trước khi thử lại.')
    st.caption('Chạy thật qua Airflow · Tự cập nhật 8 giây/lần · Animation chỉ minh họa nhiệm vụ của công đoạn')

    @st.experimental_fragment(run_every='8s')
    def progress_panel():
        info=current()
        if not info:
            st.info('Chưa có lượt demo từ nút này. Nhấn Chạy demo đồ án để bắt đầu.')
            st.markdown(render_scene([],{},animate,preview),unsafe_allow_html=True)
            return
        def local_time(value):
            if not value:
                return ''
            return datetime.fromisoformat(value).astimezone(ZoneInfo('Asia/Ho_Chi_Minh')).strftime('%d/%m %H:%M:%S')
        st.caption(f'Lượt chạy: {info["run_id"]} · bắt đầu {local_time(info["started_at"])} giờ Việt Nam')
        try:
            rows=states(info)
        except (OSError,ValueError,RuntimeError,subprocess.TimeoutExpired) as exc:
            st.error(f'Chưa đọc được trạng thái Airflow: {exc}')
            st.markdown(render_scene([],{},animate if preview else False,preview),unsafe_allow_html=True)
            return
        by_step={row['task_id']:row for row in rows}
        completed=sum(row.get('state')=='success' for row in rows)
        st.progress(completed/9,text=f'{completed}/9 bước hoàn thành — trạng thái Airflow thực tế')
        labels={None:'Chờ','success':'Hoàn thành','running':'Đang chạy','queued':'Trong hàng đợi',
            'scheduled':'Đã lên lượt','failed':'Lỗi','upstream_failed':'Bị chặn bởi bước trước'}
        work=work_path(info)
        outputs={}
        for key,_,_ in STEPS:
            if by_step.get(key,{}).get('state')=='success':
                try:
                    outputs[key]=evidence(key,work)
                except (OSError,ValueError) as exc:
                    st.error(f'Không đọc được output {key}: {exc}')
        st.markdown(render_scene(rows,outputs,animate,preview),unsafe_allow_html=True)
        with st.expander('Sơ đồ tổng thể 9 công đoạn'):
            st.markdown(render_pipeline(rows,outputs,False),unsafe_allow_html=True)
        with st.expander('Bảng thời gian từng công đoạn'):
            st.dataframe([{'Bước':f'{i+1}. {title}','Trạng thái':labels.get(by_step.get(key,{}).get('state'),by_step.get(key,{}).get('state')),
                'Bắt đầu (VN)':local_time(by_step.get(key,{}).get('start_date','')),'Kết thúc (VN)':local_time(by_step.get(key,{}).get('end_date',''))}
                for i,(key,title,_) in enumerate(STEPS)],hide_index=True,use_container_width=True)
        if 'clean' in outputs:
            clean=outputs['clean'];cols=st.columns(3)
            for col,label,name in zip(cols,('Observations RSS','Bài đã làm sạch','Observations trùng'),('raw_count','clean_count','duplicate_removed')):
                col.metric(label,clean[name])
        if 'cluster' in outputs:
            with st.expander('Kết quả phân cụm thử nghiệm'):
                cluster=outputs['cluster']
                st.bar_chart({'Cụm':[str(r['topic_cluster']) for r in cluster['sizes']],
                    'Số bài':[r['count'] for r in cluster['sizes']]},x='Cụm',y='Số bài',color='#0f766e')
                st.caption(f'Silhouette={cluster["silhouette"]}; không phải độ chính xác sentiment hoặc bằng chứng cùng sự kiện.')
        if 'save' in outputs:
            st.subheader('Bài báo sau xử lý của lượt demo')
            db=work/'serving'/('lab_'+info['scope']+'.sqlite')
            with sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True) as connection:
                sample_rows=connection.execute('SELECT title,source,category,sentiment_label FROM articles ORDER BY article_id LIMIT 10').fetchall()
                st.dataframe([dict(zip(('Tiêu đề','Nguồn','Chủ đề','Sentiment thử nghiệm'),row)) for row in sample_rows],hide_index=True,use_container_width=True)
            st.caption('SQLite riêng: '+str(db))
        if any(row.get('state') in ('failed','upstream_failed') for row in rows):
            st.error('Pipeline có bước lỗi. Giữ output để kiểm tra; không xóa raw/checkpoint hoặc lấy kết quả run cũ thay thế.')
        elif completed==9 and 'verify' in outputs:
            st.success('Demo đồ án hoàn thành: đã đối soát output của chính lượt chạy này.')
        st.link_button('Xem task và log trong Airflow','http://localhost:8088')
        with st.expander('Chi tiết kỹ thuật và giới hạn'):
            st.write(str(work))
            st.write('Spark local[2], HDFS thật, model sentiment thử nghiệm. Coverage giữ trạng thái thật; không thay serving chính, không benchmark hoặc đánh giá human gold.')
            st.json(outputs)
    progress_panel()


if __name__ == '__main__':
    main()
