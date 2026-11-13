"""Local read-only dashboard over a verified immutable publication."""
import json
import os
import sys
from pathlib import Path

import duckdb
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from olist.local_warehouse import resolve_publication

st.set_page_config(page_title='Olist · Trusted Commerce', page_icon='📦', layout='wide')
st.title('Olist · Trusted Commerce Data Platform')
st.caption('Thanh toán · Giao hàng · Khách hàng · Truy vết dữ liệu')

@st.cache_data(show_spinner=False)
def load_generation(root, pointer_bytes):
    # The pointer is part of the cache key. A refresh selects one complete generation.
    path, receipt = resolve_publication(root, pointer_bytes=pointer_bytes)
    with duckdb.connect(str(path), read_only=True) as conn:
        orders=conn.execute('SELECT * FROM fact_orders').df()
        reasons=conn.execute('SELECT * FROM quarantine_reasons ORDER BY rows DESC').df()
        sellers=conn.execute('SELECT b.order_id,b.seller_id,d.seller_state FROM bridge_order_seller b JOIN dim_seller d USING(seller_id)').df()
        tables={r[0] for r in conn.execute('SHOW TABLES').fetchall()}
        extras={name:conn.execute('SELECT * FROM '+name).df() for name in
                ['fact_order_geography','mart_customer_cohorts','mart_corridors','change_events'] if name in tables}
    return orders, reasons, sellers, receipt, extras

configured_root=Path(os.getenv('OLIST_LOCAL_ROOT','data/local'))
configured_label='Olist lịch sử' if configured_root==Path('data/local') else 'Nguồn cấu hình · OLIST_LOCAL_ROOT'
candidates=[(configured_label,configured_root),
            ('Fixture tổng hợp · dữ liệu kiểm thử',Path('data/local-fixture')),
            ('Mô phỏng correction · dữ liệu tách riêng',Path('data/simulation')),
            ('CDC PostgreSQL · nguồn mô phỏng',Path('data/cdc'))]
roots={}
for label,candidate in candidates:
    if (candidate/'current.json').exists() and candidate.resolve() not in {p.resolve() for p in roots.values()}:
        roots[label]=candidate
if not roots:
    st.info('Chưa có bản dữ liệu được công bố. Chạy scripts/final.ps1 demo-fixture để thử với dữ liệu tổng hợp, hoặc demo-real để dùng Olist lịch sử.')
    st.stop()
source_label=st.sidebar.selectbox('Nguồn dữ liệu',list(roots))
root=roots[source_label]
try:
    all_orders,reasons,bridge,receipt,extras=load_generation(str(root), (root/'current.json').read_bytes())
except Exception:
    st.error('Không thể xác minh bản dữ liệu. Kiểm tra current.json và checksum; không hiển thị dữ liệu dự phòng.')
    st.stop()
if receipt['dataset_kind']!='real_olist':
    st.info('Nguồn tổng hợp để kiểm thử pipeline; không phải dữ liệu giao dịch Olist thực tế.')
st.caption(f"Nguồn: {receipt['dataset_kind']} · Khoảng dữ liệu {str(all_orders.purchase_date.min())[:10]} → {str(all_orders.purchase_date.max())[:10]} · Công bố {receipt['published_at'][:19]} UTC")
with st.sidebar:
    st.header('Phạm vi phân tích')
    states=st.multiselect('Bang khách hàng',sorted(all_orders.customer_state.dropna().unique()))
    statuses=st.multiselect('Trạng thái đơn',sorted(all_orders.order_status.dropna().unique()))
    dates=st.date_input('Ngày mua hàng',value=(all_orders.purchase_date.min().date(),all_orders.purchase_date.max().date())) if not all_orders.empty else ()
    st.caption('Bộ lọc áp dụng cho tổng quan, nhà bán, thanh toán và bản đồ. Cohort, corridors và kiểm tra pipeline dùng toàn bộ publication để giữ đúng mẫu số.')
    if st.button('Đọc bản công bố mới'):
        load_generation.clear(); st.rerun()
orders=all_orders.copy()
if states: orders=orders[orders.customer_state.isin(states)]
if statuses: orders=orders[orders.order_status.isin(statuses)]
if len(dates)==2:
    orders=orders[(orders.purchase_date.dt.date>=dates[0]) & (orders.purchase_date.dt.date<=dates[1])]
overview,seller_tab,commerce,geography,customers,trace,trust=st.tabs([
    'Tổng quan giao hàng','Nhà bán','Thanh toán','Địa lý','Khách hàng','Order trace','Chất lượng & nguồn'])
with overview:
    if orders.empty:
        st.info('Không có đơn hàng trong phạm vi đã chọn.')
    else:
        eligible=int(orders.late_eligible.sum()); late=int(orders.is_late.fillna(0).sum())
        a,b,c,d=st.columns(4)
        a.metric('Đơn hàng',f'{len(orders):,}')
        merchandise=float(orders.merchandise_brl.sum())
        b.metric('Giá trị hàng · BRL',f'{merchandise/1_000_000:.2f}M' if merchandise>=1_000_000 else f'{merchandise:,.2f}',help=f'{merchandise:,.2f} BRL; không gồm phí vận chuyển')
        c.metric('Giao trễ',f'{late/eligible:.1%}' if eligible else 'Không đủ dữ liệu')
        d.metric('Đơn đủ điều kiện đo trễ',f'{eligible:,}')
        st.caption('Giao trễ: đơn delivered có ngày giao thực tế và ngày hẹn, giao sau ngày hẹn theo lịch. Giá trị hàng không gồm phí vận chuyển và không đồng nghĩa doanh thu nền tảng.')
        daily=orders.groupby('purchase_date').agg(orders=('order_id','size'),late=('is_late','sum'),eligible=('late_eligible','sum'))
        left,right=st.columns(2)
        with left:
            st.subheader('Số đơn theo ngày mua')
            st.line_chart(daily[['orders']])
        with right:
            st.subheader('Tỷ lệ giao trễ theo bang')
            regional=orders.groupby('customer_state').agg(eligible=('late_eligible','sum'),late=('is_late','sum'))
            regional=regional[regional.eligible>0]
            regional['Tỷ lệ trễ (%)']=100*regional.late/regional.eligible
            st.bar_chart(regional[['Tỷ lệ trễ (%)']])
            st.dataframe(regional[['eligible','late']],use_container_width=True)
        st.subheader('Đơn giao trễ để kiểm tra')
        cols=['order_id','customer_state','purchase_date','order_estimated_delivery_date','order_delivered_customer_date','merchandise_brl']
        st.dataframe(orders[orders.is_late==1][cols].head(200),hide_index=True,use_container_width=True)
        st.download_button('Tải dữ liệu đúng bộ lọc (CSV)',orders.to_csv(index=False).encode('utf-8'),file_name='olist-filtered-orders.csv',mime='text/csv')
with seller_tab:
    st.caption('Một đơn có thể thuộc nhiều nhà bán. Không cộng tổng số cặp đơn–nhà bán thành số đơn toàn hệ thống; dữ liệu không chứng minh nhà bán gây ra giao trễ.')
    pairs=bridge.merge(orders[['order_id','late_eligible','is_late']],on='order_id',how='inner')
    if pairs.empty:
        st.info('Không có cặp đơn–nhà bán trong phạm vi đã chọn.')
    else:
        minimum=st.slider('Số đơn đủ điều kiện tối thiểu',1,100,10)
        groups=pairs.groupby(['seller_id','seller_state']).agg(order_pairs=('order_id','size'),eligible=('late_eligible','sum'),late=('is_late','sum')).reset_index()
        groups=groups[groups.eligible>=minimum].copy()
        groups['late_rate']=groups.late/groups.eligible
        st.dataframe(groups.sort_values(['late_rate','eligible'],ascending=False),hide_index=True,use_container_width=True)
with commerce:
    st.subheader('Kiểm soát thanh toán theo đơn')
    complete=orders[(orders.item_count>0)&(orders.payment_count>0)]
    a,b,c=st.columns(3)
    a.metric('Có item và payment',f'{len(complete):,} / {len(orders):,}')
    b.metric('Đơn thiếu payment',f'{int((orders.payment_count==0).sum()):,}')
    exceptions=complete[complete.payment_difference_brl.abs()>0.01]
    c.metric('Chênh lệch nguồn > 0.01 BRL',f'{len(exceptions):,}')
    st.caption('Thiếu payment là chưa có bản ghi, không chứng minh khách chưa thanh toán. Chênh lệch nguồn không tự động là lỗi ETL; canceled không chứng minh đã hoàn tiền.')
    st.dataframe(exceptions[['order_id','order_status','item_count','payment_count','merchandise_brl','freight_brl','payment_brl','payment_difference_brl']].head(300),hide_index=True,use_container_width=True)
with geography:
    st.subheader('Phân bố khách hàng và hành lang giao dịch')
    if 'fact_order_geography' in extras:
        geo=extras['fact_order_geography'].merge(orders[['order_id']],on='order_id')
        located=geo.dropna(subset=['latitude','longitude'])
        st.write(f'Độ phủ tọa độ: {len(located):,} / {len(geo):,} đơn trong bộ lọc.')
        points=located.groupby(['latitude','longitude']).size().reset_index(name='orders')
        if not points.empty: st.map(points.sort_values('orders',ascending=False).head(1500),latitude='latitude',longitude='longitude',size=30)
        st.caption('Bản đồ hiển thị tối đa 1.500 ZIP đông đơn nhất; tọa độ là median các observations. Không phải vị trí chính xác từng khách hàng.')
        minimum=st.slider('Số cặp đơn–nhà bán tối thiểu cho corridor',1,200,20)
        corridors=extras['mart_corridors']
        st.dataframe(corridors[corridors.order_seller_pairs>=minimum].sort_values('order_seller_pairs',ascending=False),hide_index=True,use_container_width=True)
        st.caption('Corridors dùng toàn bộ publication. Khoảng cách đường chim bay giữa ZIP là proxy, không phải quãng đường vận chuyển; đơn nhiều seller xuất hiện trong nhiều cặp.')
    else: st.info('Chạy local-publish để cập nhật mô hình địa lý.')
with customers:
    st.subheader('Đặt hàng lặp lại theo cohort first-observed')
    st.caption('Toàn bộ đơn accepted, gồm cả canceled/unavailable; đây là hành vi tạo đơn, không chứng minh mua thành công. Không áp dụng bộ lọc đơn. Khóa customer_unique_id; first-observed không chứng minh khách mới; giá trị hàng quan sát không phải LTV.')
    if 'mart_customer_cohorts' in extras:
        horizon=st.selectbox('Thời gian quan sát đặt hàng lại (ngày)',[30,60,90])
        cohorts=extras['mart_customer_cohorts']; cohort=cohorts[cohorts.horizon_days==horizon].copy()
        cohort['repeat_rate']=cohort.repeat_customers/cohort.mature_customers.where(cohort.mature_customers>0)
        st.line_chart(cohort.set_index('cohort_month')[['repeat_rate']])
        st.dataframe(cohort,hide_index=True,use_container_width=True)
        st.caption('Chỉ khách có đủ số ngày quan sát tới cutoff mới vào mẫu số mature_customers. Cohort chưa đủ điều kiện giữ tỷ lệ null, không gán 0.')
    reviewed=orders.dropna(subset=['review_score'])
    st.write(f'Độ phủ review trong bộ lọc: {len(reviewed):,} / {len(orders):,} đơn.')
    st.dataframe(reviewed.groupby('is_late',dropna=False).agg(reviewed_orders=('order_id','size'),average_score=('review_score','mean')),use_container_width=True)
    st.caption('Review latest dùng thứ tự answer timestamp, creation timestamp và review ID. Liên hệ giao trễ–điểm số chỉ là mô tả; không suy ra nhân quả.')
with trace:
    st.subheader('Từ order đến nguồn và lịch sử correction')
    order_id=st.text_input('Order ID',value=str(orders.order_id.iloc[0]) if not orders.empty else '')
    selected=all_orders[all_orders.order_id==order_id]
    st.caption('Tra cứu theo toàn bộ publication; dùng Order ID từ bảng hoặc tệp CSV xuất ra.')
    if selected.empty: st.info('Không tìm thấy order trong publication này; có thể đã bị xóa ở nguồn mô phỏng.')
    else: st.dataframe(selected,hide_index=True,use_container_width=True)
    if order_id:
        db,_=resolve_publication(root,verify=False,pointer_bytes=json.dumps(receipt))
        with duckdb.connect(str(db),read_only=True) as conn:
            for label,table in [('Items','silver_order_items'),('Payments','silver_order_payments'),('Review history','silver_order_reviews')]:
                with st.expander(label): st.dataframe(conn.execute(f'SELECT * FROM {table} WHERE order_id=?',[order_id]).df(),hide_index=True,use_container_width=True)
    if 'change_events' in extras:
        events=extras['change_events']
        mask=events.key_json.map(lambda x:json.loads(x).get('order_id')==order_id)
        st.dataframe(events[mask],hide_index=True,use_container_width=True)
    else: st.caption('Historical snapshot không chứa lịch sử cập nhật nguồn. Chọn nguồn mô phỏng để xem before/after và source position.')
    st.code(json.dumps({k:receipt.get(k) for k in ['generation','dataset_version','baseline_generation','last_change_batch','mode']},indent=2))
with trust:
    st.subheader('Đối soát bản công bố')
    st.success('Các kiểm tra grain và đối soát theo từng đơn đều đạt tại thời điểm công bố.')
    st.json(receipt['checks'])
    st.dataframe(receipt['quality']['tables'],hide_index=True,use_container_width=True)
    st.subheader('Bản ghi cách ly theo nguyên nhân')
    st.dataframe(reasons,hide_index=True,use_container_width=True)
    st.caption('Số liệu kinh doanh chỉ dùng bản ghi accepted. Bản ghi rejected và duplicate được giữ để điều tra. Payment khác item + freight là chênh lệch nguồn cần phân tích, không tự động là lỗi pipeline.')
    with st.expander('Nguồn và phương pháp'):
        st.write('Olist là snapshot lịch sử. Correction và CDC dùng nguồn mô phỏng tách riêng. Mỗi màn hình đọc một generation bất biến; refresh mới chọn publication tiếp theo. Chất lượng ingestion trong nguồn mô phỏng là accounting của baseline, changes được kiểm tra riêng theo transaction.')
        st.json({k:v for k,v in receipt.items() if k not in ['quality','checks']})
        if receipt['dataset_kind']=='real_olist':
            st.link_button('Nguồn Olist trên Kaggle',receipt['source_reference'])
