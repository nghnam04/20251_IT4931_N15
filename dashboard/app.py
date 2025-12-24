import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Real-time Sentiment Dashboard", layout="wide")
st_autorefresh(interval=60000, key="data_refresh")

@st.cache_resource
def init_connection():
    return MongoClient("mongodb://localhost:27017")

client = init_connection()
db = client["bigdata_project"]
collection = db["windowed_results_5min"]

st.title("📊 Real-time Social Media Sentiment Dashboard")

# Sidebar - Bộ lọc
st.sidebar.header("⚙️ Cấu hình bộ lọc")

time_range_options = {
    "15 phút qua": 15,
    "30 phút qua": 30,
    "45 phút qua": 45,
    "1 tiếng qua": 60,
    "2 tiếng qua": 120,
    "4 tiếng qua": 240,
    "8 tiếng qua": 480,
    "12 tiếng qua": 720,
    "1 ngày qua": 1440,
    "2 ngày qua": 2880,
    "7 ngày qua": 10080
}

selected_range_label = st.sidebar.selectbox(
    "Khoảng thời gian (Rolling Window)", 
    list(time_range_options.keys()),
    index=0
)
minutes_back = time_range_options[selected_range_label]

# Lấy danh sách Topic 
all_topics = sorted(collection.distinct("topic"))
selected_topic = st.sidebar.selectbox("Chọn Topic", ["Tất cả"] + all_topics)

cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_back)

query = {"window_start": {"$gte": cutoff_time}}
if selected_topic != "Tất cả":
    query["topic"] = selected_topic

data = list(collection.find(query, {"_id": 0}))

if not data:
    st.warning(f"⚠️ Không có dữ liệu trong {selected_range_label}. Đang chờ dữ liệu từ Spark...")
    st.stop()

df = pd.DataFrame(data)
df["window_start"] = pd.to_datetime(df["window_start"])

# Tính toán KPI cho khoảng thời gian gần nhất
total_mentions = df["total_mentions"].sum()
total_pos = df["positive"].sum()
total_neu = df["neutral"].sum()
total_neg = df["negative"].sum()
# Sentiment Score trung bình 
avg_score = (total_pos - total_neg) / total_mentions if total_mentions > 0 else 0

# Hiển thị KPI
st.markdown(f"### ⏱️ Dữ liệu tổng hợp trong: **{selected_range_label}**")
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("Tổng Mentions", f"{int(total_mentions):,}")
kpi2.metric("Sentiment Score", round(avg_score, 3))
kpi3.metric("Tích cực (Pos)", f"{int(total_pos):,}")
kpi4.metric("Trung lập (Neu)", f"{int(total_neu):,}")
kpi5.metric("Tiêu cực (Neg)", f"{int(total_neg):,}")

st.markdown("---")

# Biểu đồ xu hướng 
if minutes_back <= 180: 
    resample_rule = "5T"
elif minutes_back <= 1440: 
    resample_rule = "15T"
else:
    resample_rule = "1H"

chart_df = (
    df.set_index("window_start")
    .resample(resample_rule)
    .agg({"sentiment_score": "mean", "total_mentions": "sum"})
    .reset_index()
    .fillna(0)
)

st.subheader(f"📈 Xu hướng cảm xúc")
fig1 = px.line(
    chart_df, x="window_start", y="sentiment_score",
    markers=True, template="plotly_white",
    range_y=[-1, 1],
    labels={"window_start": "Thời gian", "sentiment_score": "Sentiment Score"}
)

if resample_rule == "5T":
    fig1.update_xaxes(
        dtick=300000, 
        tickformat="%H:%M\n%d/%m" 
    )

fig1.add_hline(y=0, line_dash="dash", line_color="gray")
st.plotly_chart(fig1, use_container_width=True)

# Phân bổ cảm xúc và Top Topics
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📊 Tỷ lệ cảm xúc")
    sentiment_pie = pd.DataFrame({
        "Loại": ["Positive", "Neutral", "Negative"],
        "Số lượng": [total_pos, total_neu, total_neg]
    })
    fig2 = px.pie(
        sentiment_pie, values="Số lượng", names="Loại",
        color="Loại",
        color_discrete_map={"Positive":"#2ecc71", "Neutral":"#95a5a6", "Negative":"#e74c3c"},
        hole=0.3
    )
    st.plotly_chart(fig2, use_container_width=True)

with col_right:
    st.subheader("🔝 Top 5 Topics nổi bật")
    top_topics = df.groupby("topic")["total_mentions"].sum().sort_values(ascending=False).head(5).reset_index()
    fig3 = px.bar(
        top_topics, x="total_mentions", y="topic", orientation='h',
        color="total_mentions", color_continuous_scale="Viridis",
        labels={"total_mentions": "Lượt thảo luận", "topic": "Chủ đề"}
    )
    st.plotly_chart(fig3, use_container_width=True)

# Bảng chi tiết
st.subheader("📋 Danh sách các bản ghi gần nhất")
st.dataframe(
    df.sort_values("window_start", ascending=False).head(20),
    use_container_width=True
)