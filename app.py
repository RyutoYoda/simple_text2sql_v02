import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import numpy as np
import re
from openai import OpenAI

st.set_page_config(page_title="🦆 Chat2SQL Auto Graph", layout="wide")
st.title("🦆 Chat2SQL × GPT-3.5 × DuckDB 📊")

openai_api_key = st.sidebar.text_input("🔑 OpenAI API Key", type="password")

uploaded_file = st.file_uploader("📄 CSVまたはParquetファイルをアップロード", type=["csv", "parquet"])

# 🔍 グラフ用の列選定関数
def choose_chart_columns(df, chart_type):
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    datetime_cols = df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()
    object_cols = df.select_dtypes(include="object").columns.tolist()

    if chart_type == "line" and datetime_cols and numeric_cols:
        return datetime_cols[0], numeric_cols[0]
    if chart_type == "scatter" and len(numeric_cols) >= 2:
        return numeric_cols[0], numeric_cols[1]
    if chart_type == "pie" and object_cols and numeric_cols:
        return object_cols[0], numeric_cols[0]
    if len(object_cols) >= 1 and len(numeric_cols) >= 1:
        return object_cols[0], numeric_cols[0]
    elif len(numeric_cols) >= 2:
        return numeric_cols[0], numeric_cols[1]
    return None, None

if uploaded_file:
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_parquet(uploaded_file)

    # ⏰ 日付列を自動変換
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass

    st.success("✅ データを読み込みました！")
    st.dataframe(df.head())

    duck_conn = duckdb.connect()
    duck_conn.register("data", df)

    with st.expander("💡 サンプル質問"):
        st.markdown("""
        - 月ごとの売上合計を表示してください
        - 商品カテゴリごとの平均価格は？
        - 地域別の販売数を棒グラフで見せて
        - 最も売れた商品は？
        - 地域ごとの売上割合は？
        - 単価と数量の相関は？
        - 月別の販売数推移を教えて
        """)

    user_input = st.chat_input("自然言語で質問してください")

    if user_input and openai_api_key:
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("GPTがSQLを生成中..."):

                client = OpenAI(api_key=openai_api_key)
                schema_desc = "\n".join([f"{col} ({dtype})" for col, dtype in zip(df.columns, df.dtypes)])

                prompt = f"""
あなたはデータ分析アシスタントです。
次のスキーマのテーブル `data` に対して、質問に対応する DuckDB形式のSQLを生成してください。

スキーマ:
{schema_desc}

注意:
- `strftime()` や `format_date()` を使うときは、列を必ず `CAST(列 AS DATE)` にしてください。
- 出力はSQL文だけ、装飾なしで返してください。

質問:
{user_input}
"""

                try:
                    response = client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "あなたはSQLを生成するデータアナリストです。"},
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.3
                    )

                    raw_sql = response.choices[0].message.content.strip()
                    sql = re.sub(r"```sql|```", "", raw_sql).strip()
                    st.markdown(f"🧠 **生成されたSQL:**\n```sql\n{sql}\n```")

                    result_df = duck_conn.execute(sql).fetchdf()
                    st.dataframe(result_df)

                    # 🔍 グラフ種判定
                    q = user_input.lower()
                    if any(w in q for w in ["割合", "比率", "シェア"]):
                        chart_type = "pie"
                    elif any(w in q for w in ["相関", "関係", "関連"]):
                        chart_type = "scatter"
                    elif any(w in q for w in ["時間", "日時", "推移", "傾向"]):
                        chart_type = "line"
                    else:
                        chart_type = "bar"

                    x, y = choose_chart_columns(result_df, chart_type)

                    if x and y:
                        if chart_type == "bar":
                            fig = px.bar(result_df, x=x, y=y)
                        elif chart_type == "line":
                            fig = px.line(result_df, x=x, y=y)
                        elif chart_type == "scatter":
                            fig = px.scatter(result_df, x=x, y=y, trendline="ols")
                        elif chart_type == "pie":
                            fig = px.pie(result_df, names=x, values=y)
                        else:
                            fig = px.bar(result_df, x=x, y=y)

                        st.plotly_chart(fig, use_container_width=True)

                        # 相関係数（scatter時）
                        if chart_type == "scatter":
                            try:
                                corr = np.corrcoef(result_df[x], result_df[y])[0, 1]
                                st.markdown(f"📈 **相関係数**: `{corr:.3f}`")
                            except:
                                st.info("相関係数の計算に失敗しました。")
                    else:
                        st.info("自動的にグラフに使える列が見つかりませんでした。")

                except Exception as e:
                    st.error(f"❌ エラー: {e}")
else:
    st.info("まずはCSVまたはParquetファイルをアップロードしてください。")
