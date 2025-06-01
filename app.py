import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import numpy as np
import re
from openai import OpenAI

st.set_page_config(page_title="🧠 Chat2SQL Mini", layout="wide")
st.title("🦆 Chat2SQL (最小・安定版)")

openai_api_key = st.sidebar.text_input("🔑 OpenAI API Key", type="password")

uploaded_file = st.file_uploader("📄 CSVまたはParquetファイルをアップロード", type=["csv", "parquet"])

if uploaded_file:
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_parquet(uploaded_file)

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

    user_input = st.chat_input("自然言語で質問してください")

    if user_input and openai_api_key:
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("GPTがSQLを生成中..."):
                client = OpenAI(api_key=openai_api_key)

                schema_desc = "\n".join([f"{col} ({dtype})" for col, dtype in zip(df.columns, df.dtypes)])

                prompt = f"""
あなたはDuckDB向けのSQLを生成するデータアナリストです。
テーブル名は常に `data` です。
DuckDBでは文字列型の列を日時関数に使う場合、必ず `CAST(列名 AS DATE)` にしてください。
SQL文だけ返してください。

スキーマ:
{schema_desc}

質問:
{user_input}
"""

                try:
                    response = client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[
                            {"role": "system", "content": "あなたはSQLを生成するデータアナリストです。"},
                            {"role": "user", "content": prompt}
                        ]
                    )

                    raw_sql = response.choices[0].message.content.strip()
                    sql = re.sub(r"```sql|```", "", raw_sql).strip()

                    st.markdown(f"🧠 **生成されたSQL:**\n```sql\n{sql}\n```")

                    result_df = duck_conn.execute(sql).fetchdf()
                    st.dataframe(result_df)

                    if result_df.shape[1] == 2:
                        x, y = result_df.columns[0], result_df.columns[1]

                        # グラフ種自動判定
                        q = user_input.lower()
                        if any(w in q for w in ["割合", "比率", "シェア","円"]):
                            chart_type = "pie"
                        elif any(w in q for w in ["相関", "関係", "関連"]):
                            chart_type = "scatter"
                        elif any(w in q for w in ["時間", "日時", "推移", "傾向", "月", "日"]):
                            chart_type = "line"
                        else:
                            chart_type = "bar"

                        try:
                            result_df[x] = pd.to_datetime(result_df[x])
                        except:
                            pass

                        if chart_type == "pie":
                            fig = px.pie(result_df, names=x, values=y)
                        elif chart_type == "scatter":
                            fig = px.scatter(result_df, x=x, y=y, trendline="ols")
                            try:
                                corr = np.corrcoef(result_df[x], result_df[y])[0, 1]
                                st.markdown(f"📈 **相関係数**: `{corr:.3f}`")
                            except:
                                pass
                        elif chart_type == "line":
                            fig = px.line(result_df, x=x, y=y)
                        else:
                            fig = px.bar(result_df, x=x, y=y)

                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("📉 自動描画には2列の結果が必要です。")

                except Exception as e:
                    st.error(f"❌ エラー: {e}")
else:
    st.info("まずはCSVまたはParquetファイルをアップロードしてください。")
