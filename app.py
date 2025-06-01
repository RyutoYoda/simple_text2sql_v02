import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import numpy as np
import re
from openai import OpenAI

st.set_page_config(page_title="🧠 Chat2SQL", layout="wide")
st.title("🧠 Chat2SQL")

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

    st.success("✅ データを読み込みました")
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

                schema_desc = "\n".join(
                    [f"{col} ({dtype})" for col, dtype in zip(df.columns, df.dtypes)]
                )

                prompt = f"""
あなたはDuckDBに対してSQLを生成するアシスタントです。
テーブル名は `data` です。

DuckDBでは文字列を日付関数に使う場合、必ず `CAST(列 AS DATE)` を使用してください。

重要：
「関係」「相関」「関連」などの質問では、`SELECT CORR(...)` のような1列の相関係数ではなく、
`SELECT col1, col2 FROM data` のように、2列の数値データを含む結果を返してください（散布図描画のため）。

出力はSQL文のみ。コードブロックや装飾は不要です。

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

                    if result_df.shape[1] >= 2:
                        x, y = result_df.columns[0], result_df.columns[1]

                        # グラフ種推定
                        q = user_input.lower()
                        if any(w in q for w in ["割合", "比率", "シェア","円"]):
                            chart_type = "pie"
                        elif any(w in q for w in ["相関", "関係", "関連"]):
                            chart_type = "scatter"
                        elif any(w in q for w in ["時間", "日時", "推移", "傾向", "月", "日", "時系列"]):
                            chart_type = "line"
                        else:
                            chart_type = "bar"

                        # x軸をdatetimeに変換試行
                        try:
                            result_df[x] = pd.to_datetime(result_df[x])
                        except:
                            pass

                        # 🔧 散布図のために数値変換を試行
                        if chart_type == "scatter":
                            try:
                                result_df[x] = pd.to_numeric(result_df[x])
                                result_df[y] = pd.to_numeric(result_df[y])
                            except:
                                pass

                        # グラフ描画
                        if chart_type == "pie":
                            fig = px.pie(result_df, names=x, values=y)
                            st.plotly_chart(fig, use_container_width=True)

                        elif chart_type == "scatter":
                            if pd.api.types.is_numeric_dtype(result_df[x]) and pd.api.types.is_numeric_dtype(result_df[y]):
                                fig = px.scatter(result_df, x=x, y=y)
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.warning("⚠️ 散布図は数値列同士にのみ対応しています。列が文字列型のままかも？")

                        elif chart_type == "line":
                            fig = px.line(result_df, x=x, y=y)
                            st.plotly_chart(fig, use_container_width=True)

                        else:
                            fig = px.bar(result_df, x=x, y=y)
                            st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("📉 グラフ描画には2列以上の結果が必要です。")

                except Exception as e:
                    st.error(f"❌ エラー: {e}")
else:
    st.info("まずはCSVまたはParquetファイルをアップロードしてください。")
