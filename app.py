import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import openai
import os

st.set_page_config(page_title="🧠 Chat2SQL with OpenAI", layout="wide")
st.title("🧠 text2SQL_ver1")

# --- OpenAI APIキー入力 ---
openai_api_key = st.sidebar.text_input("🔑 OpenAI API Key", type="password")

# --- データ読み込み ---
uploaded_file = st.file_uploader("📄 CSVまたはParquetファイルをアップロード", type=["csv", "parquet"])

if uploaded_file:
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_parquet(uploaded_file)

    st.success("✅ データを読み込みました！")
    st.dataframe(df.head())

    conn = sqlite3.connect(":memory:")
    df.to_sql("data", conn, index=False, if_exists="replace")

    # --- ユーザーの自然言語入力 ---
    user_input = st.chat_input("データに関する質問を入力してください")

    if user_input and openai_api_key:
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("SQLを生成中..."):

                openai.api_key = openai_api_key

                schema_desc = "\n".join([f"{col} ({dtype})" for col, dtype in zip(df.columns, df.dtypes)])

                prompt = f"""
あなたはSQLデータベースのアシスタントです。
次のテーブルに基づいて、SQLite形式で適切なSQLクエリを生成してください。

スキーマ:
{schema_desc}

質問:
{user_input}

SQLクエリだけを返してください。
"""

                try:
                    response = openai.ChatCompletion.create(
                        model="gpt-4o",  # gpt-3.5-turbo でも可
                        messages=[
                            {"role": "system", "content": "あなたはSQL専門家です。"},
                            {"role": "user", "content": prompt}
                        ],
                        temperature=0.3
                    )

                    sql = response.choices[0].message.content.strip()
                    st.markdown(f"🧠 **生成されたSQL:**\n```sql\n{sql}\n```")

                    try:
                        result_df = pd.read_sql_query(sql, conn)
                        st.dataframe(result_df)

                        if result_df.shape[1] == 2:
                            fig = px.bar(result_df, x=result_df.columns[0], y=result_df.columns[1])
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("2列の結果のみグラフ化されます。")

                    except Exception as e:
                        st.error(f"❌ SQL実行時エラー: {e}")

                except Exception as e:
                    st.error(f"OpenAI API呼び出しに失敗しました: {e}")
else:
    st.info("まずはCSVまたはParquetファイルをアップロードしてください。")
