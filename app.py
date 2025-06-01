import streamlit as st
import pandas as pd
import sqlite3
import requests
import plotly.express as px
import os

# -------------------------------
# アプリ設定
# -------------------------------
st.set_page_config(page_title="自然言語データチャット", layout="wide")
st.title("📊 自然言語でグラフ生成チャット")
st.markdown("CSV または Parquet ファイルをアップロードして、自然言語で質問してみましょう。")

# -------------------------------
# Hugging Face APIキー入力
# -------------------------------
hf_token = st.sidebar.text_input("🔑 Hugging Face Token", type="password", help="https://huggingface.co/settings/tokens から取得")

# -------------------------------
# データアップロード
# -------------------------------
uploaded_file = st.file_uploader("データファイルをアップロード (CSV or Parquet)", type=["csv", "parquet"])
if uploaded_file:
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_parquet(uploaded_file)

    st.success("✅ データを読み込みました。")
    st.dataframe(df.head())

    # DBに投入
    conn = sqlite3.connect(":memory:")
    df.to_sql("data", conn, index=False, if_exists="replace")

    # チャット履歴用セッション
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # サンプル質問ボタン
    with st.expander("💡 サンプル質問", expanded=False):
        st.markdown("""
        - 月ごとの売上合計をグラフで見せて
        - 商品カテゴリごとの販売数を棒グラフで
        - 売上の平均値を教えて
        - 一番売れた商品は？
        """)

    # 入力欄
    user_input = st.chat_input("質問を入力してください（例：月ごとの売上を見せて）")

    if user_input and hf_token:
        # 履歴に追加
        st.session_state.chat_history.append({"role": "user", "content": user_input})

        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("AIが考え中..."):

                # モデルに問い合わせ
                table_info = df.head(5).to_string()
                prompt = f"""### SQLite SQL tables, with their properties:
#
# {table_info}
#
### A query to answer: {user_input}
SELECT"""

                headers = {
                    "Authorization": f"Bearer {hf_token}"
                }

                payload = {
                    "inputs": prompt,
                    "parameters": {"max_new_tokens": 128}
                }

                response = requests.post(
                    "https://api-inference.huggingface.co/models/Snowflake/Arctic-Text2SQL-R1-7B",
                    headers=headers,
                    json=payload
                )

                if response.status_code == 200:
                    output = response.json()
                    generated_sql = "SELECT" + output[0]["generated_text"]

                    st.markdown(f"🧠 **生成されたSQL:**\n```sql\n{generated_sql}\n```")

                    try:
                        result_df = pd.read_sql_query(generated_sql, conn)
                        st.dataframe(result_df)

                        # グラフ生成（2列だけなら）
                        if result_df.shape[1] == 2:
                            fig = px.bar(result_df, x=result_df.columns[0], y=result_df.columns[1])
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.info("グラフ化には2列の結果が必要です。")

                    except Exception as e:
                        st.error(f"❌ SQL実行エラー: {e}")
                else:
                    st.error("モデル呼び出し失敗。トークンやAPIステータスを確認してください。")

else:
    st.info("まずはデータファイルをアップロードしてください。")

