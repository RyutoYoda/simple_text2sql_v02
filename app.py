import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from openai import OpenAI

# -------------------------------
# ページ設定
# -------------------------------
st.set_page_config(page_title="🧠 Chat2SQL with GPT-3.5", layout="wide")
st.title("🧠 Chat2SQL with GPT-3.5 Turbo")

# -------------------------------
# OpenAI APIキー
# -------------------------------
openai_api_key = st.sidebar.text_input("🔑 OpenAI API Key", type="password")

# -------------------------------
# データアップロード
# -------------------------------
uploaded_file = st.file_uploader("📄 CSVまたはParquetファイルをアップロード", type=["csv", "parquet"])

if uploaded_file:
    # データ読み込み
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_parquet(uploaded_file)

    st.success("✅ データを読み込みました！")
    st.dataframe(df.head())

    # DBに保存
    conn = sqlite3.connect(":memory:")
    df.to_sql("data", conn, index=False, if_exists="replace")

    # サンプル質問
    with st.expander("💡 サンプル質問", expanded=False):
        st.markdown("""
        - 月ごとの売上合計を教えて
        - カテゴリごとの平均価格を表示して
        - 売上が最も多い商品は？
        - 注文数が10以上の行だけ集計して
        - 地域別の販売数を棒グラフで見たい
        """)

    # 入力チャット欄
    user_input = st.chat_input("自然言語で質問してください")

    if user_input and openai_api_key:
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("GPTがSQLを生成中..."):

                client = OpenAI(api_key=openai_api_key)

                # スキーマ記述（上位5行の型付きで）
                schema_desc = "\n".join([f"{col} ({dtype})" for col, dtype in zip(df.columns, df.dtypes)])

                # プロンプト生成
                prompt = f"""
あなたはSQLiteデータベースに対して自然言語からSQLを生成するアシスタントです。

次のスキーマに基づいて、ユーザーの質問に対応する **SQLite形式のクエリ** を生成してください。

### テーブルスキーマ:
{schema_desc}

### 質問:
{user_input}

**SQLクエリのみ**を、コードブロックや装飾なしで返してください。
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

                    # Markdownコードブロック削除
                    sql = raw_sql.strip("`").replace("```sql", "").replace("```", "").strip()

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
