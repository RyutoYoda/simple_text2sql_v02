import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
from openai import OpenAI
import re
# -------------------------
# ページ設定
# -------------------------
st.set_page_config(page_title="🦆 Chat2SQL with DuckDB", layout="wide")
st.title("🦆 Chat2SQL with DuckDB × GPT-3.5")

# -------------------------
# OpenAI APIキー
# -------------------------
openai_api_key = st.sidebar.text_input("🔑 OpenAI API Key", type="password")

# -------------------------
# ファイルアップロード
# -------------------------
uploaded_file = st.file_uploader("📄 CSVまたはParquetファイルをアップロード", type=["csv", "parquet"])

if uploaded_file:
    # データ読み込み
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_parquet(uploaded_file)

    # ✅ 日付らしき列を datetime に変換（前処理）
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass

    st.success("✅ データを読み込みました！")
    st.dataframe(df.head())

    # DuckDBにDataFrame登録
    duck_conn = duckdb.connect()
    duck_conn.register("data", df)

    # サンプル質問
    with st.expander("💡 サンプル質問", expanded=False):
        st.markdown("""
        - 月ごとの売上合計を表示してください
        - 商品カテゴリごとの平均価格は？
        - 地域別の販売数を棒グラフで見せて
        - 最も売れた商品は？
        - 購入金額が高い順に並べてください
        """)

    user_input = st.chat_input("自然言語で質問してください")

    if user_input and openai_api_key:
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("GPTがSQLを生成中..."):

                client = OpenAI(api_key=openai_api_key)

                # スキーマ情報
                schema_desc = "\n".join([f"{col} ({dtype})" for col, dtype in zip(df.columns, df.dtypes)])

                # 💡 GPTへのプロンプトにCASTの指示を追加
                prompt = f"""
あなたはデータ分析アシスタントです。

以下のスキーマに基づいて、自然言語の質問に対応する **DuckDB対応のSQLクエリ** を生成してください。

テーブル名は常に `data` です。

### スキーマ:
{schema_desc}

### 注意点:
- `strftime()`や`format_date()`関数を使う場合、文字列型の列は **必ず `CAST(列名 AS DATE)`** に変換してください。
- DuckDBでは文字列型のままでは日付関数を使えません。
- SQL文のみを返してください（コードブロックや説明は不要です）。

### 質問:
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

                    try:
                        result_df = duck_conn.execute(sql).fetchdf()
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
