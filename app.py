import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import re
import numpy as np
from openai import OpenAI

# -------------------------
# ページ設定
# -------------------------
st.set_page_config(page_title="🦆 Chat2SQL with Graph Auto", layout="wide")
st.title("🦆 Chat2SQL × GPT-3.5 × DuckDB with Smart Charting")

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

以下のスキーマを持つテーブル `data` に対して、自然言語の質問に対応する DuckDB形式のSQLクエリを生成してください。

### スキーマ:
{schema_desc}

### 注意点:
- 日付や時間列を使う場合は必ず `CAST(列名 AS DATE)` にしてください。
- `strftime()`や`format_date()`を使うときもCASTが必要です。
- SQL文のみをプレーンテキストで返してください。コードブロックや説明は不要です。

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

                        # 🔍 グラフタイプ自動選定
                        query_lower = user_input.lower()
                        if any(w in query_lower for w in ["割合", "比率", "シェア"]):
                            chart_type = "pie"
                        elif any(w in query_lower for w in ["相関", "関係", "関連"]):
                            chart_type = "scatter"
                        elif any(w in query_lower for w in ["時間", "日時", "推移", "傾向"]):
                            chart_type = "line"
                        else:
                            chart_type = "bar"

                        # ✅ 2列のときのみグラフ表示
                        if result_df.shape[1] == 2:
                            x, y = result_df.columns[0], result_df.columns[1]

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

                            # 📈 相関係数の表示（scatterのとき）
                            if chart_type == "scatter":
                                try:
                                    corr = np.corrcoef(result_df[x], result_df[y])[0, 1]
                                    st.markdown(f"📊 **相関係数**: `{corr:.3f}`")
                                except:
                                    st.info("相関係数の計算に失敗しました。")
                        else:
                            st.info("2列の結果のみグラフ化されます。")

                    except Exception as e:
                        st.error(f"❌ SQL実行時エラー: {e}")

                except Exception as e:
                    st.error(f"OpenAI API呼び出しに失敗しました: {e}")
else:
    st.info("まずはCSVまたはParquetファイルをアップロードしてください。")
