import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import numpy as np
import re
import os
from openai import OpenAI

st.set_page_config(page_title="🧠 Chat2SQL", layout="wide")
st.title("🧠 Chat2SQL")

openai_api_key = st.secrets.get("OPENAI_API_KEY")

if not openai_api_key:
    st.warning("⚠️ OPENAI_API_KEY が環境変数に設定されていません。")

uploaded_file = st.file_uploader("📄 CSVまたはParquetファイルをアップロード", type=["csv", "parquet"])

if uploaded_file:
    # ファイル読み込み
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_parquet(uploaded_file)

    # 日付っぽい列をdatetimeに変換
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass

    st.success("✅ データを読み込みました")
    st.dataframe(df.head())

    # DuckDB に登録
    duck_conn = duckdb.connect()
    duck_conn.register("data", df)

    # 💡 サンプル質問表示
    with st.expander("💡 サンプル質問（各種グラフ対応）", expanded=False):
        st.markdown("""
- **棒グラフ** → 「カテゴリごとの売上を棒グラフで表示して」
- **折れ線グラフ（時系列）** → 「月別の売上推移を教えて」
- **円グラフ** → 「地域ごとの売上割合を円グラフで見せて」
- **散布図** → 「気温とアイスの売上の関係を散布図で見せて」
        """)

    # ユーザーの自然言語入力
    user_input = st.chat_input("自然言語で質問してください")

    if user_input and openai_api_key:
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("GPTがSQLを生成中..."):
                client = OpenAI(api_key=openai_api_key)

                # テーブルスキーマ抽出
                schema_desc = "\n".join(
                    [f"{col} ({dtype})" for col, dtype in zip(df.columns, df.dtypes)]
                )

                # GPTプロンプト
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

                    # SQL抽出と整形
                    raw_sql = response.choices[0].message.content.strip()
                    sql = re.sub(r"```sql|```", "", raw_sql).strip()

                    st.markdown(f"🧠 **生成されたSQL:**\n```sql\n{sql}\n```")

                    # SQL実行
                    result_df = duck_conn.execute(sql).fetchdf()
                    st.dataframe(result_df)

                    if result_df.shape[1] >= 2:
                        x, y = result_df.columns[0], result_df.columns[1]

                        # グラフタイプ推定
                        q = user_input.lower()
                        if any(w in q for w in ["割合", "比率", "シェア", "円"]):
                            chart_type = "pie"
                        elif any(w in q for w in ["相関", "関係", "関連"]):
                            chart_type = "scatter"
                        elif any(w in q for w in ["時間", "日時", "推移", "傾向", "月", "日", "時系列"]):
                            chart_type = "line"
                        else:
                            chart_type = "bar"

                        # xをdatetimeに変換試行（折れ線用）
                        try:
                            result_df[x] = pd.to_datetime(result_df[x])
                        except:
                            pass

                        # 💬 グラフごとの一言コメント
                        if chart_type == "pie":
                            st.info("📊 円グラフでカテゴリごとの割合を可視化します。")
                        elif chart_type == "scatter":
                            st.info("📈 散布図で2つの数値の関係性を視覚化します。相関係数も表示されます。")
                        elif chart_type == "line":
                            st.info("📈 折れ線グラフで時系列の推移を表示します。")
                        else:
                            st.info("📊 棒グラフでカテゴリ別の比較を表示します。")

                        # 散布図：数値変換
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

                                # ✅ 相関係数表示
                                try:
                                    corr = np.corrcoef(result_df[x], result_df[y])[0, 1]
                                    st.success(f"📊 **相関係数**は: `{corr:.3f}`です。")
                                except:
                                    st.warning("⚠️ 相関係数の計算に失敗しました。")
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
