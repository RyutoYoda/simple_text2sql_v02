import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import numpy as np
import re
import os
import base64
from openai import OpenAI

st.set_page_config(page_title="Vizzye", layout="wide")
st.title("🧞 Vizzy")

# ロゴ画像表示
def load_image(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

image_path = "vizzy_logo.png"
if os.path.exists(image_path):
    image_base64 = load_image(image_path)
    st.markdown(
        f"""<div style="text-align: center;">
        <img src="data:image/png;base64,{image_base64}" alt="image" style="width: 100%;"/>
        </div>""",
        unsafe_allow_html=True
    )

# 説明
with st.expander("Vizzyとは❔", expanded=False):
    st.markdown("""
**Vizzy** は、自然言語でデータに質問できるビジュアル生成アプリです。  
CSV / Parquet / BigQuery / Googleスプレッドシートに対応しています。
""")

# データソース選択
source = st.selectbox("📂 データソースを選択", ["ローカルファイル", "BigQuery", "Googleスプレッドシート"])

df = None

# ローカルファイル
if source == "ローカルファイル":
    uploaded_file = st.file_uploader("📄 CSVまたはParquetファイルをアップロード", type=["csv", "parquet"])
    if uploaded_file:
        df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith(".csv") else pd.read_parquet(uploaded_file)

# BigQuery
elif source == "BigQuery":
    sa_file = st.file_uploader("🔐 BigQueryサービスアカウントJSONをアップロード", type="json", key="bq")
    if sa_file:
        with open("temp_bq.json", "wb") as f:
            f.write(sa_file.getbuffer())
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "temp_bq.json"

        from google.cloud import bigquery
        try:
            client = bigquery.Client()
            datasets = list(client.list_datasets())
            dataset_names = [d.dataset_id for d in datasets]
            selected_dataset = st.selectbox("データセット", dataset_names)
            if selected_dataset:
                tables = list(client.list_tables(selected_dataset))
                table_names = [t.table_id for t in tables]
                selected_table = st.selectbox("テーブル", table_names)
                if selected_table:
                    full_table_id = f"{client.project}.{selected_dataset}.{selected_table}"
                    df = client.query(f"SELECT * FROM `{full_table_id}` LIMIT 1000").to_dataframe()
        except Exception as e:
            st.error(f"BigQueryエラー: {e}")

# Googleスプレッドシート
elif source == "Googleスプレッドシート":
    sa_file = st.file_uploader("🔐 スプレッドシートサービスアカウントJSONをアップロード", type="json", key="sheet")
    if sa_file:
        with open("temp_sheet.json", "wb") as f:
            f.write(sa_file.read())
        import gspread
        try:
            gc = gspread.service_account(filename="temp_sheet.json")
            sheet_url = st.text_input("📄 スプレッドシートのURLを入力")
            if sheet_url:
                sh = gc.open_by_url(sheet_url)
                worksheet_names = [ws.title for ws in sh.worksheets()]
                selected_ws = st.selectbox("🧾 シートを選択", worksheet_names)
                if selected_ws:
                    ws = sh.worksheet(selected_ws)
                    data = ws.get_all_records()
                    df = pd.DataFrame(data)
        except Exception as e:
            st.error(f"スプレッドシートエラー: {e}")

# 共通処理
if df is not None:
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass

    st.success("✅ データを読み込みました")
    st.dataframe(df.head())

    # DuckDB登録
    duck_conn = duckdb.connect()
    duck_conn.register("data", df)

    # サンプル
    with st.expander("💡 サンプル質問（各種グラフ対応）", expanded=False):
        st.markdown("""
- 「カテゴリごとの売上を棒グラフで表示して」
- 「月別の売上推移を教えて」
- 「地域ごとの売上割合を円グラフで見せて」
- 「気温と売上の関係を散布図で見せて」
        """)

    # OpenAI APIキー
    openai_api_key = st.secrets.get("OPENAI_API_KEY") or st.text_input("🔑 OpenAI APIキーを入力", type="password")
    user_input = st.chat_input("自然言語で質問してください")

    if user_input and openai_api_key:
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("分析中..."):
                client = OpenAI(api_key=openai_api_key)

                schema_desc = "\n".join([f"{col} ({dtype})" for col, dtype in zip(df.columns, df.dtypes)])
                prompt = f"""
あなたはDuckDBに対してSQLを生成するアシスタントです。
テーブル名は `data` です。

DuckDBでは文字列を日付関数に使う場合、必ず `CAST(列 AS DATE)` を使用してください。
「関係」「相関」「関連」などの質問では、`SELECT col1, col2 FROM data` のように2列の数値データを含む結果を返してください（散布図描画のため）。
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

                        q = user_input.lower()
                        if any(w in q for w in ["割合", "比率", "シェア", "円"]):
                            chart_type = "pie"
                        elif any(w in q for w in ["相関", "関係", "関連"]):
                            chart_type = "scatter"
                        elif any(w in q for w in ["時間", "日時", "推移", "傾向", "月", "日", "時系列"]):
                            chart_type = "line"
                        else:
                            chart_type = "bar"

                        try:
                            result_df[x] = pd.to_datetime(result_df[x])
                        except:
                            pass

                        if chart_type == "pie":
                            st.info("📊 円グラフでカテゴリごとの割合を可視化しています")
                            fig = px.pie(result_df, names=x, values=y)
                        elif chart_type == "scatter":
                            st.info("📈 散布図で2つの数値の関係性を視覚化しています。")
                            result_df[x] = pd.to_numeric(result_df[x], errors='coerce')
                            result_df[y] = pd.to_numeric(result_df[y], errors='coerce')
                            fig = px.scatter(result_df, x=x, y=y)
                            if pd.api.types.is_numeric_dtype(result_df[x]) and pd.api.types.is_numeric_dtype(result_df[y]):
                                try:
                                    corr = np.corrcoef(result_df[x], result_df[y])[0, 1]
                                    st.success(f"📊 **相関係数**: `{corr:.3f}`")
                                except:
                                    st.warning("⚠️ 相関係数の計算に失敗しました。")
                        elif chart_type == "line":
                            st.info("📈 折れ線グラフで時系列の推移を表示しています。")
                            fig = px.line(result_df, x=x, y=y)
                        else:
                            st.info("📊 棒グラフでカテゴリ別の比較を表示しています。")
                            fig = px.bar(result_df, x=x, y=y)

                        st.plotly_chart(fig, use_container_width=True)

                        # 🔍 AIによるグラフ要約
                        summary_prompt = f"""
以下のデータは「{chart_type}」グラフで可視化されたものです。
ユーザーの質問「{user_input}」に対する結果です。
この結果から読み取れるポイントをユーザーの質問に結論から答えるように、日本語で簡潔に性格なデータを元に5行くらいで要約してください。

{result_df.head(20).to_csv(index=False)}
"""
                        try:
                            summary_response = client.chat.completions.create(
                                model="gpt-3.5-turbo",
                                messages=[
                                    {"role": "system", "content": "あなたはデータ可視化の専門家で、グラフから読み取れる内容をわかりやすく要約します。"},
                                    {"role": "user", "content": summary_prompt}
                                ]
                            )
                            summary_text = summary_response.choices[0].message.content.strip()
                            st.markdown("📝 **グラフの要約:**")
                            st.success(summary_text)

                        except Exception as e:
                            st.warning(f"要約の生成に失敗しました: {e}")

                    else:
                        st.info("📉 グラフ描画には2列以上の結果が必要です。")

                except Exception as e:
                    st.error(f"❌ エラー: {e}")

