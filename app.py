import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import re
import os
import base64
from openai import OpenAI
import faiss
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import warnings
warnings.filterwarnings('ignore')

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

**新機能**: テキストデータのクラスタリング分析も可能です！
- アンケートデータや自由記述テキストを自動でクラスタリング
- 3次元散布図で可視化
- 各クラスターの意見を自動要約
- 少数派の意見も見逃しません
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

# テキストクラスタリング機能の関数定義
def get_embeddings(texts, client):
    """OpenAI APIを使ってテキストをベクトル化"""
    embeddings = []
    batch_size = 100  # APIレート制限を考慮
    
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        try:
            response = client.embeddings.create(
                model="text-embedding-ada-002",
                input=batch
            )
            batch_embeddings = [item.embedding for item in response.data]
            embeddings.extend(batch_embeddings)
        except Exception as e:
            st.error(f"埋め込み生成エラー (batch {i//batch_size + 1}): {e}")
            return None
    
    return np.array(embeddings)

def cluster_texts_with_faiss(embeddings, n_clusters=5):
    """FAISSを使ってクラスタリング"""
    # 次元数
    d = embeddings.shape[1]
    
    # FAISSインデックスを作成（コサイン類似度用）
    # L2正規化してからインナープロダクトを使うことでコサイン類似度を計算
    faiss.normalize_L2(embeddings)
    index = faiss.IndexFlatIP(d)
    index.add(embeddings.astype(np.float32))
    
    # K-meansクラスタリング (scikit-learn使用、FAISSのk-meansは複雑なため)
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(embeddings)
    
    return cluster_labels, kmeans.cluster_centers_

def create_3d_visualization(embeddings, cluster_labels, texts, sample_size=1000):
    """3次元散布図を作成"""
    # データが多い場合はサンプリング
    if len(embeddings) > sample_size:
        indices = np.random.choice(len(embeddings), sample_size, replace=False)
        embeddings_sample = embeddings[indices]
        cluster_labels_sample = cluster_labels[indices]
        texts_sample = [texts[i] for i in indices]
    else:
        embeddings_sample = embeddings
        cluster_labels_sample = cluster_labels
        texts_sample = texts
    
    # PCAで3次元に削減
    pca = PCA(n_components=3)
    embeddings_3d = pca.fit_transform(embeddings_sample)
    
    # プロットデータ準備
    df_plot = pd.DataFrame({
        'x': embeddings_3d[:, 0],
        'y': embeddings_3d[:, 1],
        'z': embeddings_3d[:, 2],
        'cluster': cluster_labels_sample.astype(str),
        'text': [text[:100] + "..." if len(text) > 100 else text for text in texts_sample]
    })
    
    # 3D散布図作成
    fig = px.scatter_3d(
        df_plot, 
        x='x', 
        y='y', 
        z='z',
        color='cluster',
        hover_data=['text'],
        title="テキストデータのクラスタリング結果（3次元可視化）",
        labels={'cluster': 'クラスター'}
    )
    
    return fig, pca.explained_variance_ratio_

def summarize_clusters(texts, cluster_labels, client):
    """各クラスターの内容をGPTで要約"""
    n_clusters = len(np.unique(cluster_labels))
    summaries = []
    
    for cluster_id in range(n_clusters):
        cluster_texts = [texts[i] for i in range(len(texts)) if cluster_labels[i] == cluster_id]
        cluster_size = len(cluster_texts)
        
        # サンプルテキストを選択（最大20件）
        sample_texts = cluster_texts[:20] if len(cluster_texts) > 20 else cluster_texts
        
        # GPTで要約
        prompt = f"""
以下は同じクラスターに分類されたテキストデータ（全{cluster_size}件中の代表{len(sample_texts)}件）です。
このクラスターの共通した特徴や主要な意見・テーマを日本語で簡潔に要約してください。

テキストデータ:
{chr(10).join([f"- {text}" for text in sample_texts])}

要約は以下の形式でお願いします：
主なテーマ: [テーマ]
特徴: [特徴の説明]
代表的な意見: [意見の要約]
"""
        
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "あなたはテキスト分析の専門家です。与えられたテキストデータの共通点を見つけて簡潔に要約してください。"},
                    {"role": "user", "content": prompt}
                ]
            )
            summary = response.choices[0].message.content.strip()
            summaries.append({
                'cluster_id': cluster_id,
                'size': cluster_size,
                'percentage': round(cluster_size / len(texts) * 100, 1),
                'summary': summary
            })
        except Exception as e:
            summaries.append({
                'cluster_id': cluster_id,
                'size': cluster_size,
                'percentage': round(cluster_size / len(texts) * 100, 1),
                'summary': f"要約生成エラー: {e}"
            })
    
    return summaries

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

    # テキスト列を検出
    text_columns = []
    for col in df.columns:
        if df[col].dtype == 'object':  # 文字列型の列
            # 平均文字数をチェック（ある程度長いテキストかどうか）
            avg_length = df[col].dropna().astype(str).str.len().mean()
            if avg_length > 10:  # 平均10文字以上をテキストデータとみなす
                text_columns.append(col)

    # テキストクラスタリング機能の表示
    if text_columns:
        st.markdown("---")
        st.subheader("🔍 テキストデータのクラスタリング分析")
        
        with st.expander("💡 テキストクラスタリングとは", expanded=False):
            st.markdown("""
**テキストクラスタリング機能**では、アンケートの自由記述や意見データを自動で分析し、似たような内容をグループ化します。
- 大量のテキストデータから主要な意見の傾向を把握
- 少数派の意見も見逃さずに可視化
- 各グループの特徴を自動で要約
- 3次元散布図で直感的に理解
            """)
        
        selected_text_col = st.selectbox("📝 分析するテキスト列を選択", text_columns)
        n_clusters = st.slider("📊 クラスター数", min_value=2, max_value=10, value=5)
        
        if st.button("🚀 テキストクラスタリングを実行"):
            openai_api_key = st.secrets.get("OPENAI_API_KEY") or st.text_input("🔑 OpenAI APIキーを入力", type="password")
            
            if openai_api_key and selected_text_col:
                client = OpenAI(api_key=openai_api_key)
                
                # データの前処理
                text_data = df[selected_text_col].dropna().astype(str).tolist()
                text_data = [text for text in text_data if len(text.strip()) > 5]  # 短すぎるテキストを除去
                
                if len(text_data) < 10:
                    st.warning("⚠️ 分析に十分なテキストデータがありません（最低10件必要）")
                else:
                    with st.spinner("テキストをベクトル化中..."):
                        embeddings = get_embeddings(text_data, client)
                    
                    if embeddings is not None:
                        with st.spinner("クラスタリング実行中..."):
                            cluster_labels, cluster_centers = cluster_texts_with_faiss(embeddings, n_clusters)
                        
                        # 3次元可視化
                        st.subheader("📈 3次元散布図での可視化")
                        fig_3d, explained_variance = create_3d_visualization(embeddings, cluster_labels, text_data)
                        st.plotly_chart(fig_3d, use_container_width=True)
                        
                        st.info(f"📊 主成分分析による寄与率: {explained_variance[0]:.1%}, {explained_variance[1]:.1%}, {explained_variance[2]:.1%}")
                        
                        # クラスター要約
                        st.subheader("📝 各クラスターの要約")
                        with st.spinner("各クラスターを分析中..."):
                            summaries = summarize_clusters(text_data, cluster_labels, client)
                        
                        # 結果表示
                        for summary in sorted(summaries, key=lambda x: x['size'], reverse=True):
                            with st.expander(f"🏷️ クラスター {summary['cluster_id'] + 1} ({summary['size']}件, {summary['percentage']}%)", expanded=True):
                                st.markdown(summary['summary'])
                        
                        # 統計情報
                        st.subheader("📊 分析統計")
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("分析対象テキスト数", len(text_data))
                        
                        with col2:
                            st.metric("クラスター数", n_clusters)
                        
                        with col3:
                            largest_cluster_size = max([s['size'] for s in summaries])
                            smallest_cluster_size = min([s['size'] for s in summaries])
                            st.metric("最大/最小クラスター", f"{largest_cluster_size}/{smallest_cluster_size}")

    # 既存の機能（DuckDB処理）
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
                sample_data = df.head(3).to_string() 
                prompt = f"""
あなたはDuckDBに対してSQLを生成するアシスタントです。

基本ルール:
- テーブル名は `data` です
- ある程度曖昧な質問に対してもカラムを予測してSQLを発行してください
- 出力はSQL文のみ。コードブロックや装飾は不要です

重要な注意事項:
1. **列名の正確性**: 列名は下記スキーマと完全に一致させてください（括弧や特殊文字も含めて正確に）
2. **日付処理**: DuckDBでは文字列を日付関数に使う場合、必ず `CAST(列 AS DATE)` を使用してください
   ただし、日付型でない列（文字列、数値など）に対してはCASTしないでください
3. **相関・関係性分析**: 「関係」「相関」「関連」などの質問では、`SELECT col1, col2 FROM data` のように2列の数値データを含む結果を返してください（散布図描画のため）
4. **データ型の確認**: 下記のデータ型情報とサンプルデータを参考に、適切な列を選択してください
5. **文字列比較**: 文字列の比較では等号（=）やLIKEを使い、不適切なCASTは避けてください

データスキーマ:
{schema_desc}

サンプルデータ（参考）:
{sample_data}

質問: {user_input}
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
この結果から読み取れるポイントをユーザーの質問に結論から答えるように、日本語で簡潔に正確なデータを元に5行くらいで要約してください。

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
                    # エラーの種類に応じたヘルプを表示
                    error_str = str(e)
                    if "not found in FROM clause" in error_str:
                        st.info("💡 **列名エラー**: 以下の列名を参考に、正確な列名で質問してください")
                        col_list = "、".join([f"`{col}`" for col in df.columns])
                        st.markdown(f"**利用可能な列**: {col_list}")
                    elif "CAST" in error_str:
                        st.info("💡 **データ型エラー**: 日付でない列を日付として処理しようとしています")
                    else:
                        st.info("💡 質問を少し変えて、もう一度お試しください")
                    
                    # 参考用にデータのサンプルを表示
                    st.markdown("**データのサンプル（参考）:**")
                    st.dataframe(df.head())
