import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import re
from openai import OpenAI
import faiss
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import warnings
warnings.filterwarnings('ignore')
from dotenv import load_dotenv

# .envファイルから環境変数を読み込み
load_dotenv()

# 新しいコネクタシステムのインポート
try:
    from src.infrastructure.connectors.factory import ConnectorFactory
    USE_NEW_CONNECTORS = True
except ImportError as e:
    USE_NEW_CONNECTORS = False
    st.error(f"新しいコネクタシステムが利用できません: {e}")

st.set_page_config(page_title="Vizzye", layout="wide", initial_sidebar_state="expanded")

# セッション状態の初期化
if 'df' not in st.session_state:
    st.session_state.df = None
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'connector' not in st.session_state:
    st.session_state.connector = None
if 'messages' not in st.session_state:
    st.session_state.messages = []

# サイドバー
with st.sidebar:
    st.markdown("### データソース設定")
    
    # データソース選択
    if USE_NEW_CONNECTORS:
        data_sources = {
            "ローカルファイル📁": "local",
            "BigQuery🔍": "bigquery", 
            "Googleスプレッドシート🟩": "sheets",
            "Snowflake❄️": "snowflake",
            "Databricks🧱": "databricks"
        }
    else:
        data_sources = {
            "ローカルファイル": "local",
            "BigQuery": "bigquery",
            "Googleスプレッドシート": "sheets"
        }
    
    source = st.selectbox(
        "データソースを選択",
        list(data_sources.keys()),
        help="利用するデータソースを選択してください"
    )
    
    st.divider()
    
    # 各データソースの接続設定
    if source == "ローカルファイル📁":
        uploaded_file = st.file_uploader(
            "ファイルをアップロード",
            type=["csv", "parquet"],
            help="CSVまたはParquetファイルをアップロードしてください"
        )
        if uploaded_file:
            try:
                if uploaded_file.name.endswith(".csv"):
                    st.session_state.df = pd.read_csv(uploaded_file)
                else:
                    st.session_state.df = pd.read_parquet(uploaded_file)
                st.success("✅ ファイル読み込み成功！")
                st.session_state.connected = True
            except Exception as e:
                st.error(f"読み込みエラー: {e}")
    
    elif source == "BigQuery🔍":
        with st.expander("接続設定", expanded=True):
            sa_file = st.file_uploader(
                "サービスアカウントJSON",
                type="json",
                key="bq_sa",
                help="BigQueryのサービスアカウントJSONファイル"
            )
            
            if sa_file:
                if st.button("🔗 BigQueryに接続", key="bq_connect"):
                    try:
                        # 一時ファイル保存
                        with open("temp_bq.json", "wb") as f:
                            f.write(sa_file.getbuffer())
                        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "temp_bq.json"
                        
                        from google.cloud import bigquery
                        client = bigquery.Client()
                        st.session_state.bq_client = client
                        st.session_state.connected = True
                        st.success("✅ 接続成功！")
                    except Exception as e:
                        st.error(f"接続エラー: {e}")
        
        # 接続後のデータ選択
        if st.session_state.connected and hasattr(st.session_state, 'bq_client'):
            try:
                client = st.session_state.bq_client
                datasets = list(client.list_datasets())
                dataset_names = [d.dataset_id for d in datasets]
                
                selected_dataset = st.selectbox("データセット", dataset_names)
                
                if selected_dataset:
                    tables = list(client.list_tables(selected_dataset))
                    table_names = [t.table_id for t in tables]
                    selected_table = st.selectbox("テーブル", table_names)
                    
                    if selected_table:
                        if st.button("📥 データ取得", key="bq_fetch"):
                            with st.spinner("データ取得中..."):
                                full_table_id = f"{client.project}.{selected_dataset}.{selected_table}"
                                query = f"SELECT * FROM `{full_table_id}` LIMIT 1000"
                                st.session_state.df = client.query(query).to_dataframe()
                                st.success(f"✅ {len(st.session_state.df)}行のデータを取得")
            except Exception as e:
                st.error(f"エラー: {e}")
    
    elif source == "Snowflake❄️" and USE_NEW_CONNECTORS:
        with st.expander("接続設定", expanded=True):
            account = st.text_input("アカウント", placeholder="xxx.snowflakecomputing.com")
            username = st.text_input("ユーザー名")
            warehouse = st.text_input("ウェアハウス")
            
            private_key_file = st.file_uploader(
                "秘密鍵ファイル（PEM）",
                type=["pem", "key"],
                help="Programmatic Access Token用の秘密鍵"
            )
            passphrase = st.text_input("パスフレーズ（任意）", type="password")
            
            if st.button("🔗 Snowflakeに接続", key="sf_connect"):
                if all([account, username, warehouse, private_key_file]):
                    try:
                        private_key_content = private_key_file.read().decode('utf-8')
                        connector = ConnectorFactory.create_connector("snowflake")
                        credentials = {
                            "account": account,
                            "user": username,
                            "private_key": private_key_content,
                            "private_key_passphrase": passphrase if passphrase else None,
                            "warehouse": warehouse
                        }
                        
                        with st.spinner("接続中..."):
                            connector.connect(credentials)
                            st.session_state.connector = connector
                            st.session_state.connected = True
                            st.success("✅ 接続成功！")
                    except Exception as e:
                        st.error(f"接続エラー: {e}")
                else:
                    st.warning("すべての必須項目を入力してください")
        
        # 接続後のデータ選択
        if st.session_state.connected and st.session_state.connector:
            try:
                connector = st.session_state.connector
                databases = connector.list_datasets()
                selected_db = st.selectbox("データベース", databases)
                
                if selected_db:
                    # Snowflakeの場合はスキーマ選択も追加
                    if hasattr(connector, 'list_schemas'):
                        schemas = connector.list_schemas(selected_db)
                        selected_schema = st.selectbox("スキーマ", schemas)
                        
                        if selected_schema:
                            tables = connector.list_tables(selected_db, selected_schema)
                            selected_table = st.selectbox("テーブル", tables)
                            
                            if selected_table:
                                # セッション状態に保存（SQL生成時に使用）
                                st.session_state.selected_db = selected_db
                                st.session_state.selected_schema = selected_schema
                                st.session_state.selected_table = selected_table
                                
                                if st.button("📥 データ取得", key="sf_fetch"):
                                    with st.spinner("データ取得中..."):
                                        st.session_state.df = connector.get_sample_data(selected_db, selected_table, selected_schema)
                                        st.success(f"✅ {len(st.session_state.df)}行のデータを取得")
                    else:
                        tables = connector.list_tables(selected_db)
                        selected_table = st.selectbox("テーブル", tables)
                        
                        if selected_table:
                            if st.button("📥 データ取得", key="sf_fetch"):
                                with st.spinner("データ取得中..."):
                                    st.session_state.df = connector.get_sample_data(selected_db, selected_table)
                                    st.success(f"✅ {len(st.session_state.df)}行のデータを取得")
            except Exception as e:
                st.error(f"エラー: {e}")
    
    elif source == "Databricks🧱" and USE_NEW_CONNECTORS:
        with st.expander("接続設定", expanded=True):
            server_hostname = st.text_input("サーバーホスト", placeholder="xxx.cloud.databricks.com")
            http_path = st.text_input("HTTPパス", placeholder="/sql/1.0/endpoints/xxx")
            access_token = st.text_input("Access Token", type="password", help="Personal Access Token")
            catalog = st.text_input("カタログ（任意）")
            
            if st.button("🔗 Databricksに接続", key="db_connect"):
                if all([server_hostname, http_path, access_token]):
                    try:
                        connector = ConnectorFactory.create_connector("databricks")
                        credentials = {
                            "server_hostname": server_hostname,
                            "http_path": http_path,
                            "access_token": access_token,
                            "catalog": catalog if catalog else None
                        }
                        
                        with st.spinner("接続中..."):
                            connector.connect(credentials)
                            st.session_state.connector = connector
                            st.session_state.connected = True
                            st.success("✅ 接続成功！")
                    except Exception as e:
                        st.error(f"接続エラー: {e}")
                else:
                    st.warning("すべての必須項目を入力してください")
        
        # 接続後のデータ選択
        if st.session_state.connected and st.session_state.connector:
            try:
                connector = st.session_state.connector
                print(f"DEBUG - Connector class name: {type(connector).__name__}")
                
                catalogs = connector.list_datasets()
                selected_catalog = st.selectbox("カタログ", catalogs)
                
                if selected_catalog:
                    # SnowflakeとDatabricksの場合はスキーマ選択も追加
                    if type(connector).__name__ in ['SnowflakeConnector', 'DatabricksConnector']:
                        print(f"DEBUG - Schema selection UI should be shown")
                        schemas = connector.list_schemas(selected_catalog)
                        selected_schema = st.selectbox("スキーマ", schemas)
                        
                        if selected_schema:
                            tables = connector.list_tables(selected_catalog, selected_schema)
                            selected_table = st.selectbox("テーブル", tables)
                            
                            if selected_table:
                                # セッション状態に保存
                                st.session_state.selected_catalog = selected_catalog
                                st.session_state.selected_schema = selected_schema
                                st.session_state.selected_table = selected_table
                                
                                if st.button("📥 データ取得", key="db_fetch"):
                                    with st.spinner("データ取得中..."):
                                        st.session_state.df = connector.get_sample_data(selected_catalog, selected_table, schema=selected_schema)
                                        st.success(f"✅ {len(st.session_state.df)}行のデータを取得")
                    else:
                        print(f"DEBUG - Schema selection UI NOT shown for {type(connector).__name__}")
                        tables = connector.list_tables(selected_catalog)
                        selected_table = st.selectbox("テーブル", tables)
                        
                        if selected_table:
                            if st.button("📥 データ取得", key="db_fetch"):
                                with st.spinner("データ取得中..."):
                                    st.session_state.df = connector.get_sample_data(selected_catalog, selected_table)
                                    st.success(f"✅ {len(st.session_state.df)}行のデータを取得")
            except Exception as e:
                st.error(f"エラー: {e}")
    
    elif source == "Googleスプレッドシート🟩" and USE_NEW_CONNECTORS:
        with st.expander("接続設定", expanded=True):
            sa_file = st.file_uploader(
                "サービスアカウントJSON",
                type="json",
                key="gs_sa",
                help="Google SheetsAPIアクセス用のサービスアカウントJSONファイル"
            )
            sheet_url = st.text_input("スプレッドシートURL", placeholder="https://docs.google.com/spreadsheets/d/...")
            
            if st.button("🔗 Googleスプレッドシートに接続", key="gs_connect"):
                if all([sa_file, sheet_url]):
                    try:
                        # 一時ファイル保存
                        with open("temp_gs.json", "wb") as f:
                            f.write(sa_file.getbuffer())
                        
                        connector = ConnectorFactory.create_connector("google_sheets")
                        credentials = {
                            "service_account_file": "temp_gs.json",
                            "sheet_url": sheet_url
                        }
                        
                        with st.spinner("接続中..."):
                            connector.connect(credentials)
                            st.session_state.connector = connector
                            st.session_state.connected = True
                            st.success("✅ 接続成功！")
                    except Exception as e:
                        st.error(f"接続エラー: {e}")
                else:
                    st.warning("すべての必須項目を入力してください")
        
        # 接続後のデータ選択
        if st.session_state.connected and st.session_state.connector:
            try:
                connector = st.session_state.connector
                sheets = connector.list_tables("")  # Google Sheetsではdatasetパラメータ不要
                selected_sheet = st.selectbox("シート", sheets)
                
                if selected_sheet:
                    if st.button("📥 データ取得", key="gs_fetch"):
                        with st.spinner("データ取得中..."):
                            st.session_state.df = connector.get_sample_data("", selected_sheet)
                            st.success(f"✅ {len(st.session_state.df)}行のデータを取得")
            except Exception as e:
                st.error(f"エラー: {e}")

# メインエリア
st.title("🧞 Vizzy - Adhoc Analytics Assistant")

# データがロードされているかチェック
if st.session_state.df is not None:
    df = st.session_state.df

    # 日付カラムの自動変換
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass
    
    # データソースの種類を判定
    if hasattr(st.session_state, 'connector') and st.session_state.connector:
        connector = st.session_state.connector
        dialect = connector.get_dialect() if hasattr(connector, 'get_dialect') else 'duckdb'
    else:
        # ローカルファイルの場合はDuckDBを使用
        dialect = 'duckdb'
        duck_conn = duckdb.connect()
        duck_conn.register("data", df)

    # カラム分割: 左にデータプレビュー、右にチャット
    col_left, col_right = st.columns([1, 2])

    with col_left:
        st.subheader("📊 データプレビュー")
        st.write(f"データサイズ: {len(df):,}行 × {len(df.columns)}列")
        st.dataframe(df.head(100), height=600)

    with col_right:
        st.subheader("💬 データ分析チャット")

        # チャット履歴を表示
        for idx, message in enumerate(st.session_state.messages):
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
                # アシスタントのメッセージにデータとグラフを表示
                if message["role"] == "assistant" and "data" in message:
                    if "sql" in message:
                        with st.expander("生成されたSQL"):
                            st.code(message["sql"], language="sql")
                    if "dataframe" in message:
                        st.dataframe(message["dataframe"])
                    if "figure" in message:
                        st.plotly_chart(message["figure"], width="stretch")
                    if "summary" in message:
                        with st.expander("分析要約", expanded=True):
                            st.markdown(message["summary"])

                    # ダウンロードボタン
                    if "dataframe" in message and "timestamp" in message:
                        col1, col2 = st.columns(2)
                        with col1:
                            # HTMLレポート生成
                            html_report = f"""
                            <html>
                            <head>
                                <title>Vizzy分析レポート - {message['timestamp'].strftime('%Y/%m/%d %H:%M')}</title>
                                <style>
                                    body {{ font-family: Arial, sans-serif; margin: 40px; }}
                                    h1, h2 {{ color: #333; }}
                                    .query {{ background-color: #f0f0f0; padding: 10px; border-radius: 5px; }}
                                    .sql {{ background-color: #e8e8e8; padding: 10px; border-radius: 5px; font-family: monospace; white-space: pre-wrap; }}
                                    .summary {{ background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin: 20px 0; }}
                                    table {{ border-collapse: collapse; width: 100%; }}
                                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                                    th {{ background-color: #4CAF50; color: white; }}
                                </style>
                            </head>
                            <body>
                                <h1>Vizzy 分析レポート</h1>
                                <p><strong>作成日時:</strong> {message['timestamp'].strftime('%Y年%m月%d日 %H:%M:%S')}</p>

                                <h2>質問</h2>
                                <div class="query">{message.get('question', '')}</div>

                                <h2>実行したSQL</h2>
                                <div class="sql">{message.get('sql', '')}</div>

                                <h2>分析要約</h2>
                                <div class="summary">{message.get('summary', '要約なし')}</div>

                                <h2>グラフ</h2>
                                {message.get('figure', '').to_html() if 'figure' in message else '<p>グラフなし</p>'}

                                <h2>データ（上位20行）</h2>
                                {message['dataframe'].head(20).to_html()}
                            </body>
                            </html>
                            """

                            st.download_button(
                                label="📄 HTMLレポート",
                                data=html_report,
                                file_name=f"vizzy_report_{message['timestamp'].strftime('%Y%m%d_%H%M%S')}.html",
                                mime="text/html",
                                key=f"html_{idx}"
                            )

                        with col2:
                            csv = message['dataframe'].to_csv(index=False)
                            st.download_button(
                                label="📊 CSVデータ",
                                data=csv,
                                file_name=f"vizzy_data_{message['timestamp'].strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                key=f"csv_{idx}"
                            )

        # チャット入力
        if prompt := st.chat_input("質問を入力してください（例: 月別の売上推移を見せて）"):
            # ユーザーメッセージを表示
            with st.chat_message("user"):
                st.markdown(prompt)
            
            # ユーザーメッセージを履歴に追加
            st.session_state.messages.append({"role": "user", "content": prompt})

            # APIキー取得
            openai_api_key = os.getenv("OPENAI_API_KEY")
            if not openai_api_key:
                st.error("OpenAI APIキーが設定されていません。.envファイルにOPENAI_API_KEYを設定してください。")
                st.stop()

            client = OpenAI(api_key=openai_api_key)

            # スキーマ情報取得
            schema = {}
            for col in df.columns:
                dtype = str(df[col].dtype)
                schema[col] = dtype

            # サンプルデータ
            sample_data = df.head(3).to_string()

            # SQL生成プロンプト（データベース別に最適化）
            if dialect == 'snowflake':
                if hasattr(st.session_state, 'selected_db') and hasattr(st.session_state, 'selected_schema') and hasattr(st.session_state, 'selected_table'):
                    table_ref = f"{st.session_state.selected_db}.{st.session_state.selected_schema}.{st.session_state.selected_table}"
                else:
                    table_ref = "data"

                sql_generation_prompt = f"""
以下のテーブル情報を基に、ユーザーの質問に答えるSnowflake SQLクエリを生成してください。

テーブル名: {table_ref}
カラム情報: {schema}

サンプルデータ:
{sample_data}

ユーザーの質問: {prompt}

重要な指示:
- Snowflakeの構文を使用すること
- 日付関数: DATE_TRUNC(), DATEADD(), DATEDIFF()など
- 文字列関数: CONCAT(), SPLIT_PART(), REGEXP_SUBSTR()など
- グラフを要求された場合は、適切なGROUP BYとORDER BYを含める
- SQLクエリのみを返す（説明は不要）
"""
            elif dialect == 'bigquery':
                sql_generation_prompt = f"""
以下のテーブル情報を基に、ユーザーの質問に答えるBigQuery SQLクエリを生成してください。

テーブル名: data
カラム情報: {schema}

サンプルデータ:
{sample_data}

ユーザーの質問: {prompt}

重要な指示:
- BigQueryの標準SQL構文を使用すること
- 日付関数: DATE_TRUNC(), DATE_ADD(), DATE_DIFF()など
- ARRAY、STRUCTなどの複雑な型も考慮
- グラフを要求された場合は、適切なGROUP BYとORDER BYを含める
- SQLクエリのみを返す（説明は不要）
"""
            elif dialect == 'databricks':
                if hasattr(st.session_state, 'selected_catalog') and hasattr(st.session_state, 'selected_schema') and hasattr(st.session_state, 'selected_table'):
                    table_ref = f"{st.session_state.selected_catalog}.{st.session_state.selected_schema}.{st.session_state.selected_table}"
                else:
                    table_ref = "data"

                sql_generation_prompt = f"""
以下のテーブル情報を基に、ユーザーの質問に答えるDatabricks SQLクエリを生成してください。

テーブル名: {table_ref}
カラム情報: {schema}

サンプルデータ:
{sample_data}

ユーザーの質問: {prompt}

重要な指示:
- Databricksの構文を使用すること（Spark SQLベース）
- 日付関数: date_trunc(), date_add(), datediff()など
- カタログ.スキーマ.テーブル形式の完全修飾名を使用
- グラフを要求された場合は、適切なGROUP BYとORDER BYを含める
- SQLクエリのみを返す（説明は不要）
"""
            else:  # DuckDB (デフォルト)
                sql_generation_prompt = f"""
以下のテーブル情報を基に、ユーザーの質問に答えるDuckDB SQLクエリを生成してください。

テーフル名: data
カラム情報: {schema}

サンプルデータ:
{sample_data}

ユーザーの質問: {prompt}

重要な指示:
- DuckDBの構文を使用すること
- 日付型のカラムはCAST(column_name AS DATE)を使用
- グラフを要求された場合は、適切なGROUP BYとORDER BYを含める
- SQLクエリのみを返す（説明は不要）
"""

            try:
                with st.chat_message("assistant"):
                    with st.spinner("SQL生成中..."):
                        response = client.chat.completions.create(
                            model="gpt-3.5-turbo",
                            messages=[
                                {"role": "system", "content": "あなたはSQL生成の専門家です。"},
                                {"role": "user", "content": sql_generation_prompt}
                            ]
                        )

                    sql_query = response.choices[0].message.content.strip()
                    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

                    with st.expander("生成されたSQL", expanded=False):
                        st.code(sql_query, language="sql")

                    # クエリ実行
                    try:
                        with st.spinner("クエリ実行中..."):
                            if dialect in ['snowflake', 'bigquery', 'databricks'] and hasattr(connector, 'execute_query'):
                                result_df = connector.execute_query(sql_query)
                            else:
                                result_df = duck_conn.execute(sql_query).fetchdf()

                        st.dataframe(result_df)

                        # 分析要約の生成
                        with st.spinner("分析結果を要約中..."):
                            summary_prompt = f"""
以下の分析結果を要約してください：

ユーザーの質問: {prompt}
実行したSQL: {sql_query}

結果データ（上位10行）:
{result_df.head(10).to_string()}

以下の形式で要約してください：
1. 主な発見（2-3個の重要なポイント）
2. データの傾向や特徴
3. ビジネス上の示唆（あれば）

簡潔で分かりやすい日本語で記述してください。
"""
                            try:
                                summary_response = client.chat.completions.create(
                                    model="gpt-3.5-turbo",
                                    messages=[
                                        {"role": "system", "content": "あなたはデータ分析の専門家です。"},
                                        {"role": "user", "content": summary_prompt}
                                    ]
                                )
                                analysis_summary = summary_response.choices[0].message.content.strip()

                                with st.expander("分析要約", expanded=True):
                                    st.markdown(analysis_summary)
                            except Exception as e:
                                st.warning(f"要約生成エラー: {e}")
                                analysis_summary = "要約を生成できませんでした。"

                        # グラフ生成
                        fig = None
                        if len(result_df.columns) >= 2:
                            query_lower = prompt.lower()
                            colors = ['#4361ee', '#3f37c9', '#7209b7', '#b5179e', '#f72585',
                                     '#4cc9f0', '#4895ef', '#480ca8', '#560bad', '#6a4c93']

                            if any(word in prompt for word in ["円", "割合", "比率", "構成", "内訳"]) or "pie" in query_lower:
                                fig = px.pie(result_df, names=result_df.columns[0], values=result_df.columns[1],
                                           title=prompt, color_discrete_sequence=colors)
                                fig.update_layout(plot_bgcolor='white', paper_bgcolor='white', font=dict(color='#333333'))
                                st.plotly_chart(fig, width="stretch")

                            elif any(word in prompt for word in ["時系列", "推移", "変化", "折れ線"]) or any(word in query_lower for word in ["trend", "line"]):
                                fig = px.line(result_df, x=result_df.columns[0], y=result_df.columns[1],
                                            title=prompt, color_discrete_sequence=['#4361ee'])
                                fig.update_layout(plot_bgcolor='white', paper_bgcolor='white', font=dict(color='#333333'),
                                                xaxis=dict(gridcolor='#e0e0e0'), yaxis=dict(gridcolor='#e0e0e0'))
                                st.plotly_chart(fig, width="stretch")

                            elif any(word in prompt for word in ["関係", "相関", "散布"]) or any(word in query_lower for word in ["scatter", "correlation"]):
                                fig = px.scatter(result_df, x=result_df.columns[0], y=result_df.columns[1],
                                               title=prompt, color_discrete_sequence=['#4361ee'])
                                fig.update_layout(plot_bgcolor='white', paper_bgcolor='white', font=dict(color='#333333'),
                                                xaxis=dict(gridcolor='#e0e0e0'), yaxis=dict(gridcolor='#e0e0e0'))
                                st.plotly_chart(fig, width="stretch")

                            else:
                                if len(result_df) > 0:
                                    result_df_sorted = result_df.sort_values(by=result_df.columns[1], ascending=False)
                                else:
                                    result_df_sorted = result_df
                                fig = px.bar(result_df_sorted, x=result_df_sorted.columns[0], y=result_df_sorted.columns[1],
                                           title=prompt, color_discrete_sequence=['#4361ee'])
                                fig.update_layout(plot_bgcolor='white', paper_bgcolor='white', font=dict(color='#333333'),
                                                xaxis=dict(gridcolor='#e0e0e0'), yaxis=dict(gridcolor='#e0e0e0'))
                                st.plotly_chart(fig, width="stretch")

                        # アシスタントメッセージを履歴に追加
                        assistant_message = {
                            "role": "assistant",
                            "content": f"分析結果を表示しました。",
                            "data": True,
                            "sql": sql_query,
                            "dataframe": result_df,
                            "summary": analysis_summary,
                            "question": prompt,
                            "timestamp": pd.Timestamp.now()
                        }
                        if fig:
                            assistant_message["figure"] = fig
                        st.session_state.messages.append(assistant_message)

                        # 新しく生成された結果のダウンロードボタン
                        col1, col2 = st.columns(2)
                        with col1:
                            # HTMLレポート生成
                            html_report = f"""
                            <html>
                            <head>
                                <title>Vizzy分析レポート - {assistant_message['timestamp'].strftime('%Y/%m/%d %H:%M')}</title>
                                <style>
                                    body {{ font-family: Arial, sans-serif; margin: 40px; }}
                                    h1, h2 {{ color: #333; }}
                                    .query {{ background-color: #f0f0f0; padding: 10px; border-radius: 5px; }}
                                    .sql {{ background-color: #e8e8e8; padding: 10px; border-radius: 5px; font-family: monospace; white-space: pre-wrap; }}
                                    .summary {{ background-color: #f9f9f9; padding: 15px; border-radius: 5px; margin: 20px 0; }}
                                    table {{ border-collapse: collapse; width: 100%; }}
                                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                                    th {{ background-color: #4CAF50; color: white; }}
                                </style>
                            </head>
                            <body>
                                <h1>Vizzy 分析レポート</h1>
                                <p><strong>作成日時:</strong> {assistant_message['timestamp'].strftime('%Y年%m月%d日 %H:%M:%S')}</p>

                                <h2>質問</h2>
                                <div class="query">{prompt}</div>

                                <h2>実行したSQL</h2>
                                <div class="sql">{sql_query}</div>

                                <h2>分析要約</h2>
                                <div class="summary">{analysis_summary}</div>

                                <h2>グラフ</h2>
                                {fig.to_html() if fig else '<p>グラフなし</p>'}

                                <h2>データ（上位20行）</h2>
                                {result_df.head(20).to_html()}
                            </body>
                            </html>
                            """

                            st.download_button(
                                label="📄 HTMLレポート",
                                data=html_report,
                                file_name=f"vizzy_report_{assistant_message['timestamp'].strftime('%Y%m%d_%H%M%S')}.html",
                                mime="text/html",
                                key="html_new"
                            )

                        with col2:
                            csv = result_df.to_csv(index=False)
                            st.download_button(
                                label="📊 CSVデータ",
                                data=csv,
                                file_name=f"vizzy_data_{assistant_message['timestamp'].strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv",
                                key="csv_new"
                            )

                    except Exception as e:
                        st.error(f"SQLエラー: {e}")

            except Exception as e:
                st.error(f"AI生成エラー: {e}")

else:
    # データ未ロード時の案内
    st.info("左のサイドバーからデータソースを選択してください")

    with st.expander("使い方", expanded=True):
        st.markdown("""
### 🚀 クイックスタート

1. **データソースを選択**: サイドバーから利用するデータソースを選択
2. **接続設定**: 必要な認証情報を入力して接続
3. **データ取得**: テーブルを選択してデータを取得
4. **自然言語で分析**: チャットで質問を入力して分析を実行

### 📊 対応データソース

- **ローカルファイル**: CSV, Parquet
- **BigQuery**: Google Cloud BigQuery
- **Snowflake**: Programmatic Access Token認証
- **Databricks**: Personal Access Token認証
- **Google Sheets**: サービスアカウント認証

### 💡 質問例

- 「月別の売上推移を見せて」
- 「カテゴリ別の売上を棒グラフで表示」
- 「上位10商品の売上割合を円グラフで」
- 「昨年同月比の成長率を計算して」
        """)
