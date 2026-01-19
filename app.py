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

# SQLバリデーション関数
def is_safe_query(sql: str) -> tuple[bool, str]:
    """
    SELECT文のみを許可するバリデーション

    Returns:
        (bool, str): (安全かどうか, エラーメッセージ)
    """
    sql_stripped = sql.strip()
    if not sql_stripped:
        return False, "SQLクエリが空です"

    # 大文字に変換してチェック（コメントや文字列リテラルを考慮）
    sql_upper = sql_stripped.upper()

    # WITH句（CTE）をサポート
    if sql_upper.startswith('WITH'):
        # WITH句の場合、最終的なSELECTがあるかチェック
        if 'SELECT' not in sql_upper:
            return False, "WITH句の後にSELECT文が必要です"
    elif not sql_upper.startswith('SELECT'):
        return False, "SELECT文のみ実行可能です"

    # 危険なキーワードをチェック
    dangerous_keywords = [
        'UPDATE', 'DELETE', 'DROP', 'INSERT', 'CREATE',
        'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE', 'EXEC',
        'EXECUTE', 'MERGE', 'REPLACE'
    ]

    for keyword in dangerous_keywords:
        # 単語境界を考慮（例: SELECT内の"UPDATE"は許可）
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, sql_upper):
            return False, f"危険なSQL操作が検出されました: {keyword}"

    return True, ""

# セッション状態の初期化
if 'data_sources' not in st.session_state:
    st.session_state.data_sources = {}  # {データソース名: {type, df, connector, ...}}
if 'active_source' not in st.session_state:
    st.session_state.active_source = None
if 'messages' not in st.session_state:
    st.session_state.messages = {}  # {データソース名: [messages]}
if 'source_counter' not in st.session_state:
    st.session_state.source_counter = 0

# サイドバー
with st.sidebar:
    st.markdown("### データソース管理")

    # 接続済みデータソース一覧
    if st.session_state.data_sources:
        st.markdown("#### 📂 接続済みデータソース")

        source_names = list(st.session_state.data_sources.keys())

        # アクティブソース選択
        active_idx = source_names.index(st.session_state.active_source) if st.session_state.active_source in source_names else 0
        selected_source = st.selectbox(
            "表示するデータソース",
            source_names,
            index=active_idx,
            help="分析するデータソースを選択"
        )

        if selected_source != st.session_state.active_source:
            st.session_state.active_source = selected_source
            st.rerun()

        # 削除ボタン
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("🗑️", key="delete_source", help="選択中のデータソースを削除"):
                del st.session_state.data_sources[selected_source]
                if selected_source in st.session_state.messages:
                    del st.session_state.messages[selected_source]
                st.session_state.active_source = list(st.session_state.data_sources.keys())[0] if st.session_state.data_sources else None
                st.rerun()

        st.divider()

    # 新規データソース追加
    st.markdown("#### ➕ 新しいデータソースを追加")

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
        "データソース種類",
        list(data_sources.keys()),
        help="追加するデータソースを選択してください"
    )

    st.divider()
    
    # 各データソースの接続設定
    if source == "ローカルファイル📁":
        source_name = st.text_input("データソース名", placeholder="例: 売上データ_2024")
        uploaded_file = st.file_uploader(
            "ファイルをアップロード",
            type=["csv", "parquet", "xlsx", "xls"],
            help="CSV、Parquet、またはExcelファイルをアップロードしてください",
            key="local_file_uploader"
        )
        if uploaded_file and source_name:
            if st.button("📥 追加", key="add_local"):
                try:
                    if uploaded_file.name.endswith(".csv"):
                        df = pd.read_csv(uploaded_file)
                    elif uploaded_file.name.endswith(".parquet"):
                        df = pd.read_parquet(uploaded_file)
                    elif uploaded_file.name.endswith((".xlsx", ".xls")):
                        df = pd.read_excel(uploaded_file)

                    # データソースを追加
                    st.session_state.data_sources[source_name] = {
                        "type": "local",
                        "df": df,
                        "connector": None,
                        "file_name": uploaded_file.name
                    }
                    st.session_state.active_source = source_name
                    st.session_state.messages[source_name] = []
                    st.success(f"✅ {source_name}を追加しました！")
                    st.rerun()
                except Exception as e:
                    st.error(f"読み込みエラー: {e}")
    
    elif source == "BigQuery🔍":
        with st.expander("接続設定", expanded=True):
            source_name = st.text_input("データソース名", placeholder="例: プロダクトDB", key="bq_name")
            sa_file = st.file_uploader(
                "サービスアカウントJSON",
                type="json",
                key="bq_sa",
                help="BigQueryのサービスアカウントJSONファイル"
            )

            # 一時的な接続状態
            if 'temp_bq_client' not in st.session_state:
                st.session_state.temp_bq_client = None

            if sa_file and source_name:
                if st.button("🔗 BigQueryに接続", key="bq_connect"):
                    try:
                        # 一時ファイル保存
                        with open("temp_bq.json", "wb") as f:
                            f.write(sa_file.getbuffer())
                        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "temp_bq.json"

                        from google.cloud import bigquery
                        client = bigquery.Client()
                        st.session_state.temp_bq_client = client
                        st.success("✅ 接続成功！データセットとテーブルを選択してください")
                        st.rerun()
                    except Exception as e:
                        st.error(f"接続エラー: {e}")

        # 接続後のデータ選択
        if st.session_state.temp_bq_client:
            try:
                client = st.session_state.temp_bq_client
                datasets = list(client.list_datasets())
                dataset_names = [d.dataset_id for d in datasets]

                selected_dataset = st.selectbox("データセット", dataset_names, key="bq_dataset")

                if selected_dataset:
                    tables = list(client.list_tables(selected_dataset))
                    table_names = [t.table_id for t in tables]
                    selected_table = st.selectbox("テーブル", table_names, key="bq_table")

                    if selected_table:
                        if st.button("📥 追加", key="add_bq"):
                            with st.spinner("データ取得中..."):
                                full_table_id = f"{client.project}.{selected_dataset}.{selected_table}"
                                query = f"SELECT * FROM `{full_table_id}` LIMIT 1000"
                                df = client.query(query).to_dataframe()

                                source_name = st.session_state.get("bq_name", f"BigQuery_{st.session_state.source_counter}")
                                st.session_state.source_counter += 1

                                # データソースを追加
                                st.session_state.data_sources[source_name] = {
                                    "type": "bigquery",
                                    "df": df,
                                    "connector": client,
                                    "dataset": selected_dataset,
                                    "table": selected_table
                                }
                                st.session_state.active_source = source_name
                                st.session_state.messages[source_name] = []
                                st.session_state.temp_bq_client = None
                                st.success(f"✅ {source_name}を追加しました！")
                                st.rerun()
            except Exception as e:
                st.error(f"エラー: {e}")
    
    elif source == "Snowflake❄️" and USE_NEW_CONNECTORS:
        with st.expander("接続設定", expanded=True):
            source_name = st.text_input("データソース名", placeholder="例: Snowflake本番DB", key="sf_name")
            account = st.text_input("アカウント", placeholder="xxx.snowflakecomputing.com", key="sf_account")
            username = st.text_input("ユーザー名", key="sf_username")
            warehouse = st.text_input("ウェアハウス", key="sf_warehouse")

            private_key_file = st.file_uploader(
                "秘密鍵ファイル（PEM）",
                type=["pem", "key"],
                help="Programmatic Access Token用の秘密鍵",
                key="sf_key"
            )
            passphrase = st.text_input("パスフレーズ（任意）", type="password", key="sf_pass")

            # 一時的な接続状態
            if 'temp_sf_connector' not in st.session_state:
                st.session_state.temp_sf_connector = None

            if all([account, username, warehouse, private_key_file, source_name]):
                if st.button("🔗 Snowflakeに接続", key="sf_connect"):
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
                            st.session_state.temp_sf_connector = connector
                            st.success("✅ 接続成功！データベースとテーブルを選択してください")
                            st.rerun()
                    except Exception as e:
                        st.error(f"接続エラー: {e}")

        # 接続後のデータ選択
        if st.session_state.temp_sf_connector:
            try:
                connector = st.session_state.temp_sf_connector
                databases = connector.list_datasets()
                selected_db = st.selectbox("データベース", databases, key="sf_db")

                if selected_db:
                    if hasattr(connector, 'list_schemas'):
                        schemas = connector.list_schemas(selected_db)
                        selected_schema = st.selectbox("スキーマ", schemas, key="sf_schema")

                        if selected_schema:
                            tables = connector.list_tables(selected_db, selected_schema)
                            selected_table = st.selectbox("テーブル", tables, key="sf_table")

                            if selected_table:
                                if st.button("📥 追加", key="add_sf"):
                                    with st.spinner("データ取得中..."):
                                        df = connector.get_sample_data(selected_db, selected_table, selected_schema)

                                        source_name = st.session_state.get("sf_name", f"Snowflake_{st.session_state.source_counter}")
                                        st.session_state.source_counter += 1

                                        # データソースを追加
                                        st.session_state.data_sources[source_name] = {
                                            "type": "snowflake",
                                            "df": df,
                                            "connector": connector,
                                            "database": selected_db,
                                            "schema": selected_schema,
                                            "table": selected_table
                                        }
                                        st.session_state.active_source = source_name
                                        st.session_state.messages[source_name] = []
                                        st.session_state.temp_sf_connector = None
                                        st.success(f"✅ {source_name}を追加しました！")
                                        st.rerun()
            except Exception as e:
                st.error(f"エラー: {e}")
    
    elif source == "Databricks🧱" and USE_NEW_CONNECTORS:
        with st.expander("接続設定", expanded=True):
            source_name = st.text_input("データソース名", placeholder="例: Databricks分析環境", key="db_name")
            server_hostname = st.text_input("サーバーホスト", placeholder="xxx.cloud.databricks.com", key="db_host")
            http_path = st.text_input("HTTPパス", placeholder="/sql/1.0/endpoints/xxx", key="db_path")
            access_token = st.text_input("Access Token", type="password", help="Personal Access Token", key="db_token")
            catalog = st.text_input("カタログ（任意）", key="db_catalog")

            # 一時的な接続状態
            if 'temp_db_connector' not in st.session_state:
                st.session_state.temp_db_connector = None

            if all([server_hostname, http_path, access_token, source_name]):
                if st.button("🔗 Databricksに接続", key="db_connect"):
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
                            st.session_state.temp_db_connector = connector
                            st.success("✅ 接続成功！カタログとテーブルを選択してください")
                            st.rerun()
                    except Exception as e:
                        st.error(f"接続エラー: {e}")

        # 接続後のデータ選択
        if st.session_state.temp_db_connector:
            try:
                connector = st.session_state.temp_db_connector

                catalogs = connector.list_datasets()
                selected_catalog = st.selectbox("カタログ", catalogs, key="db_cat_select")

                if selected_catalog:
                    if type(connector).__name__ in ['SnowflakeConnector', 'DatabricksConnector']:
                        schemas = connector.list_schemas(selected_catalog)
                        selected_schema = st.selectbox("スキーマ", schemas, key="db_schema_select")

                        if selected_schema:
                            tables = connector.list_tables(selected_catalog, selected_schema)
                            selected_table = st.selectbox("テーブル", tables, key="db_table_select")

                            if selected_table:
                                if st.button("📥 追加", key="add_db"):
                                    with st.spinner("データ取得中..."):
                                        df = connector.get_sample_data(selected_catalog, selected_table, schema=selected_schema)

                                        source_name = st.session_state.get("db_name", f"Databricks_{st.session_state.source_counter}")
                                        st.session_state.source_counter += 1

                                        # データソースを追加
                                        st.session_state.data_sources[source_name] = {
                                            "type": "databricks",
                                            "df": df,
                                            "connector": connector,
                                            "catalog": selected_catalog,
                                            "schema": selected_schema,
                                            "table": selected_table
                                        }
                                        st.session_state.active_source = source_name
                                        st.session_state.messages[source_name] = []
                                        st.session_state.temp_db_connector = None
                                        st.success(f"✅ {source_name}を追加しました！")
                                        st.rerun()
            except Exception as e:
                st.error(f"エラー: {e}")
    
    elif source == "Googleスプレッドシート🟩" and USE_NEW_CONNECTORS:
        with st.expander("接続設定", expanded=True):
            source_name = st.text_input("データソース名", placeholder="例: 売上管理シート", key="gs_name")
            sa_file = st.file_uploader(
                "サービスアカウントJSON",
                type="json",
                key="gs_sa",
                help="Google SheetsAPIアクセス用のサービスアカウントJSONファイル"
            )
            sheet_url = st.text_input("スプレッドシートURL", placeholder="https://docs.google.com/spreadsheets/d/...", key="gs_url")

            # 一時的な接続状態
            if 'temp_gs_connector' not in st.session_state:
                st.session_state.temp_gs_connector = None

            if all([sa_file, sheet_url, source_name]):
                if st.button("🔗 Googleスプレッドシートに接続", key="gs_connect"):
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
                            st.session_state.temp_gs_connector = connector
                            st.success("✅ 接続成功！シートを選択してください")
                            st.rerun()
                    except Exception as e:
                        st.error(f"接続エラー: {e}")

        # 接続後のデータ選択
        if st.session_state.temp_gs_connector:
            try:
                connector = st.session_state.temp_gs_connector
                sheets = connector.list_tables("")  # Google Sheetsではdatasetパラメータ不要
                selected_sheet = st.selectbox("シート", sheets, key="gs_sheet_select")

                if selected_sheet:
                    if st.button("📥 追加", key="add_gs"):
                        with st.spinner("データ取得中..."):
                            df = connector.get_sample_data("", selected_sheet)

                            source_name = st.session_state.get("gs_name", f"GoogleSheets_{st.session_state.source_counter}")
                            st.session_state.source_counter += 1

                            # データソースを追加
                            st.session_state.data_sources[source_name] = {
                                "type": "google_sheets",
                                "df": df,
                                "connector": connector,
                                "sheet_name": selected_sheet
                            }
                            st.session_state.active_source = source_name
                            st.session_state.messages[source_name] = []
                            st.session_state.temp_gs_connector = None
                            st.success(f"✅ {source_name}を追加しました！")
                            st.rerun()
            except Exception as e:
                st.error(f"エラー: {e}")

# メインエリア
st.title("🧞 Vizzy - Adhoc Analytics Assistant")

# アクティブなデータソースがあるかチェック
if st.session_state.active_source and st.session_state.active_source in st.session_state.data_sources:
    active_data = st.session_state.data_sources[st.session_state.active_source]
    df = active_data['df']

    # 日付カラムの自動変換
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass

    # データソースの種類を判定
    connector = None
    duck_conn = None

    if active_data['connector']:
        connector = active_data['connector']
        dialect = connector.get_dialect() if hasattr(connector, 'get_dialect') else 'duckdb'
    else:
        # ローカルファイルの場合はDuckDBを使用
        dialect = 'duckdb'

    # DuckDBが必要な場合は常に初期化
    if dialect == 'duckdb':
        duck_conn = duckdb.connect()
        duck_conn.register("data", df)

    # カラム分割: 左にデータプレビュー、右にチャット
    col_left, col_right = st.columns([1, 2])

    with col_left:
        st.subheader("📊 データプレビュー")
        st.write(f"**{st.session_state.active_source}**")
        st.write(f"データサイズ: {len(df):,}行 × {len(df.columns)}列")
        st.dataframe(df.head(100), height=600)

    with col_right:
        st.subheader("💬 データ分析チャット")

        # 現在のデータソースのメッセージを初期化（必要に応じて）
        if st.session_state.active_source not in st.session_state.messages:
            st.session_state.messages[st.session_state.active_source] = []

        # チャット履歴を表示
        for idx, message in enumerate(st.session_state.messages[st.session_state.active_source]):
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
            st.session_state.messages[st.session_state.active_source].append({"role": "user", "content": prompt})

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
                # アクティブデータソースからテーブル情報を取得
                if 'database' in active_data and 'schema' in active_data and 'table' in active_data:
                    table_ref = f"{active_data['database']}.{active_data['schema']}.{active_data['table']}"
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
- **カラム名が小文字の場合は必ずダブルクォートで囲むこと** (例: "name", "user_id")
- 大文字のカラム名はダブルクォート不要 (例: NAME, USER_ID)
- エイリアス（AS句）も小文字の場合はダブルクォートで囲むこと
- 日付関数: DATE_TRUNC(), DATEADD(), DATEDIFF()など
- 文字列関数: CONCAT(), SPLIT_PART(), REGEXP_SUBSTR()など
- グラフを要求された場合は、適切なGROUP BYとORDER BYを含める
- SQLクエリのみを返す（説明は不要）
"""
            elif dialect == 'bigquery':
                # BigQueryの場合もテーブル情報を取得
                if 'dataset' in active_data and 'table' in active_data:
                    # BigQueryのconnectorからproject_idを取得
                    if connector and hasattr(connector, 'connection'):
                        project_id = connector.connection.project
                        table_ref = f"`{project_id}.{active_data['dataset']}.{active_data['table']}`"
                    else:
                        table_ref = f"{active_data['dataset']}.{active_data['table']}"
                else:
                    table_ref = "data"

                sql_generation_prompt = f"""
以下のテーブル情報を基に、ユーザーの質問に答えるBigQuery SQLクエリを生成してください。

テーブル名: {table_ref}
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
                # アクティブデータソースからテーブル情報を取得
                if 'catalog' in active_data and 'schema' in active_data and 'table' in active_data:
                    table_ref = f"{active_data['catalog']}.{active_data['schema']}.{active_data['table']}"
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
                            model="gpt-5-nano",
                            messages=[
                                {"role": "system", "content": "あなたはSQL生成の専門家です。"},
                                {"role": "user", "content": sql_generation_prompt}
                            ]
                        )

                    sql_query = response.choices[0].message.content.strip()
                    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

                    with st.expander("生成されたSQL", expanded=False):
                        st.code(sql_query, language="sql")

                    # SQLバリデーション
                    is_safe, error_message = is_safe_query(sql_query)
                    if not is_safe:
                        st.error(f"🚫 セキュリティエラー: {error_message}")
                        st.warning("このアプリケーションはSELECT文のみ実行可能です。データの変更・削除を行うSQL操作は許可されていません。")
                        st.session_state.messages[st.session_state.active_source].append({
                            "role": "assistant",
                            "content": f"申し訳ございません。生成されたSQLが安全性チェックに失敗しました。\n\nエラー: {error_message}\n\nこのアプリケーションはSELECT文のみ実行可能です。",
                            "sql": sql_query,
                            "error": error_message
                        })
                        st.stop()

                    # クエリ実行
                    try:
                        with st.spinner("クエリ実行中..."):
                            if dialect in ['snowflake', 'bigquery', 'databricks'] and connector and hasattr(connector, 'execute_query'):
                                result_df = connector.execute_query(sql_query)
                            elif duck_conn is not None:
                                result_df = duck_conn.execute(sql_query).fetchdf()
                            else:
                                raise RuntimeError(f"データソース'{active_data['type']}'でのクエリ実行に失敗しました。DuckDB接続が初期化されていません。")

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
                                    model="gpt-5-nano",
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
                        st.session_state.messages[st.session_state.active_source].append(assistant_message)

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

#### データソースの追加
1. **データソース種類を選択**: サイドバーの「新しいデータソースを追加」から選択
2. **データソース名を入力**: わかりやすい名前をつける（例: "売上データ2024"）
3. **接続設定**: 必要な認証情報を入力して接続
4. **テーブル選択**: データセット/テーブルを選択
5. **追加**: 「📥 追加」ボタンでデータソースを追加

#### 分析の実行
1. **データソース切り替え**: サイドバーのドロップダウンから選択
2. **データプレビュー**: 左側でデータを確認
3. **チャットで質問**: 右側のチャット欄に自然言語で入力
4. **結果確認**: SQL、グラフ、分析要約が自動生成
5. **レポート保存**: HTMLレポートやCSVをダウンロード

### 📊 対応データソース

- **ローカルファイル**: CSV, Parquet
- **BigQuery**: Google Cloud BigQuery
- **Snowflake**: Programmatic Access Token認証
- **Databricks**: Personal Access Token認証
- **Google Sheets**: サービスアカウント認証

### ✨ 複数データソース機能

- 複数のデータソースを同時に接続可能
- サイドバーで簡単に切り替え
- 各データソースごとに独立したチャット履歴
- 不要なデータソースは🗑️ボタンで削除

### 💡 質問例

- 「月別の売上推移を見せて」
- 「カテゴリ別の売上を棒グラフで表示」
- 「上位10商品の売上割合を円グラフで」
- 「昨年同月比の成長率を計算して」
        """)
