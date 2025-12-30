# snow_text2sql

## ディレクトリ構造

```
simple_text2sql_v02/
├── app.py                    # メインのStreamlitアプリケーション
├── requirements.txt          # 依存パッケージ
├── vizzy_logo.png           # アプリケーションロゴ
├── README.md                # このファイル
└── src/
    ├── __init__.py
    ├── domain/              # ドメイン層
    │   ├── __init__.py
    │   ├── models.py        # DataSource, Query等のドメインモデル
    │   └── interfaces.py    # 抽象インターフェース定義
    ├── infrastructure/      # インフラストラクチャ層
    │   ├── __init__.py
    │   ├── connectors/      # データベースコネクタ実装
    │   │   ├── __init__.py
    │   │   ├── base.py     # 基底コネクタクラス
    │   │   ├── bigquery.py # BigQueryコネクタ
    │   │   ├── snowflake.py # Snowflakeコネクタ (Programmatic Access Token認証)
    │   │   ├── databricks.py # Databricksコネクタ (Personal Access Token認証)
    │   │   ├── local_file.py # ローカルファイルコネクタ
    │   │   └── google_sheets.py # Google Sheetsコネクタ
    │   └── sql_engine.py    # DuckDB処理エンジン
    ├── application/         # アプリケーション層
    │   ├── __init__.py
    │   ├── text2sql.py     # Text2SQL処理ロジック
    │   └── clustering.py   # テキストクラスタリング処理
    └── presentation/        # プレゼンテーション層
        ├── __init__.py
        ├── components/      # UIコンポーネント
        │   ├── __init__.py
        │   ├── data_source_selector.py # データソース選択UI
        │   └── chart_builder.py # チャート生成コンポーネント
        └── pages/          # Streamlitページ
            ├── __init__.py
            └── main.py     # メインページ
```

## 対応データソース

- **BigQuery**: Google Cloud BigQuery
- **Snowflake**: Snowflake Data Warehouse (Programmatic Access Token認証)
- **Databricks**: Databricks SQL Warehouse (Personal Access Token認証)
- **ローカルファイル**: CSV, Parquet
- **Google Sheets**: Google スプレッドシート