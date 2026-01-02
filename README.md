# Vizzy - Adhoc Analytics Assistant
読み取り専用権限でデータベースに接続し、自然言語でアドホック分析を行えるAIアシスタントツールです。SELECT文のみ実行可能で、安全にデータ探索ができます。
## 使用技術

<p align="left">
  <a href="https://www.python.org/">
    <img src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white" />
  </a>
  <a href="https://streamlit.io/">
    <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=flat&logo=streamlit&logoColor=white" />
  </a>
  <a href="https://openai.com/">
    <img src="https://img.shields.io/badge/OpenAI-412991?style=flat&logo=openai&logoColor=white" />
  </a>
  <a href="https://duckdb.org/">
    <img src="https://img.shields.io/badge/DuckDB-FFF000?style=flat&logo=duckdb&logoColor=black" />
  </a>
  <a href="https://pandas.pydata.org/">
    <img src="https://img.shields.io/badge/pandas-150458?style=flat&logo=pandas&logoColor=white" />
  </a>
  <a href="https://plotly.com/">
    <img src="https://img.shields.io/badge/Plotly-3F4F75?style=flat&logo=plotly&logoColor=white" />
  </a>
</p>

## 対応データソース

<p align="left">
  <a href="https://cloud.google.com/bigquery">
    <img src="https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=google-cloud&logoColor=white" />
  </a>
  <a href="https://www.snowflake.com/">
    <img src="https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white" />
  </a>
  <a href="https://www.databricks.com/">
    <img src="https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white" />
  </a>
  <a href="https://www.google.com/sheets/about/">
    <img src="https://img.shields.io/badge/Google%20Sheets-34A853?style=flat&logo=google-sheets&logoColor=white" />
  </a>
</p>

## 主な機能

- **Text2SQL**: 自然言語をSELECT文に自動変換
- **自動ビジュアライゼーション**: クエリ結果から最適なグラフを自動生成
- **マルチデータソース対応**: 5つの主要データソースに対応（読み取り専用）
- **アドホック分析**: その場で思いついた質問を即座にSQL化して実行
- **安全な探索**: SELECT文のみ実行可能で、データの変更リスクなし

## 使用技術

### AIモデル
- **SQL生成**: OpenAI GPT-3.5-turbo
- **テキスト埋め込み**: text-embedding-ada-002
- **クラスタリング**: FAISS + scikit-learn K-means

### フレームワーク・ライブラリ
- **UI**: Streamlit
- **データ処理**: pandas, DuckDB
- **可視化**: Plotly
- **ベクトル検索**: FAISS

## 対応データソース

### クラウドデータウェアハウス
- **[BigQuery](src/infrastructure/connectors/bigquery.py)**: Google Cloud BigQuery
  - サービスアカウントJSON認証
- **[Snowflake](src/infrastructure/connectors/snowflake.py)**: Snowflake Data Warehouse
  - Programmatic Access Token (秘密鍵) 認証
- **[Databricks](src/infrastructure/connectors/databricks.py)**: Databricks SQL Warehouse
  - Personal Access Token (PAT) 認証

### その他のデータソース
- **[ローカルファイル](src/infrastructure/connectors/local_file.py)**: CSV, Parquet
- **[Google Sheets](src/infrastructure/connectors/google_sheets.py)**: スプレッドシート
  - サービスアカウントJSON認証

## アーキテクチャ

クリーンアーキテクチャを採用し、拡張性と保守性を確保しています。

### ディレクトリ構造

```
simple_text2sql_v02/
├── app.py                    # メインのStreamlitアプリケーション
├── requirements.txt          # 依存パッケージ
├── README.md                # このファイル
└── src/
    ├── domain/              # ドメイン層
    │   └── interfaces.py    # DataSourceConnector インターフェース
    └── infrastructure/      # インフラストラクチャ層
        └── connectors/      # データベースコネクタ実装
            ├── base.py      # 基底コネクタクラス
            ├── factory.py   # コネクタファクトリー
            ├── bigquery.py
            ├── snowflake.py
            ├── databricks.py
            ├── local_file.py
            └── google_sheets.py
```

### 主要コンポーネント

- **[DataSourceConnector インターフェース](src/domain/interfaces.py)**: すべてのコネクタが実装する共通インターフェース
- **[BaseConnector](src/infrastructure/connectors/base.py)**: コネクタの基底実装
- **[ConnectorFactory](src/infrastructure/connectors/factory.py)**: コネクタのファクトリーパターン実装

## クイックスタート

### 必要条件
- Python 3.8以上
- OpenAI APIキー

### インストール

```bash
# リポジトリのクローン
git clone https://github.com/RyutoYoda/simple_text2sql_v02.git
cd simple_text2sql_v02

# 依存関係のインストール
pip install -r requirements.txt
```

### 実行

```bash
streamlit run app.py
```

## 使い方

1. **データソース選択**: サイドバーから使用するデータソースを選択
2. **認証情報入力**: 各データソースに必要な認証情報を入力
3. **データ取得**: テーブルを選択してデータを読み込み
4. **自然言語クエリ**: 質問を入力して分析を実行

### 質問例

- 月別の売上推移を見せて
- カテゴリ別の売上を棒グラフで表示
- 上位10商品の売上割合を円グラフで
- 昨年同月比の成長率を計算して

## 各コネクタの設定方法

### Snowflake
1. Snowflakeアカウントで秘密鍵ペアを生成
2. 公開鍵をユーザープロファイルに登録
3. 秘密鍵（PEMファイル）をアップロード

### Databricks
1. Databricksワークスペースでアクセストークンを生成
2. SQLウェアハウスのエンドポイント情報を取得
3. トークンとエンドポイント情報を入力

### BigQuery
1. GCPコンソールでサービスアカウントを作成
2. BigQuery権限を付与
3. JSONキーをダウンロードしてアップロード

## 開発者向け情報

### 新しいコネクタの追加方法

1. `src/infrastructure/connectors/`に新しいコネクタクラスを作成
2. `DataSourceConnector`インターフェースを実装
3. `factory.py`にコネクタを登録

```python
# 例: 新しいコネクタの実装
from src.infrastructure.connectors.base import BaseConnector

class MyNewConnector(BaseConnector):
    def connect(self, credentials: Dict[str, Any]) -> None:
        # 接続ロジックを実装
        pass
    
    # その他の必要なメソッドを実装
```

### コネクタインターフェース

すべてのコネクタは以下のメソッドを実装する必要があります：

- `connect(credentials: Dict[str, Any]) -> None`: データソースへの接続
- `list_datasets() -> List[str]`: データセット/スキーマ一覧の取得
- `list_tables(dataset: str) -> List[str]`: テーブル一覧の取得
- `get_sample_data(dataset: str, table: str, limit: int) -> pd.DataFrame`: サンプルデータの取得
- `get_table_schema(dataset: str, table: str) -> Dict[str, str]`: テーブルスキーマの取得
- `close() -> None`: 接続のクローズ

## 主要な依存関係

```
streamlit==1.29.0
pandas==2.0.3
plotly==5.18.0
duckdb==0.9.2
openai>=1.0.0
numpy==1.24.3
faiss-cpu==1.7.4
scikit-learn==1.3.2
google-cloud-bigquery==3.13.0
snowflake-connector-python==3.5.0
databricks-sql-connector==3.0.1
gspread==5.12.0
```

## トラブルシューティング

### 接続エラーが発生する場合
- 認証情報が正しいか確認
- ネットワーク接続を確認
- 必要な権限が付与されているか確認

### OpenAI APIエラー
- APIキーが有効か確認
- APIの利用制限に達していないか確認

## コントリビューション

プルリクエストを歓迎します。新機能の提案やバグ報告は[Issues](https://github.com/RyutoYoda/simple_text2sql_v02/issues)へ。
