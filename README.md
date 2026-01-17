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

- **複数データソース同時接続**: 複数のデータソースを同時に接続し、切り替えながら分析可能
- **Text2SQL**: 自然言語をSELECT文に自動変換
- **チャット形式の分析UI**: データソースごとに独立したチャット履歴で対話的に分析
- **自動ビジュアライゼーション**: クエリ結果から最適なグラフを自動生成
- **マルチデータソース対応**: 5つの主要データソースに対応（読み取り専用）
- **アドホック分析**: その場で思いついた質問を即座にSQL化して実行
- **安全な探索**: SELECT文のみ実行可能で、データの変更リスクなし

## アーキテクチャ

クリーンアーキテクチャを採用し、拡張性と保守性を確保しています。

### ディレクトリ構造

```
vizzy-adhoc-analytics/
├── app.py                    # メインのStreamlitアプリケーション
├── requirements.txt          # 依存パッケージ
├── architecture/             # アーキテクチャドキュメント
│   ├── data_flow.md         # データフロー図
│   └── system_architecture.md # システムアーキテクチャ図
└── src/
    ├── domain/              # ドメイン層（インターフェース定義）
    └── infrastructure/      # インフラストラクチャ層（実装）
        └── connectors/      # データソースコネクタ
```

## クイックスタート

### 必要条件
- Python 3.8以上
- OpenAI APIキー

### インストール

```bash
# リポジトリのクローン
git clone https://github.com/RyutoYoda/vizzy-adhoc-analytics.git
cd vizzy-adhoc-analytics

# 依存関係のインストール
pip install -r requirements.txt
```

### 実行

```bash
streamlit run app.py
```

アプリ起動後、画面内の「使い方」セクションで詳しい使い方を確認できます。

## アーキテクチャ詳細

詳細なアーキテクチャ図は以下を参照してください：
- [データフロー図](architecture/data_flow.md)
- [システムアーキテクチャ図](architecture/system_architecture.md)

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

## コントリビューション

プルリクエストを歓迎します。新機能の提案やバグ報告は[Issues](https://github.com/RyutoYoda/vizzy-adhoc-analytics/issues)へ。
