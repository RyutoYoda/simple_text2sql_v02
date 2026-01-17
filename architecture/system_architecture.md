# Vizzy システムアーキテクチャ

## 概要
Vizzyは、レイヤードアーキテクチャ（DDD風）を採用したStreamlitベースのWebアプリケーションです。

## システム全体図

```mermaid
graph TB
    subgraph "フロントエンド"
        UI[Streamlit UI<br/>app.py]
        Session[セッション管理<br/>st.session_state]
    end

    subgraph "複数データソース管理"
        Session --> DataSources[data_sources dict<br/>複数データソース保持]
        Session --> ActiveSource[active_source<br/>現在選択中のソース]
        Session --> Messages[messages dict<br/>ソースごとのチャット履歴]
    end

    subgraph "Text2SQL処理フロー"
        UI --> NLQ[自然言語クエリ]
        NLQ --> OpenAI[OpenAI API<br/>GPT-3.5-turbo]
        OpenAI --> SQL[SQL生成]
        SQL --> Router{データソース判定}
    end

    subgraph "クエリ実行エンジン"
        Router -->|ローカルデータ| DuckDB[DuckDB<br/>インメモリ実行]
        Router -->|リモートDB| Connectors[コネクタ経由実行]
    end

    subgraph "インフラストラクチャ層"
        DuckDB --> LocalFile[LocalFileConnector]
        Connectors --> BigQuery[BigQueryConnector]
        Connectors --> Sheets[GoogleSheetsConnector]
        Connectors --> Snowflake[SnowflakeConnector]
        Connectors --> Databricks[DatabricksConnector]
    end

    subgraph "外部データソース"
        LocalFile --> Files[ローカルファイル<br/>CSV/Parquet]
        BigQuery --> GCP[Google Cloud Platform<br/>BigQuery]
        Sheets --> GoogleAPI[Google Sheets API]
        Snowflake --> SnowflakeDB[(Snowflake<br/>Data Warehouse)]
        Databricks --> DatabricksDB[(Databricks<br/>Lakehouse)]
    end

    subgraph "結果処理"
        DuckDB --> Result[実行結果<br/>DataFrame]
        Connectors --> Result
        Result --> Viz[可視化<br/>Plotly]
        Viz --> UI
    end

    UI <--> Session
    DataSources --> Router

    style NLQ fill:#e1f5fe
    style OpenAI fill:#fff3e0
    style SQL fill:#f3e5f5
    style Router fill:#fce4ec
    style Result fill:#e8f5e9
    style Session fill:#fff9c4
    style DataSources fill:#f0f4c3
```

## レイヤー構成

```mermaid
graph LR
    subgraph "プレゼンテーション層"
        A[app.py<br/>メインUI]
        B[Text2SQL<br/>自然言語処理]
        C[Visualization<br/>可視化]
    end

    subgraph "アプリケーション層"
        D[Query Router<br/>クエリルーティング]
        E[Schema Analyzer<br/>スキーマ解析]
    end

    subgraph "ドメイン層"
        F[interfaces.py<br/>抽象インターフェース]
    end

    subgraph "インフラストラクチャ層"
        G[base.py<br/>基底クラス]
        H[factory.py<br/>ファクトリ]
        I[各種コネクタ<br/>実装クラス]
        J[DuckDB<br/>ローカル実行エンジン]
    end

    subgraph "外部サービス層"
        K[OpenAI API<br/>LLM]
        L[データソース<br/>BigQuery/Snowflake等]
    end

    A --> B
    B --> K
    A --> D
    D --> E
    E --> F
    D --> J
    F --> G
    G --> I
    H --> I
    I --> L
    A --> C
```

## コネクタクラス図

```mermaid
classDiagram
    class DataSourceConnector {
        <<interface>>
        +connect(credentials: Dict)
        +list_datasets() List~str~
        +list_tables(dataset: str) List~str~
        +get_sample_data(dataset: str, table: str) DataFrame
        +get_table_schema(dataset: str, table: str) Dict
        +execute_query(query: str) DataFrame
        +get_dialect() str
        +close()
    }

    class BaseConnector {
        <<abstract>>
        -connection: Any
        -is_connected: bool
        +__init__()
        +_ensure_connected()
    }

    class SnowflakeConnector {
        -cursor: Any
        +connect(credentials: Dict)
        +list_schemas(database: str) List~str~
        +list_tables(dataset: str, schema: str) List~str~
        +get_sample_data(dataset: str, table: str, schema: str) DataFrame
        +execute_query(query: str) DataFrame
        +get_dialect() str
    }

    class BigQueryConnector {
        +connect(credentials: Dict)
        +list_datasets() List~str~
        +list_tables(dataset: str) List~str~
        +get_sample_data(dataset: str, table: str) DataFrame
        +execute_query(query: str) DataFrame
    }

    class LocalFileConnector {
        -df: DataFrame
        +connect(file_path: str, file_type: str)
        +list_datasets() List~str~
        +list_tables(dataset: str) List~str~
        +get_sample_data(dataset: str, table: str) DataFrame
    }

    DataSourceConnector <|-- BaseConnector
    BaseConnector <|-- SnowflakeConnector
    BaseConnector <|-- BigQueryConnector
    BaseConnector <|-- LocalFileConnector
    BaseConnector <|-- GoogleSheetsConnector
    BaseConnector <|-- DatabricksConnector

    class ConnectorFactory {
        <<static>>
        +create_connector(source_type: str) BaseConnector
    }

    ConnectorFactory ..> BaseConnector : creates
```

## セッション状態管理

複数データソースの同時接続を実現するため、以下のセッション状態を管理しています。

```mermaid
stateDiagram-v2
    [*] --> NoSource: アプリ起動
    NoSource --> SingleSource: 最初のデータソース追加
    SingleSource --> MultiSource: 2つ目以降のデータソース追加
    MultiSource --> MultiSource: データソース追加/削除
    MultiSource --> SingleSource: 1つまで削除
    SingleSource --> NoSource: 全て削除

    state SingleSource {
        [*] --> Active: data_sources[name]に保存
        Active --> Analyzing: チャット質問
        Analyzing --> Active: 結果表示
    }

    state MultiSource {
        [*] --> SourceA
        SourceA --> SourceB: データソース切り替え
        SourceB --> SourceC: データソース切り替え
        SourceC --> SourceA: データソース切り替え

        state SourceA {
            [*] --> ActiveA: active_source = A
            ActiveA --> AnalyzingA: チャット質問
            AnalyzingA --> ActiveA: messages[A]に保存
        }

        state SourceB {
            [*] --> ActiveB: active_source = B
            ActiveB --> AnalyzingB: チャット質問
            AnalyzingB --> ActiveB: messages[B]に保存
        }

        state SourceC {
            [*] --> ActiveC: active_source = C
            ActiveC --> AnalyzingC: チャット質問
            AnalyzingC --> ActiveC: messages[C]に保存
        }
    }
```

### データ構造

```python
# セッション状態の構造
st.session_state = {
    # 複数データソースを名前をキーとして管理
    'data_sources': {
        '売上データ2024': {
            'type': 'snowflake',
            'df': DataFrame,
            'connector': SnowflakeConnector,
            'database': 'PROD_DB',
            'schema': 'SALES',
            'table': 'ORDERS'
        },
        'マーケティングデータ': {
            'type': 'bigquery',
            'df': DataFrame,
            'connector': BigQueryConnector,
            'project': 'my-project',
            'dataset': 'marketing',
            'table': 'campaigns'
        },
        'ローカル分析用CSV': {
            'type': 'local',
            'df': DataFrame,
            'connector': None,
            'file_name': 'analysis.csv'
        }
    },

    # 現在表示中のデータソース名
    'active_source': '売上データ2024',

    # データソースごとの独立したチャット履歴
    'messages': {
        '売上データ2024': [
            {'role': 'user', 'content': '月別の売上を見せて'},
            {'role': 'assistant', 'content': '...', 'sql': '...', 'result_df': ...}
        ],
        'マーケティングデータ': [
            {'role': 'user', 'content': 'キャンペーン効果を分析'},
            {'role': 'assistant', 'content': '...', 'sql': '...', 'result_df': ...}
        ],
        'ローカル分析用CSV': []
    }
}
```

## 主要な特徴

1. **複数データソース同時接続**:
   - 複数のデータソースを同時に接続・管理
   - シームレスなデータソース切り替え
   - データソースごとに独立したチャット履歴
   - 直感的なUI（サイドバー）でデータソースを管理

2. **Text2SQL統合アーキテクチャ**:
   - 自然言語→OpenAI API→SQL生成→実行の一貫したフロー
   - データソースに応じた最適なSQL方言の生成（Snowflake、BigQuery、Databricks、DuckDB）
   - カラム名の大文字・小文字やクォーテーションルールを自動判定

3. **マルチデータソース対応**:
   - BigQuery、Snowflake、Databricks、Google Sheets、ローカルファイル
   - 統一インターフェースで操作方法を覚え直す必要なし
   - データソースごとに最適化されたSQL生成

4. **アダプティブ実行エンジン**:
   - ローカルデータ: DuckDB (インメモリ高速処理)
   - リモートDB: 各コネクタ経由でサーバーサイド実行
   - データの所在に応じた最適な実行戦略

5. **読み取り専用安全設計**:
   - SELECT文のみ実行可能
   - データの誤変更・削除リスクを完全排除

6. **インテリジェント可視化**:
   - AIがデータとクエリ結果から最適なグラフタイプを推定
   - Plotlyによるインタラクティブな可視化

7. **独立したコンテキスト管理**:
   - データソースごとに独立したチャット履歴
   - データソースを切り替えても過去の分析履歴を維持
   - 複数のデータソースで並行して分析作業が可能