# Vizzy システムアーキテクチャ

## 概要
Vizzyは、レイヤードアーキテクチャ（DDD風）を採用したStreamlitベースのWebアプリケーションです。

## システム全体図

```mermaid
graph TB
    subgraph "フロントエンド"
        UI[Streamlit UI<br/>app.py]
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

    style NLQ fill:#e1f5fe
    style OpenAI fill:#fff3e0
    style SQL fill:#f3e5f5
    style Router fill:#fce4ec
    style Result fill:#e8f5e9
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

## 主要な特徴

1. **Text2SQL統合アーキテクチャ**: 
   - 自然言語→OpenAI API→SQL生成→実行の一貫したフロー
   - データソースに応じた最適なSQL方言の生成

2. **マルチデータソース対応**: 
   - BigQuery、Snowflake、Databricks、Google Sheets、ローカルファイル
   - 統一インターフェースで操作方法を覚え直す必要なし

3. **アダプティブ実行エンジン**: 
   - ローカルデータ: DuckDB (インメモリ高速処理)
   - リモートDB: 各コネクタ経由でサーバーサイド実行
   - データの所在に応じた最適な実行戦略

4. **読み取り専用安全設計**: 
   - SELECT文のみ実行可能
   - データの誤変更・削除リスクを完全排除

5. **インテリジェント可視化**:
   - AIがデータとクエリ結果から最適なグラフタイプを推定
   - Plotlyによるインタラクティブな可視化