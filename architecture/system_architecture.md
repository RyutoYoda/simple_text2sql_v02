# Vizzy システムアーキテクチャ

## 概要
Vizzyは、レイヤードアーキテクチャ（DDD風）を採用したStreamlitベースのWebアプリケーションです。

## システム全体図

```mermaid
graph TB
    subgraph "フロントエンド"
        UI[Streamlit UI<br/>app.py]
    end

    subgraph "アプリケーション層"
        UI --> Presentation[Presentation Layer<br/>components/pages]
    end

    subgraph "ドメイン層"
        Presentation --> Domain[Domain Layer<br/>interfaces.py]
    end

    subgraph "インフラストラクチャ層"
        Domain --> Infra[Infrastructure Layer<br/>connectors/]
        
        subgraph "コネクタ実装"
            Infra --> LocalFile[LocalFileConnector]
            Infra --> BigQuery[BigQueryConnector]
            Infra --> Sheets[GoogleSheetsConnector]
            Infra --> Snowflake[SnowflakeConnector]
            Infra --> Databricks[DatabricksConnector]
        end
    end

    subgraph "外部サービス"
        LocalFile --> Files[ローカルファイル<br/>CSV/Parquet]
        BigQuery --> GCP[Google Cloud Platform<br/>BigQuery]
        Sheets --> GoogleAPI[Google Sheets API]
        Snowflake --> SnowflakeDB[(Snowflake<br/>Data Warehouse)]
        Databricks --> DatabricksDB[(Databricks<br/>Lakehouse)]
    end

    subgraph "AI/ML サービス"
        UI --> OpenAI[OpenAI API<br/>GPT-3.5-turbo]
        OpenAI --> SQL[SQL生成]
    end

    subgraph "ローカル処理"
        UI --> DuckDB[DuckDB<br/>インメモリDB]
        DuckDB --> Query[クエリ実行<br/>※ローカルデータのみ]
    end
```

## レイヤー構成

```mermaid
graph LR
    subgraph "プレゼンテーション層"
        A[app.py<br/>メインUI]
        B[components/<br/>UIコンポーネント]
        C[pages/<br/>ページ]
    end

    subgraph "ドメイン層"
        D[interfaces.py<br/>抽象インターフェース]
    end

    subgraph "インフラストラクチャ層"
        E[base.py<br/>基底クラス]
        F[factory.py<br/>ファクトリ]
        G[各種コネクタ<br/>実装クラス]
    end

    A --> D
    B --> D
    C --> D
    D --> E
    E --> G
    F --> G
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

1. **読み取り専用設計**: SELECT文のみ実行可能で、データの変更リスクなし
2. **マルチデータソース対応**: 統一インターフェースで複数のデータソースに対応
3. **AI駆動**: OpenAI APIを使用した自然言語→SQL変換
4. **アダプティブ実行**: データソースに応じて最適な実行方法を選択
   - ローカルデータ: DuckDB (インメモリ処理)
   - リモートDB: 直接SQL実行 (サーバーサイド処理)