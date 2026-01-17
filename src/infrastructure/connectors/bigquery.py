from typing import Dict, List, Any, Optional
import pandas as pd
from google.cloud import bigquery
from src.infrastructure.connectors.base import BaseConnector


class BigQueryConnector(BaseConnector):
    """BigQueryコネクタの実装"""
    
    def connect(self, credentials: Dict[str, Any]) -> None:
        """BigQueryに接続
        
        Args:
            credentials: {
                "credentials_path": "path/to/service_account.json",
                "project_id": Optional[str]  # 省略時はJSONファイルから取得
            }
        """
        credentials_path = credentials.get("credentials_path")
        project_id = credentials.get("project_id")
        
        if credentials_path:
            self.connection = bigquery.Client.from_service_account_json(
                credentials_path,
                project=project_id
            )
        else:
            # デフォルト認証を使用
            self.connection = bigquery.Client(project=project_id)
        
        self.is_connected = True
    
    def list_datasets(self) -> List[str]:
        """利用可能なデータセットのリストを取得"""
        self._ensure_connected()
        datasets = list(self.connection.list_datasets())
        return [dataset.dataset_id for dataset in datasets]
    
    def list_tables(self, dataset: str) -> List[str]:
        """指定データセット内のテーブルリストを取得"""
        self._ensure_connected()
        tables = list(self.connection.list_tables(dataset))
        return [table.table_id for table in tables]
    
    def get_sample_data(self, dataset: str, table: str, limit: int = 1000) -> pd.DataFrame:
        """サンプルデータを取得"""
        self._ensure_connected()
        full_table_id = f"{self.connection.project}.{dataset}.{table}"
        query = f"SELECT * FROM `{full_table_id}` LIMIT {limit}"
        return self.connection.query(query).to_dataframe()
    
    def get_table_schema(self, dataset: str, table: str) -> Dict[str, str]:
        """テーブルスキーマを取得"""
        self._ensure_connected()
        table_ref = self.connection.dataset(dataset).table(table)
        table = self.connection.get_table(table_ref)

        schema = {}
        for field in table.schema:
            schema[field.name] = field.field_type

        return schema

    def execute_query(self, query: str) -> pd.DataFrame:
        """SQLクエリを実行"""
        self._ensure_connected()
        return self.connection.query(query).to_dataframe()

    def get_dialect(self) -> str:
        """SQLダイアレクトを返す"""
        return "bigquery"