from typing import Dict, List, Any, Optional
import pandas as pd
from databricks import sql
from src.infrastructure.connectors.base import BaseConnector


class DatabricksConnector(BaseConnector):
    """Databricksコネクタの実装"""
    
    def connect(self, credentials: Dict[str, Any]) -> None:
        """Databricksに接続
        
        Args:
            credentials: {
                "server_hostname": "xxx.cloud.databricks.com",
                "http_path": "/sql/1.0/endpoints/xxx",
                "access_token": "dapi...", # Personal Access Token (PAT)
                "catalog": Optional[str],
                "schema": Optional[str]
            }
        """
        self.connection = sql.connect(
            server_hostname=credentials['server_hostname'],
            http_path=credentials['http_path'],
            access_token=credentials['access_token']
        )
        self.cursor = self.connection.cursor()
        
        # デフォルトカタログ・スキーマの設定
        if credentials.get('catalog'):
            self.cursor.execute(f"USE CATALOG {credentials['catalog']}")
        if credentials.get('schema'):
            self.cursor.execute(f"USE SCHEMA {credentials['schema']}")
        
        self.is_connected = True
    
    def list_datasets(self) -> List[str]:
        """利用可能なカタログ（またはスキーマ）のリストを取得"""
        self._ensure_connected()
        self.cursor.execute("SHOW CATALOGS")
        catalogs = self.cursor.fetchall()
        return [catalog[0] for catalog in catalogs]
    
    def list_tables(self, dataset: str) -> List[str]:
        """指定カタログ内のテーブルリストを取得"""
        self._ensure_connected()
        self.cursor.execute(f"USE CATALOG {dataset}")
        self.cursor.execute("SHOW TABLES")
        tables = self.cursor.fetchall()
        return [table[1] for table in tables]  # table_name列を取得
    
    def get_sample_data(self, dataset: str, table: str, limit: int = 1000) -> pd.DataFrame:
        """サンプルデータを取得"""
        self._ensure_connected()
        query = f"SELECT * FROM {dataset}.{table} LIMIT {limit}"
        self.cursor.execute(query)
        
        # カラム名を取得
        columns = [desc[0] for desc in self.cursor.description]
        data = self.cursor.fetchall()
        
        return pd.DataFrame(data, columns=columns)
    
    def get_table_schema(self, dataset: str, table: str) -> Dict[str, str]:
        """テーブルスキーマを取得"""
        self._ensure_connected()
        self.cursor.execute(f"DESCRIBE TABLE {dataset}.{table}")
        schema_info = self.cursor.fetchall()
        
        schema = {}
        for row in schema_info:
            column_name = row[0]
            data_type = row[1]
            schema[column_name] = data_type
        
        return schema
    
    def close(self) -> None:
        """接続を閉じる"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.is_connected = False