from typing import Dict, List, Any, Optional
import pandas as pd
from src.domain.interfaces import DataSourceConnector


class BaseConnector(DataSourceConnector):
    """コネクタの基底実装クラス"""
    
    def __init__(self):
        self.connection = None
        self.is_connected = False
    
    def connect(self, credentials: Dict[str, Any]) -> None:
        """継承先で実装"""
        raise NotImplementedError
    
    def list_datasets(self) -> List[str]:
        """継承先で実装"""
        raise NotImplementedError
    
    def list_tables(self, dataset: str) -> List[str]:
        """継承先で実装"""
        raise NotImplementedError
    
    def get_sample_data(self, dataset: str, table: str, schema: str = None, limit: int = 1000) -> pd.DataFrame:
        """継承先で実装"""
        raise NotImplementedError
    
    def get_table_schema(self, dataset: str, table: str, schema: str = None) -> Dict[str, str]:
        """継承先で実装"""
        raise NotImplementedError
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """SQLクエリを実行
        
        Args:
            query: 実行するSQLクエリ
            
        Returns:
            クエリ結果のDataFrame
        """
        raise NotImplementedError
    
    def get_dialect(self) -> str:
        """SQLダイアレクトを返す
        
        Returns:
            "duckdb", "snowflake", "bigquery", "databricks"など
        """
        return "duckdb"
    
    def close(self) -> None:
        """接続を閉じる"""
        if self.connection:
            self.connection.close()
        self.is_connected = False
    
    def _ensure_connected(self) -> None:
        """接続確認ヘルパー"""
        if not self.is_connected:
            raise ConnectionError("Not connected to data source")