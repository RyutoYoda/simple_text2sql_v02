from typing import Dict, List, Any, Optional
import pandas as pd
import os
from src.infrastructure.connectors.base import BaseConnector


class LocalFileConnector(BaseConnector):
    """ローカルファイルコネクタの実装"""
    
    def __init__(self):
        super().__init__()
        self.file_path = None
        self.df = None
    
    def connect(self, credentials: Dict[str, Any]) -> None:
        """ファイルを読み込む

        Args:
            credentials: {
                "file_path": "path/to/file.csv",
                "file_type": "csv" or "parquet" or "excel"
            }
        """
        self.file_path = credentials['file_path']
        file_type = credentials.get('file_type', 'csv').lower()

        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        if file_type == 'csv':
            self.df = pd.read_csv(self.file_path)
        elif file_type == 'parquet':
            self.df = pd.read_parquet(self.file_path)
        elif file_type in ['excel', 'xlsx', 'xls']:
            self.df = pd.read_excel(self.file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        self.is_connected = True
    
    def list_datasets(self) -> List[str]:
        """ファイル名を返す（データセットの代わり）"""
        self._ensure_connected()
        return [os.path.basename(self.file_path)]
    
    def list_tables(self, dataset: str) -> List[str]:
        """'data'という固定テーブル名を返す"""
        self._ensure_connected()
        return ['data']
    
    def get_sample_data(self, dataset: str, table: str, limit: int = 1000) -> pd.DataFrame:
        """サンプルデータを取得"""
        self._ensure_connected()
        return self.df.head(limit)
    
    def get_table_schema(self, dataset: str, table: str) -> Dict[str, str]:
        """テーブルスキーマを取得"""
        self._ensure_connected()
        schema = {}
        for col in self.df.columns:
            dtype = str(self.df[col].dtype)
            # pandas dtypeをSQL型にマッピング
            if 'int' in dtype:
                schema[col] = 'INTEGER'
            elif 'float' in dtype:
                schema[col] = 'FLOAT'
            elif 'object' in dtype or 'string' in dtype:
                schema[col] = 'STRING'
            elif 'datetime' in dtype:
                schema[col] = 'TIMESTAMP'
            elif 'bool' in dtype:
                schema[col] = 'BOOLEAN'
            else:
                schema[col] = 'STRING'
        
        return schema