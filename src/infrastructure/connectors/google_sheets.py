from typing import Dict, List, Any, Optional
import pandas as pd
import gspread
from src.infrastructure.connectors.base import BaseConnector


class GoogleSheetsConnector(BaseConnector):
    """Google Sheetsコネクタの実装"""
    
    def __init__(self):
        super().__init__()
        self.gc = None
        self.sheet = None
        self.worksheet = None
    
    def connect(self, credentials: Dict[str, Any]) -> None:
        """Google Sheetsに接続
        
        Args:
            credentials: {
                "credentials_path": "path/to/service_account.json",
                "sheet_url": "https://docs.google.com/spreadsheets/...",
                "worksheet_name": Optional[str]  # 省略時は最初のシート
            }
        """
        credentials_path = credentials['credentials_path']
        self.gc = gspread.service_account(filename=credentials_path)
        
        sheet_url = credentials.get('sheet_url')
        if sheet_url:
            self.sheet = self.gc.open_by_url(sheet_url)
            worksheet_name = credentials.get('worksheet_name')
            if worksheet_name:
                self.worksheet = self.sheet.worksheet(worksheet_name)
            else:
                self.worksheet = self.sheet.get_worksheet(0)
        
        self.is_connected = True
    
    def list_datasets(self) -> List[str]:
        """スプレッドシートのタイトルを返す"""
        self._ensure_connected()
        if self.sheet:
            return [self.sheet.title]
        return []
    
    def list_tables(self, dataset: str) -> List[str]:
        """ワークシート名のリストを返す"""
        self._ensure_connected()
        if self.sheet:
            return [ws.title for ws in self.sheet.worksheets()]
        return []
    
    def get_sample_data(self, dataset: str, table: str, limit: int = 1000) -> pd.DataFrame:
        """指定したワークシートのデータを取得"""
        self._ensure_connected()
        if self.sheet:
            worksheet = self.sheet.worksheet(table)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
            return df.head(limit)
        return pd.DataFrame()
    
    def get_table_schema(self, dataset: str, table: str) -> Dict[str, str]:
        """テーブルスキーマを取得（最初の行をカラム名として推定）"""
        self._ensure_connected()
        df = self.get_sample_data(dataset, table, limit=5)
        
        schema = {}
        for col in df.columns:
            dtype = str(df[col].dtype)
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
    
    def close(self) -> None:
        """接続を閉じる（Google Sheetsでは特に何もしない）"""
        self.is_connected = False