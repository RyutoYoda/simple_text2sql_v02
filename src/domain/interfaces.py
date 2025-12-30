from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import pandas as pd


class DataSourceConnector(ABC):
    """データソースコネクタの基底インターフェース"""
    
    @abstractmethod
    def connect(self, credentials: Dict[str, Any]) -> None:
        """データソースに接続"""
        pass
    
    @abstractmethod
    def list_datasets(self) -> List[str]:
        """利用可能なデータセット/スキーマのリストを取得"""
        pass
    
    @abstractmethod
    def list_tables(self, dataset: str) -> List[str]:
        """指定データセット内のテーブルリストを取得"""
        pass
    
    @abstractmethod
    def get_sample_data(self, dataset: str, table: str, limit: int = 1000) -> pd.DataFrame:
        """サンプルデータを取得"""
        pass
    
    @abstractmethod
    def get_table_schema(self, dataset: str, table: str) -> Dict[str, str]:
        """テーブルスキーマ（カラム名と型）を取得"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """接続を閉じる"""
        pass