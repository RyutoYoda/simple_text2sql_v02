from typing import Dict, Any, List
from src.domain.interfaces import DataSourceConnector
from src.infrastructure.connectors.bigquery import BigQueryConnector
from src.infrastructure.connectors.snowflake import SnowflakeConnector
from src.infrastructure.connectors.databricks import DatabricksConnector
from src.infrastructure.connectors.local_file import LocalFileConnector
from src.infrastructure.connectors.google_sheets import GoogleSheetsConnector


class ConnectorFactory:
    """データソースコネクタのファクトリークラス"""
    
    _connectors = {
        "bigquery": BigQueryConnector,
        "snowflake": SnowflakeConnector,
        "databricks": DatabricksConnector,
        "local_file": LocalFileConnector,
        "google_sheets": GoogleSheetsConnector,
    }
    
    @classmethod
    def create_connector(cls, connector_type: str) -> DataSourceConnector:
        """指定されたタイプのコネクタを作成
        
        Args:
            connector_type: コネクタタイプ（bigquery, snowflake, databricks, local_file, google_sheets）
            
        Returns:
            DataSourceConnector: コネクタインスタンス
            
        Raises:
            ValueError: 不明なコネクタタイプの場合
        """
        connector_type = connector_type.lower()
        if connector_type not in cls._connectors:
            raise ValueError(f"Unknown connector type: {connector_type}")
        
        return cls._connectors[connector_type]()
    
    @classmethod
    def get_available_connectors(cls) -> List[str]:
        """利用可能なコネクタタイプのリストを返す"""
        return list(cls._connectors.keys())