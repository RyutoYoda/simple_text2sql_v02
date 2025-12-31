from typing import Dict, List, Any, Optional
import pandas as pd
import snowflake.connector
from src.infrastructure.connectors.base import BaseConnector


class SnowflakeConnector(BaseConnector):
    """Snowflakeコネクタの実装"""
    
    def connect(self, credentials: Dict[str, Any]) -> None:
        """Snowflakeに接続
        
        Args:
            credentials: {
                "account": "xxx.snowflakecomputing.com",
                "user": "username",
                "private_key": "private_key_content",
                "private_key_passphrase": Optional[str], # パスフレーズ（必要な場合）
                "warehouse": "warehouse_name",
                "database": Optional[str],
                "schema": Optional[str],
                "role": Optional[str]
            }
        """
        # プライベートキーを使った認証の設定
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        
        # プライベートキーをバイト形式に変換
        private_key_bytes = credentials['private_key'].encode()
        passphrase = credentials.get('private_key_passphrase')
        
        if passphrase:
            passphrase_bytes = passphrase.encode()
        else:
            passphrase_bytes = None
        
        # プライベートキーオブジェクトを作成
        private_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=passphrase_bytes,
            backend=default_backend()
        )
        
        # DERフォーマットに変換
        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        self.connection = snowflake.connector.connect(
            account=credentials['account'],
            user=credentials['user'],
            private_key=private_key_der,
            warehouse=credentials.get('warehouse'),
            database=credentials.get('database'),
            schema=credentials.get('schema'),
            role=credentials.get('role')
        )
        self.cursor = self.connection.cursor()
        self.is_connected = True
    
    def list_datasets(self) -> List[str]:
        """利用可能なデータベースのリストを取得"""
        self._ensure_connected()
        self.cursor.execute("SHOW DATABASES")
        databases = self.cursor.fetchall()
        return [db[1] for db in databases]  # name列を取得
    
    def list_schemas(self, database: str) -> List[str]:
        """指定データベース内のスキーマリストを取得"""
        self._ensure_connected()
        self.cursor.execute(f"USE DATABASE {database}")
        self.cursor.execute("SHOW SCHEMAS")
        schemas = self.cursor.fetchall()
        return [schema[1] for schema in schemas]  # name列を取得
    
    def list_tables(self, dataset: str, schema: str = None) -> List[str]:
        """指定データベース・スキーマ内のテーブルリストを取得"""
        self._ensure_connected()
        self.cursor.execute(f"USE DATABASE {dataset}")
        
        if schema:
            self.cursor.execute(f"USE SCHEMA {schema}")
        else:
            # スキーマが指定されていない場合は、最初のスキーマを使用
            self.cursor.execute("SHOW SCHEMAS")
            schemas = self.cursor.fetchall()
            if schemas:
                default_schema = schemas[0][1]  # 最初のスキーマ名
                self.cursor.execute(f"USE SCHEMA {default_schema}")
        
        self.cursor.execute("SHOW TABLES")
        tables = self.cursor.fetchall()
        return [table[1] for table in tables]  # name列を取得
    
    def get_sample_data(self, dataset: str, table: str, schema: str = None, limit: int = 1000) -> pd.DataFrame:
        """サンプルデータを取得"""
        self._ensure_connected()
        if schema:
            query = f"SELECT * FROM {dataset}.{schema}.{table} LIMIT {limit}"
        else:
            query = f"SELECT * FROM {dataset}.{table} LIMIT {limit}"
        self.cursor.execute(query)
        
        # カラム名を取得
        columns = [desc[0] for desc in self.cursor.description]
        data = self.cursor.fetchall()
        
        return pd.DataFrame(data, columns=columns)
    
    def get_table_schema(self, dataset: str, table: str, schema: str = None) -> Dict[str, str]:
        """テーブルスキーマを取得"""
        self._ensure_connected()
        if schema:
            self.cursor.execute(f"DESCRIBE TABLE {dataset}.{schema}.{table}")
        else:
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