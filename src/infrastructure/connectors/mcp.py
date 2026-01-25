"""
MCP (Model Context Protocol) Connector
Streamable HTTP経由でMCPサーバーに接続
"""
from typing import Dict, List, Any, Optional
import httpx
from datetime import datetime


class MCPConnector:
    """MCP Server用のコネクタ（Streamlit用に同期版に変更）"""

    def __init__(self):
        self.server_url: Optional[str] = None
        self.api_key: Optional[str] = None
        self.client: Optional[httpx.Client] = None
        self.is_connected = False
        self.tools: List[Dict[str, Any]] = []
        self.server_info: Dict[str, Any] = {}
        self.request_id = 0

    def connect(self, server_url: str, api_key: Optional[str] = None, server_name: str = "MCP Server") -> Dict[str, Any]:
        """
        MCPサーバーに接続

        Args:
            server_url: MCPサーバーのURL (例: https://your-mcp-server.com/mcp)
            api_key: API Key認証用のトークン（オプション）
            server_name: サーバーの表示名

        Returns:
            接続情報とサーバー情報
        """
        self.server_url = server_url
        self.api_key = api_key

        # HTTPクライアント作成（同期版）
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        self.client = httpx.Client(
            headers=headers,
            timeout=30.0
        )

        try:
            self.request_id += 1
            # MCPサーバーに初期化リクエスト送信
            response = self.client.post(
                server_url,
                json={
                    "jsonrpc": "2.0",
                    "id": self.request_id,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {},
                        "clientInfo": {
                            "name": "FlashViz",
                            "version": "1.0.0"
                        }
                    }
                }
            )
            response.raise_for_status()
            result = response.json()

            if "error" in result:
                raise ConnectionError(f"MCP initialization failed: {result['error']}")

            self.server_info = result.get("result", {}).get("serverInfo", {})
            self.is_connected = True

            # ツール一覧を取得
            self.refresh_tools()

            return {
                "status": "connected",
                "server_name": server_name,
                "server_info": self.server_info,
                "tools_count": len(self.tools),
                "connected_at": datetime.now().isoformat()
            }

        except Exception as e:
            self.is_connected = False
            raise ConnectionError(f"Failed to connect to MCP server: {str(e)}")

    def refresh_tools(self) -> List[Dict[str, Any]]:
        """
        利用可能なツール一覧を取得・更新

        Returns:
            ツールのリスト
        """
        if not self.is_connected or not self.client:
            raise ConnectionError("Not connected to MCP server")

        try:
            self.request_id += 1
            response = self.client.post(
                self.server_url,
                json={
                    "jsonrpc": "2.0",
                    "id": self.request_id,
                    "method": "tools/list",
                    "params": {}
                }
            )
            response.raise_for_status()
            result = response.json()

            if "error" in result:
                raise RuntimeError(f"Failed to list tools: {result['error']}")

            self.tools = result.get("result", {}).get("tools", [])
            return self.tools

        except Exception as e:
            raise RuntimeError(f"Failed to refresh tools: {str(e)}")

    def list_tools(self) -> List[Dict[str, Any]]:
        """
        キャッシュされたツール一覧を返す（同期版）

        Returns:
            ツールのリスト
        """
        return self.tools

    def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Any:
        """
        MCPツールを実行

        Args:
            tool_name: 実行するツールの名前
            arguments: ツールに渡す引数

        Returns:
            ツールの実行結果
        """
        if not self.is_connected or not self.client:
            raise ConnectionError("Not connected to MCP server")

        try:
            self.request_id += 1
            response = self.client.post(
                self.server_url,
                json={
                    "jsonrpc": "2.0",
                    "id": self.request_id,
                    "method": "tools/call",
                    "params": {
                        "name": tool_name,
                        "arguments": arguments
                    }
                }
            )
            response.raise_for_status()
            result = response.json()

            if "error" in result:
                raise RuntimeError(f"Tool execution failed: {result['error']}")

            return result.get("result", {})

        except Exception as e:
            raise RuntimeError(f"Failed to call tool '{tool_name}': {str(e)}")

    def close(self) -> None:
        """接続を閉じる"""
        if self.client:
            self.client.close()
        self.is_connected = False
        self.tools = []
        self.server_info = {}

    def get_server_info(self) -> Dict[str, Any]:
        """サーバー情報を取得"""
        return {
            "server_info": self.server_info,
            "is_connected": self.is_connected,
            "server_url": self.server_url,
            "tools_count": len(self.tools)
        }


# エイリアス（後方互換性のため）
class MCPConnectorSync(MCPConnector):
    """MCPConnectorと同じ（もはや非同期ラッパー不要）"""
    pass
