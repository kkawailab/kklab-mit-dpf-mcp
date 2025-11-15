#!/usr/bin/env python3
"""
東海地方のダムデータをMLIT DPF MCPから取得してJSONファイルに保存するスクリプト
"""
import json
import sys
import os

# MCPクライアントのパスを追加
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def fetch_tokai_dams():
    """
    東海地方（愛知、岐阜、静岡、三重）のダムデータを取得

    注意: このスクリプトは、MCP ServerがClaude Codeセッション内で実行されることを前提としています。
    スタンドアロンで実行する場合は、MCPクライアントのセットアップが必要です。
    """

    # 都道府県コード
    prefectures = [
        ("23", "愛知県"),
        ("21", "岐阜県"),
        ("22", "静岡県"),
        ("24", "三重県")
    ]

    all_dams = []

    print("東海地方のダムデータを取得しています...")
    print("注意: このスクリプトは実際にはMCPツールへのアクセスが必要です")
    print("Claude Codeセッション内で実行するか、手動でデータを取得してください")

    # ここでは、サンプルデータを返す
    # 実際のデータは、Claude Code内でMCPツールを使って取得する必要があります

    sample_data = {
        "message": "このスクリプトは Claude Code セッション内で MCP ツールを使用して実行する必要があります",
        "instructions": [
            "1. Claude Code 内で各県のダムデータを MCP ツールで取得",
            "2. 取得したデータを tokai_dams.json に保存",
            "3. plot_tokai_dams.py を実行して地図を生成"
        ],
        "mcp_command_example": "mcp__mlit-dpf-mcp__get_all_data で各県のデータを取得"
    }

    return sample_data


def main():
    """メイン処理"""
    result = fetch_tokai_dams()

    # 結果を表示
    print(json.dumps(result, ensure_ascii=False, indent=2))

    print("\n" + "="*60)
    print("次のステップ:")
    print("1. Claude Code セッション内で MCP ツールを使用してダムデータを取得")
    print("2. 取得したデータを tokai_dams.json として保存")
    print("3. python plot_tokai_dams.py を実行して地図を生成")
    print("="*60)


if __name__ == "__main__":
    main()
