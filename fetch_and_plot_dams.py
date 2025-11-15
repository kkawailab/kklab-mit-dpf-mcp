#!/usr/bin/env python3
"""
東海地方のダムデータを取得して地図にプロットするスクリプト
"""
import json

# 各県のダムIDリスト（既に取得済み）
dam_ids = {
    "愛知県": [
        "0575417e-83c2-4900-bac3-646e2d8a1eea", "1b1a0b3c-01fb-4e67-ba04-4bc84844f092",
        "22c89eeb-2ce3-4b1e-ae60-a786d605eb6c", "2b936e5b-de3c-4e25-bfe3-b7abd7e59483",
        "34703f81-c3b3-48ad-98b9-a2cc77e6f36a", "38d52718-8562-4cdb-a363-c3427fc3df72",
        "3c9b7ef8-9420-4459-a4f4-1deb780b1be9", "3cb5645d-cab1-48eb-a398-c3e17d5be072",
        "3d88c1e9-9d57-4780-9053-4eab3ae09f1b", "3ecaf2c7-09f7-414d-9dcf-2571f92174fc",
        "41b8c512-c526-4559-b8cb-237cdc17445f", "5a94c125-a47f-4cd9-97c3-28e546bbd064",
        "5e3d5aa9-3cf4-4466-840d-2d4e1c674561", "6023f276-70ea-47ca-8938-0439456f59e3",
        "6743eb15-b09a-4314-ae32-01a248397ab5", "71a4d884-7fd7-421e-b871-a588931b9446",
        "7b40c87e-2e80-462b-adb9-ca84b1ced024", "7f46e2d4-50a6-41db-bf0f-285ee0f0f7e8",
        "8792ce15-7756-4b19-8fe4-993489cb5adc", "8f6277ec-5aa7-4d24-a696-147aeae9b12a",
        "9d7e75c1-3e04-4d4b-9aa1-8f5c4501c08e", "a96930ef-66b4-4a8a-a384-d908a85a3c3d",
        "af23d637-4754-4a93-a984-4b08ae646a2d", "b20c3722-7c68-4a5b-bf37-ec2b3ad2c123",
        "b23746c1-caa0-4467-b06f-8fd493ef6921", "bfd870bd-d040-456d-8eab-63d3d372817a",
        "c3025797-865b-45e3-9ff5-2db62cfb963b", "c7d581b5-ce68-4040-b518-177eeab599ed",
        "d2f71aeb-47b4-49f8-9c67-06c7a5194462", "d4068951-96ff-4963-b3c0-c8d927457d61",
        "d5aa822e-3658-4d16-b8b4-e509dd16c592", "d90f84d5-fe42-451e-a22d-3788683bac01",
        "f318a45e-a0f9-48d3-9247-0d5aaaac824b", "f77087b0-1040-4297-b0a9-a1ecbf1c46df",
        "f800c6c7-257a-4091-876e-cce19651d3d3"
    ]
}

# サンプルデータ（実際のデータは後で追加）
# ここでは手動でサンプルデータを作成
sample_dams = [
    {"title": "設楽", "lat": 35.09027, "lon": 137.5525, "prefecture": "愛知県"},
    {"title": "御母衣", "lat": 36.13813, "lon": 136.9104, "prefecture": "岐阜県"},
    {"title": "井川", "lat": 35.21005, "lon": 138.2227, "prefecture": "静岡県"}
]

print("サンプルダムデータを作成しました")
print(f"ダム数: {len(sample_dams)}")

# JSONファイルに保存
with open("tokai_dams_sample.json", "w", encoding="utf-8") as f:
    json.dump(sample_dams, f, ensure_ascii=False, indent=2)

print("tokai_dams_sample.json に保存しました")
