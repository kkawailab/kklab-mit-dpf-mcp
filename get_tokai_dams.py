#!/usr/bin/env python3
"""
東海地方のダムデータを取得してJSONファイルに保存するスクリプト
"""
import json
import subprocess

def get_dams_for_prefecture(pref_code, pref_name):
    """指定された都道府県のダムデータを取得"""
    print(f"{pref_name}のダムデータを取得中...")

    # MCPツールを使ってダムデータを取得
    cmd = [
        "npx",
        "-y",
        "mlit-dpf-mcp",
        "get_all_data",
        "--dataset_id", "dhb",
        "--prefecture_code", pref_code,
        "--size", "1000",
        "--max_batches", "1",
        "--include_metadata", "true"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"エラー: {result.stderr}")
        return []

    data = json.loads(result.stdout)
    dams = []

    for item in data.get("items", []):
        metadata = item.get("metadata", {})
        if metadata and "DPF:latitude" in metadata and "DPF:longitude" in metadata:
            dam_info = {
                "id": item.get("id"),
                "title": item.get("title"),
                "lat": metadata.get("DPF:latitude"),
                "lon": metadata.get("DPF:longitude"),
                "height": metadata.get("DHB:height"),
                "purpose": metadata.get("DHB:purpose"),
                "type": metadata.get("DHB:type"),
                "year": metadata.get("DPF:year"),
                "prefecture": pref_name
            }
            dams.append(dam_info)

    print(f"{pref_name}: {len(dams)}件のダムを取得")
    return dams

def main():
    """メイン処理"""
    # 東海地方の都道府県
    prefectures = [
        ("23", "愛知県"),
        ("21", "岐阜県"),
        ("22", "静岡県"),
        ("24", "三重県")
    ]

    all_dams = []

    for pref_code, pref_name in prefectures:
        dams = get_dams_for_prefecture(pref_code, pref_name)
        all_dams.extend(dams)

    # JSONファイルに保存
    output_file = "tokai_dams.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_dams, f, ensure_ascii=False, indent=2)

    print(f"\n合計 {len(all_dams)} 件のダムデータを {output_file} に保存しました")

if __name__ == "__main__":
    main()
