#!/usr/bin/env python3
"""
東海地方のダムを地図にプロットするスクリプト
"""
import json
import folium
from folium.plugins import MarkerCluster

def create_dam_map(dams_data, output_file="tokai_dams_map.html"):
    """
    ダムデータから地図を作成

    Args:
        dams_data: ダム情報のリスト
        output_file: 出力HTMLファイル名
    """
    if not dams_data:
        print("ダムデータが空です")
        return

    # 東海地方の中心座標（おおよそ）
    center_lat = 35.3
    center_lon = 137.5

    # 地図を作成
    dam_map = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=8,
        tiles='OpenStreetMap'
    )

    # 都道府県ごとに色を設定
    prefecture_colors = {
        "愛知県": "red",
        "岐阜県": "blue",
        "静岡県": "green",
        "三重県": "purple"
    }

    # MarkerClusterを使用してマーカーをグループ化
    marker_cluster = MarkerCluster().add_to(dam_map)

    # 各ダムをマーカーとして追加
    for dam in dams_data:
        lat = dam.get('lat')
        lon = dam.get('lon')

        if lat is None or lon is None:
            continue

        title = dam.get('title', '不明')
        prefecture = dam.get('prefecture', '不明')
        color = prefecture_colors.get(prefecture, 'gray')

        # ポップアップの内容
        popup_text = f"""
        <b>{title}</b><br>
        都道府県: {prefecture}<br>
        """

        if 'height' in dam and dam['height']:
            popup_text += f"堤高: {dam['height']}m<br>"
        if 'year' in dam and dam['year']:
            year = dam['year'][0] if isinstance(dam['year'], list) else dam['year']
            popup_text += f"完成年: {year}<br>"

        # マーカーを追加
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_text, max_width=300),
            tooltip=title,
            icon=folium.Icon(color=color, icon='tint', prefix='fa')
        ).add_to(marker_cluster)

    # 凡例を追加
    legend_html = '''
    <div style="position: fixed;
                bottom: 50px; right: 50px; width: 180px; height: 150px;
                background-color: white; border:2px solid grey; z-index:9999;
                font-size:14px; padding: 10px">
    <p style="margin: 0; padding: 0; font-weight: bold;">凡例</p>
    <p style="margin: 5px 0;"><i class="fa fa-map-marker" style="color:red"></i> 愛知県</p>
    <p style="margin: 5px 0;"><i class="fa fa-map-marker" style="color:blue"></i> 岐阜県</p>
    <p style="margin: 5px 0;"><i class="fa fa-map-marker" style="color:green"></i> 静岡県</p>
    <p style="margin: 5px 0;"><i class="fa fa-map-marker" style="color:purple"></i> 三重県</p>
    </div>
    '''
    dam_map.get_root().html.add_child(folium.Element(legend_html))

    # HTMLファイルとして保存
    dam_map.save(output_file)
    print(f"地図を {output_file} に保存しました")
    print(f"合計 {len([d for d in dams_data if d.get('lat') and d.get('lon')])} 件のダムをプロットしました")

    return dam_map


def main():
    """メイン処理"""
    # JSONファイルからダムデータを読み込み
    try:
        with open("tokai_dams.json", "r", encoding="utf-8") as f:
            dams_data = json.load(f)
    except FileNotFoundError:
        print("tokai_dams.json が見つかりません")
        print("先に fetch_tokai_dams.py を実行してダムデータを取得してください")
        return

    # 地図を作成
    create_dam_map(dams_data)


if __name__ == "__main__":
    main()
