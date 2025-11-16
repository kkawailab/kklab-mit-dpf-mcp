# MLIT DATA PLATFORM MCP Server

> **⚠️ 重要な免責事項**
> 本リポジトリは、国土交通省の公式リポジトリ [mlit-dpf-mcp](https://github.com/MLIT-DATA-PLATFORM/mlit-dpf-mcp) を利用して作成された非公式のアプリケーションです。
> **国土交通省の認可や承認を受けたものではありません。**
> 本リポジトリの利用により生じたいかなる損失及び障害等について、作成者は責任を負わないものとします。

## 目次
- [MLIT DATA PLATFORM MCP Server](#mlit-data-platform-mcp-server)
  - [目次](#目次)
  - [1. 概要](#1-概要)
  - [2. 主な機能](#2-主な機能)
  - [3. 動作環境](#3-動作環境)
  - [4. インストールとセットアップ](#4-インストールとセットアップ)
    - [4.1. Claude Desktopでの使用方法](#41-claude-desktopでの使用方法)
    - [4.2. Claude Codeでの使用方法](#42-claude-codeでの使用方法)
  - [5. 使用例](#5-使用例)
    - [5.1. 基本的な検索](#51-基本的な検索)
    - [5.2. ダム地図の作成（サンプルプロジェクト）](#52-ダム地図の作成サンプルプロジェクト)
  - [6. トラブルシューティング](#6-トラブルシューティング)
  - [7. ディレクトリ構成](#7-ディレクトリ構成)
  - [8. ライセンス](#8-ライセンス)
  - [9. 注意事項](#9-注意事項)
  - [10. お問い合わせ](#10-お問い合わせ)


## 1. 概要

本リポジトリは、国土交通省の公式リポジトリ [mlit-dpf-mcp](https://github.com/MLIT-DATA-PLATFORM/mlit-dpf-mcp) をベースに作成された**非公式**のMCP (Model Context Protocol) サーバーです。

国土交通省が保有するデータと民間等のデータを連携し、一元的に検索・表示・ダウンロードを可能にする[国土交通データプラットフォーム](https://www.mlit-data.jp/)が提供する利用者向けAPIと接続します。

本MCPサーバーを利用することで、大規模言語モデル（LLM）と直接連携し、対話形式で直感的にデータを検索・取得することが可能になります。APIに関する専門的な知識がなくても、誰でも簡単に国土交通データプラットフォームから曖昧な指示や複雑な条件設定でデータを検索・取得が可能な、新しいデータ活用のかたちを提供します。

**⚠️ 本リポジトリは国土交通省の認可や承認を受けたものではありません。個人が作成した非公式のアプリケーションです。**


## 2. 主な機能
国土交通データプラットフォームの利用者向けAPIを活用し、以下の機能を提供します：
* `search`（キーワードの指定によりデータを検索します。並べ替えや件数の指定も可能です。）
* `search_by_location_rectangle`（指定した矩形範囲と交差するデータを検索します。）
* `search_by_location_point_distance`（指定した地点と半径からなる円形範囲と交差するデータを検索します。）
* `search_by_attribute`（カタログ名、データセット名、都道府県、市区町村などの属性を指定してデータを検索します。）
* `get_data`（データの詳細情報を取得します。）
* `get_data_summary`（データIDとタイトルなどのデータの基本情報を取得します。）
* `get_data_catalog`（データカタログやデータセットの詳細情報を取得します。）
* `get_data_catalog_summary`（IDやタイトルなどのデータカタログやデータセットの基本情報を取得します。）
* `get_file_download_urls`（ファイルのダウンロード用URLを取得します（有効期限：60秒）。）
* `get_zipfile_download_url`（複数ファイルをZIP形式でまとめたダウンロードURLを取得します（有効期限：60秒）。）
* `get_thumbnail_urls`（サムネイル画像のURLを取得します（有効期限：60秒）。）
* `get_all_data`（条件に一致する大量のデータを一括取得します。）
* `get_count_data`（条件に一致するデータ件数を取得します。）
* `get_suggest`（キーワード検索時の候補を取得します。）
* `get_prefecture_data`（都道府県名・コードの一覧を取得します。）
* `get_municipality_data`（市区町村名・コードの一覧を取得します。）
* `get_mesh`（指定したメッシュに含まれるデータを取得します。）
* `normalize_codes`（入力された都道府県名・市区町村名を正規化します。）


## 3. 動作環境

* OS：Windows 10 / 11 または macOS 13以降
* MCPホスト：Claude Desktopなど
* MCPサーバー実行環境：Python 3.10+
* メモリ：8GB以上推奨
* ストレージ：空き容量 1GB以上（キャッシュやログを含む）

## 4. インストールとセットアップ

本MCPサーバーは、**Claude Desktop** と **Claude Code** の両方で使用できます。それぞれの設定方法を以下に説明します。

### 共通の準備手順

どちらの環境でも、まず以下の準備が必要です。

#### 1. APIキーの取得

国土交通データプラットフォームでアカウントを作成し、APIキーを取得してください。

詳しい手順は、[こちら](https://www.mlit-data.jp/api_docs/usage/introduction.html)をご覧ください。

#### 2. リポジトリのクローン

```bash
git clone https://github.com/MLIT-DATA-PLATFORM/mlit-dpf-mcp.git
cd mlit-dpf-mcp
```

---

### 4.1. Claude Desktopでの使用方法

Claude Desktop（デスクトップアプリ）で使用する場合の設定方法です。

#### 前提条件
- Claude Desktopアプリがインストールされている
- Python 3.10以上がインストールされている

#### 手順

**1. 仮想環境を作成 & 有効化**

```bash
python -m venv .venv
.venv\Scripts\activate      # Windows
source .venv/bin/activate   # macOS/Linux
```

**2. 依存ライブラリをインストール**

```bash
pip install -e .
pip install aiohttp pydantic tenacity python-json-logger mcp python-dotenv
```

**3. 環境変数を設定**

`.env.example`をコピーし、 `.env` ファイルを作成します：

```
MLIT_API_KEY=your_api_key_here
MLIT_BASE_URL=https://www.mlit-data.jp/api/v1/
```

`your_api_key_here`は必ず、手順1で取得したAPIキーに置き換えてください。

**4. Claude Desktopの設定ファイルを開く**

- **Windows：** `C:\Users\<ユーザー名>\AppData\Roaming\Claude\claude_desktop_config.json`
- **macOS：** `~/Library/Application Support/Claude/claude_desktop_config.json`
- Claude Desktopアプリの設定画面にある「開発者」メニューの「設定を編集」ボタンをクリックして`claude_desktop_config.json`を開くことも可能です。

**5. MCPサーバーの構成を追加**

```json
{
  "mcpServers": {
    "mlit-dpf-mcp": {
      "command": "......./mlit-dpf-mcp/.venv/Scripts/python.exe",
      "args": [
        "....../mlit-dpf-mcp/src/server.py"
      ],
      "env": {
        "MLIT_API_KEY": "your_api_key_here",
        "MLIT_BASE_URL": "https://www.mlit-data.jp/api/v1/",
        "PYTHONUNBUFFERED": "1",
        "LOG_LEVEL": "WARNING"
      }
    }
  }
}
```

`command`と`args`は必ず、実際のパスに変更してください。
`your_api_key_here`は必ず、手順1で取得したAPIキーに置き換えてください。

**6. Claude Desktop を再起動**

---

### 4.2. Claude Codeでの使用方法

Claude Code（VS Code拡張機能）で使用する場合の設定方法です。

#### 前提条件
- Visual Studio Code がインストールされている
- Claude Code 拡張機能がインストールされている
- Python 3.10以上がインストールされている
- `uv` または `pip` がインストールされている

#### 手順

**1. プロジェクトディレクトリに移動**

```bash
cd mlit-dpf-mcp
```

**2. 依存ライブラリをインストール**

`uv`を使用する場合（推奨）:
```bash
uv pip install -e .
```

または通常の`pip`を使用:
```bash
pip install -e .
```

**3. MCP設定ファイルを作成**

プロジェクトのルートディレクトリに `.mcp.json` ファイルを作成します：

**4. MCP設定を記述**

`.mcp.json` に以下の内容を記述します：

```json
{
  "mcpServers": {
    "mlit-dpf-mcp": {
      "command": "uv",
      "args": [
        "--directory",
        "/絶対パス/mlit-dpf-mcp",
        "run",
        "python",
        "-m",
        "src.server"
      ],
      "env": {
        "MLIT_API_KEY": "your_api_key_here",
        "MLIT_BASE_URL": "https://www.mlit-data.jp/api/v1/"
      }
    }
  }
}
```

**重要な設定項目:**
- `"/絶対パス/mlit-dpf-mcp"`: このリポジトリの絶対パスに置き換えてください
  - Linux/macOS例: `"/home/username/mlit-dpf-mcp"`
  - Windows例: `"C:/Users/username/mlit-dpf-mcp"` (スラッシュを使用)
- `"your_api_key_here"`: 取得したAPIキーに置き換えてください

**注意:** `.mcp.json` はプロジェクトのルートディレクトリに配置してください（`.claude/` ディレクトリ内ではありません）

**5. VS Codeでプロジェクトを開く**

```bash
code .
```

**6. Claude Codeを起動**

VS Codeのコマンドパレット（Ctrl+Shift+P / Cmd+Shift+P）から「Claude Code: Start」を選択します。

**7. MCPサーバーの接続を確認**

Claude Codeのチャット画面で、MCPツールが利用可能になっていることを確認します。ツールアイコン（🔧）をクリックすると、`mcp__mlit-dpf-mcp__`で始まる各種ツールが表示されます。

#### トラブルシューティング（Claude Code）

**MCPサーバーが起動しない場合:**

1. `.mcp.json` のパスが正しいか確認（プロジェクトルートに配置されているか）
2. APIキーが正しく設定されているか確認
3. VS Codeの出力パネル（Output Panel）で「Claude Code」を選択し、エラーログを確認

**依存関係のエラーが出る場合:**

```bash
uv pip install aiohttp pydantic tenacity python-json-logger mcp python-dotenv
```

---

## 5. 使用例

### 5.1. 基本的な検索

MCPサーバーが正しく設定されていれば、Claude DesktopまたはClaude Codeで自然言語による対話形式でデータを検索できます。

**例：キーワード検索**
```
「東京都のダムを教えて」
```

Claudeは自動的にMCPツールを使用して、以下のような処理を実行します：
1. 都道府県名「東京都」を正規化してコード「13」を取得
2. データセット「ダム便覧（dhb）」を検索
3. 該当するダムのリストを表示

**例：位置情報による検索**
```
「東京駅から半径5km以内にある公共施設を検索して」
```

**例：データの可視化**
```
「岐阜県のダムを地図にプロットして」
```

### 5.2. ダム地図の作成（サンプルプロジェクト）

このリポジトリには、東海地方のダムを地図にプロットするサンプルプロジェクトが含まれています。

#### 含まれるファイル

- `plot_tokai_dams.py` - 地図生成のメインスクリプト
- `tokai_dams.json` - 東海地方のダムデータ（12件のサンプル）
- `tokai_dams_map.html` - 生成されたインタラクティブ地図
- `fetch_tokai_dams.py`, `get_tokai_dams.py` - データ取得用スクリプト

#### 実行方法

**1. 必要なライブラリをインストール**

```bash
uv pip install folium
```

または：

```bash
pip install folium
```

**2. 地図を生成**

```bash
python plot_tokai_dams.py
```

**3. ブラウザで地図を開く**

```bash
# Linux/macOS
xdg-open tokai_dams_map.html

# Windows
start tokai_dams_map.html
```

#### 地図の特徴

- 都道府県ごとに色分けされたマーカー
  - 🔴 赤：愛知県
  - 🔵 青：岐阜県
  - 🟢 緑：静岡県
  - 🟣 紫：三重県
- マーカーをクリックするとダムの詳細情報を表示
- MarkerCluster機能で見やすく表示
- インタラクティブなズーム・パン操作

#### カスタマイズ

全216件のダムデータを取得して完全な地図を作成する場合は、Claude CodeまたはClaude Desktopで以下のように依頼してください：

```
「東海地方の全てのダムデータを取得してtokai_dams.jsonを更新して」
```

---

## 6. トラブルシューティング

### よくある問題と解決方法

#### 1. MCPサーバーが起動しない

**症状：** Claude DesktopまたはClaude CodeでMCPツールが表示されない

**解決方法：**
- 設定ファイルのパスが正しいか確認
- APIキーが正しく設定されているか確認
- Python環境が正しくアクティベートされているか確認

**Claude Desktopの場合：**
```bash
# パスの確認
which python  # macOS/Linux
where python  # Windows
```

**Claude Codeの場合：**
VS Codeの出力パネル（Output）で「Claude Code」を選択し、エラーログを確認してください。

#### 2. APIキーのエラー

**症状：** `MLIT_API_KEY is not set` などのエラーメッセージ

**解決方法：**
- `.mcp.json`（Claude Code）または`claude_desktop_config.json`（Claude Desktop）で、`MLIT_API_KEY`が正しく設定されているか確認
- APIキーに余分なスペースや引用符が含まれていないか確認
- 国土交通データプラットフォームでAPIキーが有効か確認

#### 3. 依存関係のエラー

**症状：** `ModuleNotFoundError: No module named 'aiohttp'` などのエラー

**解決方法：**
```bash
# 必要なパッケージを再インストール
uv pip install aiohttp pydantic tenacity python-json-logger mcp python-dotenv

# または
pip install aiohttp pydantic tenacity python-json-logger mcp python-dotenv
```

#### 4. 地図生成のエラー

**症状：** `ModuleNotFoundError: No module named 'folium'`

**解決方法：**
```bash
uv pip install folium
# または
pip install folium
```

#### 5. データが取得できない

**症状：** 検索しても結果が返ってこない

**解決方法：**
- APIキーの有効性を確認
- 検索条件を変更してみる（キーワードを簡略化、範囲を広げるなど）
- ネットワーク接続を確認
- APIのレート制限に達していないか確認

### デバッグ方法

#### ログレベルの変更

より詳細なログを確認したい場合は、設定ファイルで`LOG_LEVEL`を変更してください：

**Claude Desktop（`claude_desktop_config.json`）：**
```json
"env": {
  "LOG_LEVEL": "DEBUG"
}
```

**Claude Code（`.mcp.json`）：**
```json
"env": {
  "LOG_LEVEL": "DEBUG"
}
```

#### 手動でのサーバー起動テスト

MCPサーバーが正しく動作するか、手動で起動してテストできます：

```bash
cd mlit-dpf-mcp
export MLIT_API_KEY=your_api_key_here
export MLIT_BASE_URL=https://www.mlit-data.jp/api/v1/
python -m src.server
```

エラーが表示される場合は、そのメッセージに従って問題を解決してください。

---

## 7. ディレクトリ構成

```
mlit-dpf-mcp/
├─ .mcp.json                     # MCP設定ファイル（Claude Code用）
├─ .claude/                      # Claude Code設定ディレクトリ（権限設定など）
│  └─ settings.local.json        # ツール権限設定ファイル
├─ src/
│  ├─ server.py                  # MCP サーバー & ツール定義
│  ├─ client.py                  # MLIT GraphQL API クライアント
│  ├─ schemas.py                 # Pydantic モデル（入力バリデーション）
│  ├─ config.py                  # 環境変数ロード & 設定検証
│  └─ utils.py                   # ロギング、タイマー、レート制限
├─ plot_tokai_dams.py            # 地図プロット用スクリプト（サンプル）
├─ fetch_tokai_dams.py           # ダムデータ取得スクリプト（サンプル）
├─ get_tokai_dams.py             # データ取得ヘルパースクリプト（サンプル）
├─ tokai_dams.json               # 東海地方のダムデータ（サンプル）
├─ tokai_dams_map.html           # 生成された地図HTMLファイル（サンプル）
├─ pyproject.toml                # プロジェクト設定ファイル
├─ uv.lock                       # 依存関係ロックファイル
├─ README.md                     # このファイル
├─ LICENSE                       # ライセンスファイル
└─ .env.example                  # 環境変数のサンプル
```

## 8. ライセンス

本リポジトリはMITライセンスで提供されています。詳細は[LICENSE](./LICENSE)を参照してください。

---

## 9. 注意事項

### 非公式リポジトリについて
* **本リポジトリは、国土交通省の公式リポジトリ [mlit-dpf-mcp](https://github.com/MLIT-DATA-PLATFORM/mlit-dpf-mcp) をベースに作成された非公式のアプリケーションです。**
* **国土交通省の認可、承認、推奨を受けたものではありません。**
* **本リポジトリは個人が作成したものであり、国土交通省および国土交通データプラットフォームとは一切関係ありません。**

### データ利用について
* 本リポジトリで提供されるデータの利用に関しては、 [国土交通データプラットフォームの利用規約](https://www.mlit-data.jp/assets/policy/%E5%9B%BD%E5%9C%9F%E4%BA%A4%E9%80%9A%E3%83%87%E3%83%BC%E3%82%BF%E3%83%97%E3%83%A9%E3%83%83%E3%83%88%E3%83%95%E3%82%A9%E3%83%BC%E3%83%A0%E5%88%A9%E7%94%A8%E8%A6%8F%E7%B4%84.pdf)に従う必要があります。ご使用前に国土交通データプラットフォームの利用規約を必ずご確認ください。
* 本リポジトリの個人情報の取り扱いは、[国土交通データプラットフォームのプライバシーポリシー](https://www.mlit-data.jp/assets/policy/%E5%9B%BD%E5%9C%9F%E4%BA%A4%E9%80%9A%E3%83%87%E3%83%BC%E3%82%BF%E3%83%97%E3%83%A9%E3%83%83%E3%83%88%E3%83%95%E3%82%A9%E3%83%BC%E3%83%A0_%E3%83%97%E3%83%A9%E3%82%A4%E3%83%90%E3%82%B7%E3%83%BC%E3%83%9D%E3%83%AA%E3%82%B7%E3%83%BC.pdf)に準拠する必要があります。

### 免責事項
* 本リポジトリは非公式のアプリケーションとして提供しているものです。動作保証は行っておりません。
* 本リポジトリの内容は予告なく変更・削除する可能性があります。
* 本リポジトリの利用により生じた損失及び障害等について、作成者は一切の責任を負わないものとします。
* 本リポジトリに関する問い合わせを国土交通省または国土交通データプラットフォームに行わないでください。

---

## 10. お問い合わせ

本リポジトリは非公式のアプリケーションです。

* **本リポジトリに関する問い合わせは、GitHubのIssuesまでお願いします。**
* **国土交通省または国土交通データプラットフォームへの問い合わせは行わないでください。**

国土交通データプラットフォームの公式リポジトリや利用者向けAPIに関する問い合わせは、以下の公式チャンネルをご利用ください：
* [国土交通データプラットフォーム公式リポジトリ](https://github.com/MLIT-DATA-PLATFORM/mlit-dpf-mcp)
* [国土交通データプラットフォームお問い合わせフォーム](https://docs.google.com/forms/d/e/1FAIpQLScHlMUInwpoyREX672SFJuwo8ZfpllQUatPuYNRiKYZkoe6nQ/viewform)
