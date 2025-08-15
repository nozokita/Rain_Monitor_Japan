# 降水監視ダッシュボード（Streamlit + 常駐モニター）

監視（収集）は `monitor.py`、表示と設定編集は `app.py`（Streamlit）が担当します。  
**Windows でもタスクスケジューラー不要**で常駐動作できます。

---

## セットアップ

```bash
# 推奨（仮想環境）
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install --upgrade pip
pip install streamlit pandas altair pillow requests
# Windows で Outlook送信を使う場合（任意）
pip install pywin32
```

既存の config.json はそのまま使えます。UI から編集も可能です。

## 起動方法

1) **モニター（常駐・5分おき自己ループ）**
```bash
python monitor.py
```
config.json → "monitoring": {"enabled": true, "interval_minutes": 5}

常駐ログ: `logs/monitor.log`  
稼働ハートビート: `logs/monitor_heartbeat.json`（UIの「稼働状況」に反映）  
予測ホライズン: config["leads"]（既定 [0,15,30,45,60]）

2) **ダッシュボード（閲覧・設定編集）**
```bash
streamlit run app.py
```
# http://localhost:8501

左サイドバーのトグルで 1分ごと自動更新 をON/OFF  
**「🛠 設定」**タブから config.json をGUIで編集（保存ボタンで書き込み）

- 地点（name/lat/lon）の追加・編集
- メール通知（宛先、SMTP設定、ON/OFF）
- しきい値（強い雨/激しい雨）、収集間隔、保持日数、SQLiteパス
- 予測リード（0/15/30/45/60…）

## データベース（UI想定スキーマ）

```sql
CREATE TABLE IF NOT EXISTS nowcast(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  point_name TEXT,
  lat REAL,
  lon REAL,
  basetime TEXT,
  validtime TEXT,
  lead_min INTEGER,
  mmph REAL,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_nowcast_point_time
  ON nowcast(point_name, validtime, lead_min);
```

monitor.py は上記 nowcast へ保存します（既存の readings に併存させることも可能）。

## メール通知

- **Windows**：Outlook（pywin32）を利用
- **macOS/Linux**：config.notification.smtp を設定し、enabled: true にすると送信

## ログ＆稼働確認

- `logs/monitor.log` … monitor の動作ログ（UIの「🩺 稼働状況」でも末尾を表示）
- `logs/monitor_heartbeat.json` … {"last_run":"YYYY-mm-dd HH:MM:SS","ok":true,…} を書き出し

## トラブルシューティング

**UIが更新されない**
- 左サイドバーの「1分ごと自動更新」をONに。モニター側が書いているか `logs/monitor.log` を確認。

**DBが空のまま**
- monitor.py のログにエラーがないか、config.storage.sqlite_path の書き込み権限を確認。

**メールが届かない**
- Windows：Outlook起動/アカウント設定を確認
- SMTP：ホスト/ポート/ユーザ/パスワード/From を再確認。ファイアウォールも確認。

**45分先が出ない**
- config.json の "leads" に [0,15,30,45,60] が含まれているか確認。
- monitor.py が最新の config["leads"] を参照しているか確認。
