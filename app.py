#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — シンプル＆クリーンなダッシュボード v3
- 日本時間（JST）で統一表示
- わかりやすい日本語表記
- ズーム可能な雨雲レーダー
- シンプルな降水量カードデザイン
"""

from __future__ import annotations
import os, json, sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from textwrap import dedent

JST = timezone(timedelta(hours=9))
CONFIG_PATH = os.environ.get("NOWCAST_CONFIG", "config.json")

DEFAULTS = {
    "locations": [
        {
            "name": "三島駅",
            "lat": 35.126474871810345,
            "lon": 138.91109391000256,
            "email_to": "",
            "thresholds": {"heavy_rain": 30, "torrential_rain": 50},
            "enabled": True
        }
    ],
    "monitoring": {"enabled": True, "interval_minutes": 5},
    "thresholds": {"heavy_rain": 30, "torrential_rain": 50},
    "notification": {
        "enabled": True,
        "admin_email": "",
        "admin_notification_times": ["09:00", "17:00"],
        "cooldown_minutes": 30,
        "outlook": {"enabled": True, "importance": "Normal"}
    },
    "storage": {"sqlite_path": "data/nowcast.sqlite", "retention_days": 3},
    "log": {"suppress_warn": True},
    "leads": [0, 15, 30, 45, 60],
    "debug": False
}

# ---------- 設定管理 ----------
def load_config() -> Dict[str, Any]:
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}
    else:
        cfg = {}
    
    def deep_merge(a, b):
        for k, v in b.items():
            if isinstance(v, dict):
                a[k] = deep_merge(a.get(k, {}), v)
            elif isinstance(v, list):
                a.setdefault(k, v)
            else:
                a.setdefault(k, v)
        return a
    
    return deep_merge(cfg, DEFAULTS.copy())

def save_config(cfg: Dict[str, Any]) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

# ---------- データベース ----------
def connect_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    
    # テーブル作成
    conn.execute("""
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
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_nowcast_point_time ON nowcast(point_name, validtime, lead_min)")
    
    conn.execute("""
    CREATE TABLE IF NOT EXISTS notification_history(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        point_name TEXT,
        notification_type TEXT,
        recipients TEXT,
        subject TEXT,
        body TEXT,
        mmph REAL,
        threshold_type TEXT,
        sent_at TEXT DEFAULT (datetime('now'))
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_notification_point ON notification_history(point_name, sent_at)")
    
    return conn

def fetch_latest_timestamp(conn: sqlite3.Connection) -> Optional[datetime]:
    """最新のデータ取得時刻をJSTで取得"""
    row = conn.execute("SELECT MAX(datetime(created_at)) AS ts FROM nowcast").fetchone()
    if row and row["ts"]:
        try:
            # UTCとして読み込んでJSTに変換
            utc_dt = datetime.fromisoformat(row["ts"])
            return utc_dt.replace(tzinfo=timezone.utc).astimezone(JST)
        except Exception:
            pass
    return None

def fetch_forecast_cards(conn: sqlite3.Connection, point_name: str, leads: List[int]) -> Dict[int, Optional[float]]:
    """最新の予測値を取得"""
    result: Dict[int, Optional[float]] = {m: None for m in leads}
    
    for lead in leads:
        row = conn.execute("""
            SELECT mmph FROM nowcast 
            WHERE point_name = ? AND lead_min = ?
            ORDER BY datetime(validtime) DESC LIMIT 1
        """, (point_name, lead)).fetchone()
        
        if row:
            result[lead] = float(row["mmph"])
    
    return result

def fetch_notification_history(conn: sqlite3.Connection, days: int = 7) -> pd.DataFrame:
    """通知履歴を取得"""
    cutoff = (datetime.now(JST) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    df = pd.read_sql_query("""
        SELECT point_name, notification_type, recipients, subject, 
               mmph, threshold_type, sent_at
        FROM notification_history
        WHERE datetime(sent_at) >= datetime(?)
        ORDER BY datetime(sent_at) DESC
    """, conn, params=(cutoff,))
    
    if not df.empty:
        df["sent_at"] = pd.to_datetime(df["sent_at"])
    return df

def read_heartbeat() -> Dict[str, Any]:
    hb_path = os.path.join("logs", "monitor_heartbeat.json")
    try:
        with open(hb_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

# ---------- シンプルなスタイル ----------
def inject_simple_css():
    st.markdown("""
    <style>
    /* フォント設定 */
    html, body, [class*="css"] {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", "Hiragino Sans", "Yu Gothic", sans-serif;
    }
    
    /* ヘッダー */
    .header-container {
        background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
    }
    
    .header-title {
        font-size: 1.8rem;
        font-weight: 700;
        margin-bottom: 0.75rem;
    }
    
    .header-stats {
        display: flex;
        gap: 1.5rem;
        flex-wrap: wrap;
    }
    
    .stat-item {
        background: rgba(255,255,255,0.15);
        padding: 0.5rem 1rem;
        border-radius: 8px;
        font-size: 0.9rem;
    }
    
    /* 降水量カード - シンプル版 */
    .rain-card {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 1rem;
        text-align: center;
        transition: all 0.2s ease;
        height: 100%;
    }
    
    .rain-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    }
    
    .rain-time {
        font-size: 0.85rem;
        color: #6b7280;
        margin-bottom: 0.5rem;
        font-weight: 500;
    }
    
    .rain-value {
        font-size: 1.75rem;
        font-weight: 700;
        margin: 0.25rem 0;
        line-height: 1;
    }
    
    .rain-unit {
        font-size: 0.8rem;
        color: #9ca3af;
    }
    
    /* 安全レベル別の色分け */
    .rain-card.safe {
        border-color: #10b981;
        background: #f0fdf4;
    }
    .rain-card.safe .rain-value {
        color: #10b981;
    }
    
    .rain-card.warn {
        border-color: #f59e0b;
        background: #fffbeb;
    }
    .rain-card.warn .rain-value {
        color: #f59e0b;
    }
    
    .rain-card.danger {
        border-color: #ef4444;
        background: #fef2f2;
    }
    .rain-card.danger .rain-value {
        color: #ef4444;
    }
    
    .rain-card.nodata {
        border-color: #d1d5db;
        background: #f9fafb;
    }
    .rain-card.nodata .rain-value {
        color: #9ca3af;
    }
    
    /* 地点セクション */
    .location-section {
        background: white;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
    }
    
    .location-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }
    
    .location-title {
        font-size: 1.25rem;
        font-weight: 600;
        color: #1f2937;
    }
    
    .location-meta {
        color: #6b7280;
        font-size: 0.85rem;
    }
    
    /* テーブルスタイル */
    .simple-table {
        width: 100%;
        border-collapse: collapse;
    }
    
    .simple-table th {
        background: #f9fafb;
        padding: 0.75rem;
        text-align: left;
        font-weight: 600;
        color: #374151;
        border-bottom: 1px solid #e5e7eb;
    }
    
    .simple-table td {
        padding: 0.75rem;
        border-bottom: 1px solid #f3f4f6;
    }
    
    /* バッジ */
    .badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.8rem;
        font-weight: 500;
    }
    
    .badge.success {
        background: #d1fae5;
        color: #065f46;
    }
    
    .badge.warning {
        background: #fed7aa;
        color: #92400e;
    }
    
    .badge.error {
        background: #fee2e2;
        color: #991b1b;
    }
    
    /* ボタン調整 */
    .stButton > button {
        border-radius: 8px;
        font-weight: 500;
    }
    </style>
    """, unsafe_allow_html=True)

# ---------- メインUI ----------
st.set_page_config(
    page_title="降水監視システム",
    page_icon="🌧️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# スタイル適用（戻り値を変数に格納して表示を防ぐ）
_ = inject_simple_css()

cfg = load_config()
conn = connect_db(cfg["storage"]["sqlite_path"])

# メタリフレッシュ（60秒）
_ = st.markdown("<meta http-equiv='refresh' content='60'>", unsafe_allow_html=True)

# ---------- ヘッダー ----------
latest_ts = fetch_latest_timestamp(conn)
hb = read_heartbeat()
ok = hb.get("ok", False)
last_run = hb.get("last_run")

# JSTで表示
latest_ts_str = latest_ts.strftime('%Y-%m-%d %H:%M:%S') if latest_ts else '—'

header_html = f"""
<div class='header-container'>
    <div class='header-title'>🌧️ 降水監視システム</div>
    <div class='header-stats'>
        <div class='stat-item'>
            📊 最終更新: {latest_ts_str}
        </div>
        <div class='stat-item'>
            {'✅' if ok else '⚠️'} システム: {'稼働中' if ok else '停止中'}
        </div>
        <div class='stat-item'>
            🔄 最終チェック: {last_run or '—'}
        </div>
    </div>
</div>
"""
_ = st.markdown(header_html, unsafe_allow_html=True)

# ---------- 雨雲レーダー（ズーム調整可能） ----------
with st.expander("🗾 雨雲レーダー（気象庁 高解像度降水ナウキャスト）", expanded=True):
    # ズームレベル選択
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        zoom_level = st.slider("ズームレベル", min_value=5, max_value=10, value=8, help="地図の拡大率を調整")
    
    leaflet_html = f"""
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <div id="map" style="width:100%;height:450px;border-radius:12px;overflow:hidden;border:1px solid #e5e7eb;"></div>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script>
    (async () => {{
        const locations = {json.dumps(cfg.get("locations", []))};
        const center = locations.length > 0 
            ? [locations[0].lat, locations[0].lon]
            : [35.0, 137.0];
        
        const map = L.map('map', {{
            zoomControl: true,
            scrollWheelZoom: true
        }}).setView(center, {zoom_level});
        
        // 地理院地図（淡色）
        L.tileLayer('https://cyberjapandata.gsi.go.jp/xyz/pale/{{z}}/{{x}}/{{y}}.png', {{
            maxZoom: 18,
            attribution: '© GSI Japan'
        }}).addTo(map);
        
        try {{
            // JMAナウキャストデータ取得
            const n1 = await fetch('https://www.jma.go.jp/bosai/jmatile/data/nowc/targetTimes_N1.json')
                .then(r => r.json());
            
            let basetime, validtime;
            if (Array.isArray(n1) && n1.length > 0) {{
                if (typeof n1[0] === 'string') {{
                    basetime = n1[0];
                    validtime = n1[0];
                }} else if (n1[0].basetime) {{
                    basetime = n1[0].basetime;
                    validtime = n1[0].validtime;
                }}
            }}
            
            if (basetime && validtime) {{
                // 降水強度レイヤー
                const jmaUrl = `https://www.jma.go.jp/bosai/jmatile/data/nowc/${{basetime}}/none/${{validtime}}/surf/hrpns/{{z}}/{{x}}/{{y}}.png`;
                L.tileLayer(jmaUrl, {{
                    opacity: 0.7,
                    maxZoom: 15,
                    attribution: '© JMA'
                }}).addTo(map);
            }}
            
            // 監視地点をマーカーで表示
            locations.forEach(loc => {{
                if (loc.enabled !== false) {{
                    const marker = L.circleMarker([loc.lat, loc.lon], {{
                        radius: 8,
                        fillColor: '#4f46e5',
                        color: '#fff',
                        weight: 2,
                        opacity: 1,
                        fillOpacity: 0.8
                    }}).addTo(map);
                    
                    marker.bindPopup(`
                        <div style="min-width:150px">
                            <b>${{loc.name}}</b><br>
                            緯度: ${{loc.lat.toFixed(6)}}<br>
                            経度: ${{loc.lon.toFixed(6)}}
                        </div>
                    `);
                }}
            }});
            
        }} catch(e) {{
            console.error('JMAデータ取得エラー:', e);
        }}
    }})();
    </script>
    """
    components.html(leaflet_html, height=470, scrolling=False)

# ---------- タブ ----------
tabs = st.tabs(["📊 現在の状況", "⚙️ 地点設定", "📧 通知設定", "📜 通知履歴", "🔧 システム管理"])

# 📊 現在の状況
with tabs[0]:
    leads = cfg.get("leads", [0, 15, 30, 45, 60])
    
    # 各地点の表示
    locations = cfg.get("locations", [])
    if not locations:
        st.warning("監視地点が設定されていません。「地点設定」タブから追加してください。")
    else:
        for loc in locations:
            if not loc.get("enabled", True):
                continue
            
            name = loc.get("name", "(無名)")
            lat = loc.get("lat", 0)
            lon = loc.get("lon", 0)
            
            # 地点別閾値の取得
            if loc.get("thresholds"):
                heavy = loc["thresholds"].get("heavy_rain", 30)
                torrential = loc["thresholds"].get("torrential_rain", 50)
            else:
                heavy = cfg["thresholds"]["heavy_rain"]
                torrential = cfg["thresholds"]["torrential_rain"]
            
            # 地点セクション
            with st.container():
                st.markdown(f"""
                <div class='location-section'>
                    <div class='location-header'>
                        <div class='location-title'>📍 {name}</div>
                        <div class='location-meta'>
                            {lat:.6f}, {lon:.6f} | 
                            閾値: 強い雨 {heavy:.0f}mm/h / 激しい雨 {torrential:.0f}mm/h
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            # 予測値カード（シンプル版）
            cols = st.columns(len(leads))
            cards = fetch_forecast_cards(conn, name, leads)
            
            for i, lead in enumerate(leads):
                v = cards.get(lead)
                
                if v is None:
                    cls = "nodata"
                    time_label = f"{lead}分後" if lead > 0 else "現在"
                    value_text = "—"
                    unit = ""
                else:
                    if v >= torrential:
                        cls = "danger"
                    elif v >= heavy:
                        cls = "warn"
                    else:
                        cls = "safe"
                    time_label = f"{lead}分後" if lead > 0 else "現在"
                    value_text = f"{v:.1f}"
                    unit = "mm/h"
                
                with cols[i]:
                    st.markdown(f"""
                    <div class='rain-card {cls}'>
                        <div class='rain-time'>{time_label}</div>
                        <div class='rain-value'>{value_text}</div>
                        <div class='rain-unit'>{unit}</div>
                    </div>
                    """, unsafe_allow_html=True)
            
            st.markdown("---")

# ⚙️ 地点設定
with tabs[1]:
    st.header("📍 監視地点の設定")
    
    with st.form("location_settings", clear_on_submit=False):
        st.subheader("監視地点一覧")
        
        # 既存地点の編集
        locations = cfg.get("locations", [])
        updated_locations = []
        
        for idx, loc in enumerate(locations):
            with st.expander(f"{loc.get('name', '無名')} {'(無効)' if not loc.get('enabled', True) else ''}", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    name = st.text_input("地点名", value=loc.get("name", ""), key=f"name_{idx}")
                    lat = st.number_input("緯度", value=float(loc.get("lat", 35.0)), format="%.6f", key=f"lat_{idx}")
                    lon = st.number_input("経度", value=float(loc.get("lon", 135.0)), format="%.6f", key=f"lon_{idx}")
                    enabled = st.checkbox("この地点を有効にする", value=loc.get("enabled", True), key=f"enabled_{idx}")
                
                with col2:
                    email = st.text_input("通知先メール（カンマ区切り）", value=loc.get("email_to", ""), key=f"email_{idx}")
                    use_custom = st.checkbox("地点別閾値を使用", value=bool(loc.get("thresholds")), key=f"custom_{idx}")
                    
                    if use_custom:
                        heavy = st.number_input("強い雨 (mm/h)", 1, 200, 
                                               value=loc.get("thresholds", {}).get("heavy_rain", 30), 
                                               key=f"heavy_{idx}")
                        torrential = st.number_input("激しい雨 (mm/h)", 1, 200,
                                                    value=loc.get("thresholds", {}).get("torrential_rain", 50),
                                                    key=f"torr_{idx}")
                        thresholds = {"heavy_rain": heavy, "torrential_rain": torrential}
                    else:
                        thresholds = None
                
                updated_locations.append({
                    "name": name,
                    "lat": lat,
                    "lon": lon,
                    "email_to": email.strip(),
                    "thresholds": thresholds,
                    "enabled": enabled
                })
        
        # 新規地点追加
        st.subheader("新規地点を追加")
        new_name = st.text_input("地点名", key="new_name")
        col1, col2 = st.columns(2)
        with col1:
            new_lat = st.number_input("緯度", value=35.0, format="%.6f", key="new_lat")
        with col2:
            new_lon = st.number_input("経度", value=135.0, format="%.6f", key="new_lon")
        new_email = st.text_input("通知先メール（カンマ区切り）", key="new_email")
        
        if st.form_submit_button("💾 設定を保存", use_container_width=True, type="primary"):
            if new_name:
                updated_locations.append({
                    "name": new_name,
                    "lat": new_lat,
                    "lon": new_lon,
                    "email_to": new_email.strip(),
                    "thresholds": None,
                    "enabled": True
                })
            
            cfg["locations"] = updated_locations
            save_config(cfg)
            st.success("✅ 地点設定を保存しました")
            st.rerun()

# 📧 通知設定
with tabs[2]:
    st.header("📧 通知設定")
    
    with st.form("notification_settings", clear_on_submit=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("基本設定")
            noti_enabled = st.checkbox(
                "通知機能を有効にする",
                value=cfg["notification"]["enabled"]
            )
            
            cooldown = st.slider(
                "通知間隔（分）",
                min_value=5,
                max_value=120,
                value=cfg["notification"].get("cooldown_minutes", 30),
                step=5,
                help="同一地点への連続通知を防ぐための最小間隔"
            )
            
            st.subheader("既定の閾値")
            st.caption("地点別設定がない場合の標準値")
            global_heavy = st.number_input(
                "強い雨 (mm/h)",
                min_value=1,
                max_value=200,
                value=cfg["thresholds"]["heavy_rain"]
            )
            global_torr = st.number_input(
                "激しい雨 (mm/h)",
                min_value=1,
                max_value=200,
                value=cfg["thresholds"]["torrential_rain"]
            )
        
        with col2:
            st.subheader("管理者設定")
            admin_email = st.text_input(
                "管理者メールアドレス",
                value=cfg["notification"].get("admin_email", ""),
                help="システム稼働レポートの送信先"
            )
            
            # 通知時刻の選択
            times = cfg["notification"].get("admin_notification_times", ["09:00", "17:00"])
            time_options = ["06:00", "07:00", "08:00", "09:00", "10:00", "11:00", "12:00",
                           "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00", "20:00", "21:00"]
            selected_times = st.multiselect(
                "稼働レポート送信時刻",
                options=time_options,
                default=times,
                help="管理者への定期レポート送信時刻"
            )
            
            st.subheader("Outlook設定")
            outlook_enabled = st.checkbox(
                "Windows Outlookを使用",
                value=cfg["notification"]["outlook"]["enabled"]
            )
            
            importance = st.selectbox(
                "メール重要度",
                options=["Low", "Normal", "High"],
                index=["Low", "Normal", "High"].index(cfg["notification"]["outlook"].get("importance", "Normal"))
            )
        
        if st.form_submit_button("💾 通知設定を保存", use_container_width=True, type="primary"):
            cfg["notification"]["enabled"] = noti_enabled
            cfg["notification"]["cooldown_minutes"] = cooldown
            cfg["notification"]["admin_email"] = admin_email.strip()
            cfg["notification"]["admin_notification_times"] = selected_times
            cfg["notification"]["outlook"]["enabled"] = outlook_enabled
            cfg["notification"]["outlook"]["importance"] = importance
            cfg["thresholds"]["heavy_rain"] = global_heavy
            cfg["thresholds"]["torrential_rain"] = global_torr
            save_config(cfg)
            st.success("✅ 通知設定を保存しました")
            st.rerun()

# 📜 通知履歴
with tabs[3]:
    st.header("📜 通知履歴")
    
    # フィルター
    col1, col2, col3 = st.columns(3)
    with col1:
        days_filter = st.selectbox("表示期間", options=[1, 3, 7, 14, 30], index=2)
    with col2:
        type_filter = st.selectbox("種類", 
                                   options=["すべて", "降水アラート", "稼働レポート"],
                                   index=0)
    with col3:
        if st.button("🔄 更新", use_container_width=True):
            st.rerun()
    
    # 履歴データ取得
    history_df = fetch_notification_history(conn, days=days_filter)
    
    if history_df.empty:
        st.info("通知履歴がありません")
    else:
        # フィルタリング
        if type_filter == "降水アラート":
            history_df = history_df[history_df["notification_type"] == "threshold_alert"]
        elif type_filter == "稼働レポート":
            history_df = history_df[history_df["notification_type"] == "admin_heartbeat"]
        
        # 統計
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("総通知数", len(history_df))
        with col2:
            alert_count = len(history_df[history_df["notification_type"] == "threshold_alert"])
            st.metric("アラート数", alert_count)
        with col3:
            if not history_df.empty and "mmph" in history_df.columns:
                max_mmph = history_df["mmph"].max()
                st.metric("最大降水量", f"{max_mmph:.1f} mm/h" if pd.notna(max_mmph) else "—")
            else:
                st.metric("最大降水量", "—")
        with col4:
            locations_notified = history_df["point_name"].nunique()
            st.metric("通知地点数", locations_notified)
        
        # 履歴テーブル
        st.subheader("詳細履歴")
        
        # 表示用にフォーマット
        display_df = history_df.copy()
        
        # JSTとして表示
        display_df["sent_at"] = pd.to_datetime(display_df["sent_at"]).dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')
        display_df["sent_at"] = display_df["sent_at"].dt.strftime("%Y-%m-%d %H:%M")
        
        display_df["mmph"] = display_df["mmph"].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "—")
        
        # タイプを日本語に変換
        type_map = {
            "threshold_alert": "降水アラート",
            "admin_heartbeat": "稼働レポート"
        }
        display_df["notification_type"] = display_df["notification_type"].map(type_map).fillna("その他")
        
        # 閾値タイプを日本語に
        threshold_map = {
            "heavy": "強い雨",
            "torrential": "激しい雨"
        }
        if "threshold_type" in display_df.columns:
            display_df["threshold_type"] = display_df["threshold_type"].map(threshold_map).fillna("—")
        
        # カラム名を日本語に
        display_df = display_df.rename(columns={
            "sent_at": "送信日時",
            "point_name": "地点",
            "notification_type": "種類",
            "threshold_type": "レベル",
            "mmph": "降水量(mm/h)",
            "recipients": "送信先"
        })
        
        # 表示カラムを選択
        display_cols = ["送信日時", "地点", "種類", "レベル", "降水量(mm/h)", "送信先"]
        display_cols = [col for col in display_cols if col in display_df.columns]
        
        st.dataframe(
            display_df[display_cols],
            use_container_width=True,
            hide_index=True,
            height=400
        )

# 🔧 システム管理
with tabs[4]:
    st.header("🔧 システム管理")
    
    # システム状態
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("システム状態")
        hb = read_heartbeat()
        
        if hb.get("ok"):
            st.success("✅ システムは正常に稼働しています")
        else:
            st.warning("⚠️ システムに問題がある可能性があります")
        
        st.metric("最終実行", hb.get("last_run", "—"))
        st.metric("状態", "正常" if hb.get("ok") else "異常")
        if hb.get("error"):
            st.error(f"エラー: {hb['error']}")
    
    with col2:
        st.subheader("データベース情報")
        
        with conn:
            # データ件数
            data_count = conn.execute("SELECT COUNT(*) FROM nowcast").fetchone()[0]
            st.metric("観測データ数", f"{data_count:,}")
            
            # 通知件数
            noti_count = conn.execute("SELECT COUNT(*) FROM notification_history").fetchone()[0]
            st.metric("通知履歴数", f"{noti_count:,}")
            
            # ストレージサイズ
            if os.path.exists(cfg["storage"]["sqlite_path"]):
                size_mb = os.path.getsize(cfg["storage"]["sqlite_path"]) / (1024 * 1024)
                st.metric("DBサイズ", f"{size_mb:.2f} MB")
    
    st.divider()
    
    # システム設定
    st.subheader("システム設定")
    
    with st.form("system_settings", clear_on_submit=False):
        col1, col2 = st.columns(2)
        
        with col1:
            monitoring_enabled = st.checkbox(
                "監視を有効にする",
                value=cfg["monitoring"]["enabled"]
            )
            
            interval = st.number_input(
                "データ収集間隔（分）",
                min_value=1,
                max_value=60,
                value=cfg["monitoring"]["interval_minutes"],
                help="気象データの更新頻度"
            )
            
            leads = st.multiselect(
                "予測時間（分後）",
                options=[0, 15, 30, 45, 60, 75, 90],
                default=cfg.get("leads", [0, 15, 30, 45, 60]),
                help="表示する予測時間"
            )
        
        with col2:
            retention = st.number_input(
                "データ保持期間（日）",
                min_value=1,
                max_value=365,
                value=cfg["storage"]["retention_days"],
                help="古いデータの自動削除"
            )
            
            debug_mode = st.checkbox(
                "デバッグモード",
                value=cfg.get("debug", False),
                help="詳細なログを出力"
            )
            
            suppress_warn = st.checkbox(
                "警告を抑制",
                value=cfg["log"].get("suppress_warn", True),
                help="ログの警告メッセージを減らす"
            )
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.form_submit_button("💾 システム設定を保存", use_container_width=True, type="primary"):
                cfg["monitoring"]["enabled"] = monitoring_enabled
                cfg["monitoring"]["interval_minutes"] = interval
                cfg["leads"] = sorted(leads)
                cfg["storage"]["retention_days"] = retention
                cfg["debug"] = debug_mode
                cfg["log"]["suppress_warn"] = suppress_warn
                save_config(cfg)
                st.success("✅ システム設定を保存しました")
                st.rerun()
    
    st.divider()
    
    # データ管理
    st.subheader("データ管理")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🗑️ 古いデータを削除", use_container_width=True):
            try:
                days = cfg["storage"]["retention_days"]
                cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
                
                with conn:
                    # 削除前のカウント
                    before_nowcast = conn.execute("SELECT COUNT(*) FROM nowcast").fetchone()[0]
                    before_history = conn.execute("SELECT COUNT(*) FROM notification_history").fetchone()[0]
                    
                    # 削除実行
                    conn.execute("DELETE FROM nowcast WHERE datetime(validtime) < datetime(?)", (cutoff,))
                    conn.execute("DELETE FROM notification_history WHERE datetime(sent_at) < datetime(?)", (cutoff,))
                    conn.commit()
                    
                    # 削除後のカウント
                    after_nowcast = conn.execute("SELECT COUNT(*) FROM nowcast").fetchone()[0]
                    after_history = conn.execute("SELECT COUNT(*) FROM notification_history").fetchone()[0]
                    
                    deleted_nowcast = before_nowcast - after_nowcast
                    deleted_history = before_history - after_history
                    
                st.success(f"削除完了: 観測データ {deleted_nowcast}件, 通知履歴 {deleted_history}件")
            except Exception as e:
                st.error(f"削除エラー: {e}")
    
    with col2:
        if st.button("🔄 データベース最適化", use_container_width=True):
            try:
                with conn:
                    conn.execute("VACUUM")
                st.success("データベースを最適化しました")
            except Exception as e:
                st.error(f"最適化エラー: {e}")
    
    with col3:
        if st.button("📋 ログをクリア", use_container_width=True):
            try:
                log_file = os.path.join("logs", "monitor.log")
                if os.path.exists(log_file):
                    # 最新100行を残す
                    with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                        lines = f.readlines()
                    
                    with open(log_file, "w", encoding="utf-8") as f:
                        f.writelines(lines[-100:] if len(lines) > 100 else lines)
                    
                    st.success("古いログをクリアしました（最新100行を保持）")
                else:
                    st.info("ログファイルがありません")
            except Exception as e:
                st.error(f"クリアエラー: {e}")
    
    st.divider()
    
    # ログ表示
    st.subheader("📋 システムログ（最新50行）")
    
    log_file = os.path.join("logs", "monitor.log")
    if os.path.exists(log_file):
        try:
            with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()[-50:]
                
                # エラー/警告をハイライト
                formatted_lines = []
                for line in lines:
                    if "[ERROR]" in line:
                        formatted_lines.append(f"🔴 {line.strip()}")
                    elif "[WARN]" in line:
                        formatted_lines.append(f"🟡 {line.strip()}")
                    elif "[通知]" in line or "[管理者通知]" in line:
                        formatted_lines.append(f"📧 {line.strip()}")
                    else:
                        formatted_lines.append(line.strip())
                
                log_text = "\n".join(formatted_lines) if formatted_lines else "(ログなし)"
        except Exception:
            log_text = "(ログ読み込みエラー)"
    else:
        log_text = "(ログファイルなし)"
    
    st.text_area("", value=log_text, height=300, disabled=True, label_visibility="collapsed")
    
    # 詳細ログダウンロード
    if os.path.exists(log_file):
        with open(log_file, "rb") as f:
            st.download_button(
                label="📥 完全なログをダウンロード",
                data=f,
                file_name=f"monitor_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                mime="text/plain"
            )

# ---------- フッター ----------
st.markdown("---")
st.caption("降水監視システム v3.0 | データソース: 気象庁 高解像度降水ナウキャスト")