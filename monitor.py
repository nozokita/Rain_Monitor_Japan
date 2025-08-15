#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
monitor.py – 改善版：正確な予測ロジック＆地点別メール通知対応
- 2つ目のコードの優れた予測ロジック（N1/N2適切選択）を採用
- 各地点に個別の通知先メールアドレスと閾値を設定可能
- 管理者へ9時/17時に稼働状況を自動通知
- Windows Outlook専用のメール送信実装
- 通知履歴の記録と重複防止機構（30分クールダウン）
"""

import os, sys, json, time, math, sqlite3, atexit, signal, argparse
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Dict, Any, Tuple, Optional, List, Set

# 依存
try:
    import requests
    from requests.adapters import HTTPAdapter
    try:
        from urllib3.util.retry import Retry
    except Exception:
        Retry = None
    from PIL import Image
except Exception as e:
    print("[ERROR] 必要なパッケージがありません。次を実行してください:", file=sys.stderr)
    print("  python -m pip install requests pillow", file=sys.stderr)
    raise

# Windows Outlook
try:
    import win32com.client
    import pythoncom
    WINDOWS_EMAIL = True
except Exception:
    WINDOWS_EMAIL = False
    print("[INFO] Windows Outlook機能は無効です（pywin32未インストール）")

# ───────── 設定と既定 ─────────
JST = timezone(timedelta(hours=9))
SUPPRESS_WARN = True
LOCK_PATH = "logs/monitor.pid"

DEFAULT_CONFIG = {
    "locations": [
        {
            "name": "三島駅",
            "lat": 35.126474871810345,
            "lon": 138.91109391000256,
            "email_to": "",  # 地点別通知先（カンマ区切り）
            "thresholds": {  # 地点別閾値（オプション）
                "heavy_rain": 30,
                "torrential_rain": 50
            },
            "enabled": True  # 地点別の有効/無効
        }
    ],
    "monitoring": {
        "enabled": True,
        "interval_minutes": 5
    },
    "thresholds": {  # グローバル閾値（デフォルト）
        "heavy_rain": 30,
        "torrential_rain": 50
    },
    "notification": {
        "enabled": True,
        "admin_email": "",  # 管理者メールアドレス
        "admin_notification_times": ["09:00", "17:00"],  # 定期通知時刻
        "cooldown_minutes": 30,  # 同一地点への通知間隔（分）
        "outlook": {
            "enabled": True,
            "importance": "Normal"  # Normal/High/Low
        }
    },
    "storage": {
        "sqlite_path": "data/nowcast.sqlite",
        "retention_days": 3
    },
    "log": {"suppress_warn": True},
    "leads": [0, 15, 30, 45, 60],
    "debug": False
}

# ───────── JMA配色と変換 ─────────
STEP_TO_MM_IDENTITY = {a: a for a in range(1, 61)}
STEP_TO_MM_IDENTITY.update({61: 80, 62: 100, 63: 150, 64: 200, 65: 300})

def convert_step_to_mmh_jma_bins(step: int) -> float:
    if not isinstance(step, int) or step <= 0: return 0.0
    m = float(STEP_TO_MM_IDENTITY.get(step, 0.0))
    if m <= 0.0:  return 0.0
    if m <= 1.0:  return 1.0
    if m <= 5.0:  return 5.0
    if m <= 10.0: return 10.0
    if m <= 20.0: return 20.0
    if m <= 30.0: return 30.0
    if m <= 50.0: return 50.0
    if m <= 80.0: return 80.0
    return m

JMA_COLOR_BINS = {
    (242,242,255): 1.0,
    (160,210,255): 5.0,
    (33,140,255):  10.0,
    (0,65,255):    20.0,
    (250,245,0):   30.0,
    (255,153,0):   50.0,
    (255,40,0):    80.0,
    (180,0,104):   80.0,
}

def near_color_to_mmh(r:int, g:int, b:int, tol:int=2) -> Optional[float]:
    for (cr,cg,cb), rep in JMA_COLOR_BINS.items():
        if abs(r-cr)<=tol and abs(g-cg)<=tol and abs(b-cb)<=tol:
            return float(rep)
    return None

# ───────── データベース管理 ─────────
def ensure_db(path: str):
    """データベースとテーブルを初期化"""
    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
    with sqlite3.connect(path) as con:
        # 既存のnowcastテーブル
        con.execute("""
        CREATE TABLE IF NOT EXISTS nowcast(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            point_name TEXT,
            lat REAL, lon REAL,
            basetime TEXT,
            validtime TEXT,
            lead_min INTEGER,
            mmph REAL,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_nowcast_point_time ON nowcast(point_name, validtime, lead_min)")
        
        # 新規：通知履歴テーブル
        con.execute("""
        CREATE TABLE IF NOT EXISTS notification_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            point_name TEXT,
            notification_type TEXT,  -- 'threshold_alert' or 'admin_heartbeat'
            recipients TEXT,
            subject TEXT,
            body TEXT,
            mmph REAL,
            threshold_type TEXT,     -- 'heavy' or 'torrential'
            sent_at TEXT DEFAULT (datetime('now'))
        )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_notification_point ON notification_history(point_name, sent_at)")

def save_notification_history(path: str, point_name: str, noti_type: str, 
                            recipients: str, subject: str, body: str,
                            mmph: float = None, threshold_type: str = None):
    """通知履歴を保存"""
    with sqlite3.connect(path) as con:
        con.execute("""
            INSERT INTO notification_history(point_name, notification_type, recipients, 
                                           subject, body, mmph, threshold_type)
            VALUES(?,?,?,?,?,?,?)
        """, (point_name, noti_type, recipients, subject, body, mmph, threshold_type))
        con.commit()

def check_recent_notification(path: str, point_name: str, cooldown_minutes: int) -> bool:
    """指定時間内に同一地点への通知があったかチェック"""
    with sqlite3.connect(path) as con:
        cutoff = (datetime.now() - timedelta(minutes=cooldown_minutes)).strftime("%Y-%m-%d %H:%M:%S")
        cur = con.execute("""
            SELECT COUNT(*) as cnt FROM notification_history 
            WHERE point_name = ? AND notification_type = 'threshold_alert' 
                  AND datetime(sent_at) > datetime(?)
        """, (point_name, cutoff))
        return cur.fetchone()[0] > 0

def purge_old_rows(path: str, keep_days: int):
    """古いデータを削除"""
    with sqlite3.connect(path) as con:
        con.execute("DELETE FROM nowcast WHERE datetime(validtime) < datetime('now', ?)",
                   (f'-{int(keep_days)} days',))
        con.execute("DELETE FROM notification_history WHERE datetime(sent_at) < datetime('now', ?)",
                   (f'-{int(keep_days * 2)} days',))  # 通知履歴は2倍の期間保持
        con.commit()

def save_nowcast(path: str, point_name: str, lat: float, lon: float,
                basetime_utc: str, validtime_utc: str, lead_min: int, mmph: float):
    """観測データを保存"""
    vt_utc = datetime.strptime(validtime_utc, "%Y%m%d%H%M%S")
    vt_jst = vt_utc + timedelta(hours=9)
    vt_iso = vt_jst.strftime("%Y-%m-%d %H:%M:%S")
    
    with sqlite3.connect(path) as con:
        con.execute("""
            INSERT INTO nowcast(point_name,lat,lon,basetime,validtime,lead_min,mmph)
            VALUES(?,?,?,?,?,?,?)
        """, (point_name, lat, lon, basetime_utc, vt_iso, int(lead_min), float(mmph)))
        con.commit()

# ───────── Windows Outlook メール送信 ─────────
class OutlookMailer:
    """Windows Outlook COM APIを使用したメール送信"""
    
    def __init__(self, importance: str = "Normal"):
        if not WINDOWS_EMAIL:
            raise RuntimeError("Windows Outlook機能は利用できません")
        self.importance = importance
        
    def send(self, to_addresses: str, subject: str, body: str, is_html: bool = False) -> bool:
        """
        メール送信
        Args:
            to_addresses: セミコロン区切りのメールアドレス
            subject: 件名
            body: 本文
            is_html: HTML形式かどうか
        Returns:
            送信成功時True
        """
        try:
            pythoncom.CoInitialize()
            outlook = win32com.client.Dispatch("Outlook.Application")
            mail = outlook.CreateItem(0)  # 0 = Mail Item
            
            mail.To = to_addresses.replace(",", ";")  # Outlookはセミコロン区切り
            mail.Subject = subject
            
            if is_html:
                mail.HTMLBody = body
            else:
                mail.Body = body
                
            # 重要度設定
            if self.importance == "High":
                mail.Importance = 2
            elif self.importance == "Low":
                mail.Importance = 0
            else:
                mail.Importance = 1
                
            mail.Send()
            pythoncom.CoUninitialize()
            return True
            
        except Exception as e:
            log_message(f"[ERROR] Outlookメール送信失敗: {e}")
            try:
                pythoncom.CoUninitialize()
            except:
                pass
            return False

# ───────── 通知管理 ─────────
class NotificationManager:
    """通知の管理と送信"""
    
    def __init__(self, cfg: Dict[str, Any], db_path: str):
        self.cfg = cfg
        self.db_path = db_path
        self.mailer = None
        
        if WINDOWS_EMAIL and cfg["notification"].get("outlook", {}).get("enabled", True):
            self.mailer = OutlookMailer(
                importance=cfg["notification"]["outlook"].get("importance", "Normal")
            )
    
    def check_and_notify(self, point_name: str, location_cfg: Dict[str, Any], 
                         forecasts: Dict[int, float]):
        """閾値超過チェックと通知"""
        
        # 地点が無効な場合はスキップ
        if not location_cfg.get("enabled", True):
            return
        
        # 地点別またはグローバル閾値を取得
        if location_cfg.get("thresholds"):
            heavy = location_cfg["thresholds"].get("heavy_rain", 30)
            torrential = location_cfg["thresholds"].get("torrential_rain", 50)
        else:
            heavy = self.cfg["thresholds"]["heavy_rain"]
            torrential = self.cfg["thresholds"]["torrential_rain"]
        
        # 通知先取得
        recipients = location_cfg.get("email_to", "").strip()
        if not recipients or not self.mailer:
            return
            
        # クールダウンチェック
        cooldown = self.cfg["notification"].get("cooldown_minutes", 30)
        if check_recent_notification(self.db_path, point_name, cooldown):
            log_message(f"[{point_name}] 通知クールダウン中（{cooldown}分）")
            return
        
        # 最大値と発生時刻を特定
        max_mmph = 0
        max_lead = 0
        alert_triggered = False
        
        for lead, mmph in forecasts.items():
            if mmph and mmph > max_mmph:
                max_mmph = mmph
                max_lead = lead
                if mmph >= heavy:
                    alert_triggered = True
        
        if not alert_triggered:
            return
        
        # 閾値判定
        threshold_type = None
        alert_level = ""
        if max_mmph >= torrential:
            threshold_type = "torrential"
            alert_level = "【警報級】激しい雨"
        elif max_mmph >= heavy:
            threshold_type = "heavy"
            alert_level = "【注意】強い雨"
        
        # メール作成
        now_jst = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
        subject = f"[降水アラート] {point_name} - {alert_level}"
        
        body = f"""
降水監視システムからの自動通知

■ 観測地点: {point_name}
■ 座標: ({location_cfg['lat']:.6f}, {location_cfg['lon']:.6f})
■ 検出時刻: {now_jst}

■ アラート内容:
{alert_level}が予測されています。

■ 予測降水量:
"""
        for lead in sorted(forecasts.keys()):
            mmph = forecasts.get(lead)
            if mmph:
                time_str = "現在" if lead == 0 else f"{lead}分後"
                mark = ""
                if mmph >= torrential:
                    mark = " 🚨"
                elif mmph >= heavy:
                    mark = " ⚠️"
                body += f"  ・{time_str}: {mmph:.1f} mm/h{mark}\n"
        
        body += f"""
■ 最大降水量: {max_mmph:.1f} mm/h ({max_lead}分後)
■ 閾値設定: 強い雨 {heavy}mm/h, 激しい雨 {torrential}mm/h

このメールは自動送信されています。
次回通知まで最低{cooldown}分のクールダウン期間があります。
"""
        
        # 送信
        if self.mailer.send(recipients, subject, body):
            save_notification_history(
                self.db_path, point_name, "threshold_alert",
                recipients, subject, body, max_mmph, threshold_type
            )
            log_message(f"[通知] {point_name} へアラート送信: {recipients}")
    
    def send_admin_heartbeat(self):
        """管理者への定期通知"""
        admin_email = self.cfg["notification"].get("admin_email", "").strip()
        if not admin_email or not self.mailer:
            return
            
        now = datetime.now(JST)
        current_time = now.strftime("%H:%M")
        
        # 設定時刻チェック
        notification_times = self.cfg["notification"].get("admin_notification_times", ["09:00", "17:00"])
        if current_time not in notification_times:
            return
        
        # 既に送信済みかチェック（1時間以内）
        with sqlite3.connect(self.db_path) as con:
            cutoff = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            cur = con.execute("""
                SELECT COUNT(*) FROM notification_history
                WHERE notification_type = 'admin_heartbeat' AND datetime(sent_at) > datetime(?)
            """, (cutoff,))
            if cur.fetchone()[0] > 0:
                return
        
        # 稼働状況を集計
        with sqlite3.connect(self.db_path) as con:
            # 直近1時間のデータ数
            hour_ago = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            cur = con.execute("""
                SELECT point_name, COUNT(*) as cnt, MAX(mmph) as max_mmph
                FROM nowcast 
                WHERE datetime(created_at) > datetime(?)
                GROUP BY point_name
            """, (hour_ago,))
            location_stats = {row[0]: {"count": row[1], "max": row[2]} for row in cur.fetchall()}
            
            # 直近24時間のアラート数
            day_ago = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
            cur = con.execute("""
                SELECT COUNT(*) as cnt
                FROM notification_history
                WHERE notification_type = 'threshold_alert' AND datetime(sent_at) > datetime(?)
            """, (day_ago,))
            alerts_24h = cur.fetchone()[0]
        
        # メール作成
        subject = f"[降水監視] 定期稼働レポート - {now.strftime('%Y-%m-%d %H:%M')}"
        body = f"""
降水監視システム 定期稼働レポート

■ レポート生成時刻: {now.strftime('%Y-%m-%d %H:%M:%S')}
■ システム状態: 正常稼働中

■ 監視地点データ収集状況（過去1時間）:
"""
        
        active_count = 0
        for loc in self.cfg.get("locations", []):
            name = loc.get("name", "無名")
            enabled = loc.get("enabled", True)
            stats = location_stats.get(name, {"count": 0, "max": 0})
            
            if enabled and stats["count"] > 0:
                active_count += 1
                status = "✅ 正常"
                max_str = f"最大 {stats['max']:.1f}mm/h" if stats['max'] else ""
            elif enabled:
                status = "⚠️ データなし"
                max_str = ""
            else:
                status = "⏸️ 無効"
                max_str = ""
                
            body += f"  ・{name}: {stats['count']}件 {status} {max_str}\n"
        
        body += f"""

■ システム統計:
  ・有効な監視地点: {active_count}/{len(self.cfg.get('locations', []))}
  ・過去24時間のアラート送信数: {alerts_24h}件
  ・収集間隔: {self.cfg['monitoring']['interval_minutes']}分
  ・データ保持期間: {self.cfg['storage']['retention_days']}日

■ 次回定期レポート予定時刻:
  {', '.join(notification_times)}

このメールは管理者向け定期レポートです。
システムは正常に稼働しています。
"""
        
        # 送信
        if self.mailer.send(admin_email, subject, body):
            save_notification_history(
                self.db_path, "ADMIN", "admin_heartbeat",
                admin_email, subject, body
            )
            log_message(f"[管理者通知] 定期レポート送信: {admin_email}")

# ───────── ユーティリティ ─────────
def log_message(msg: str, also_print=True):
    os.makedirs("logs", exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    with open("logs/monitor.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")
    if also_print:
        print(line, flush=True)

def write_heartbeat(ok: bool, error: str = ""):
    try:
        os.makedirs("logs", exist_ok=True)
        with open("logs/monitor_heartbeat.json", "w", encoding="utf-8") as f:
            json.dump({
                "last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ok": bool(ok),
                "error": error or ""
            }, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log_message(f"[WARN] ハートビート書き込み失敗: {e}")

def load_config(path="config.json") -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        cfg = {}
    
    def deepmerge(a, b):
        for k, v in b.items():
            if isinstance(v, dict):
                a[k] = deepmerge(a.get(k, {}), v)
            elif isinstance(v, list):
                a.setdefault(k, v)
            else:
                a.setdefault(k, v)
        return a
    
    return deepmerge(cfg, DEFAULT_CONFIG.copy())

# ───────── JMA Nowcast API ─────────
class JMANowcastAPI:
    """2つ目のコードの優れた予測ロジックを採用"""
    BASE = "https://www.jma.go.jp/bosai/jmatile/data/nowc"
    
    def __init__(self, zoom=10):
        self.zoom = zoom
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "rain-monitor/2.0"})
        if Retry:
            adapter = HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.5, 
                                                    status_forcelist=(429,500,502,503,504)))
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        self._cache: Dict[str, Dict[str, Any]] = {}

    def _get_target_times(self, kind: str):
        """targetTimes取得（60秒キャッシュ）"""
        now = time.time()
        ent = self._cache.get(kind)
        if ent and now - ent["ts"] < 60:
            return ent["data"]
        url = f"{self.BASE}/targetTimes_{kind}.json"
        r = self.session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        self._cache[kind] = {"ts": now, "data": data}
        return data

    @staticmethod
    def _normalize(raw):
        """targetTimesを正規化"""
        out = []
        if isinstance(raw, list):
            for it in raw:
                if isinstance(it, dict) and "basetime" in it and "validtime" in it:
                    out.append({"basetime": it["basetime"], "validtime": it["validtime"]})
                elif isinstance(it, str):
                    out.append({"basetime": it, "validtime": it})
        out.sort(key=lambda x: x["validtime"], reverse=True)
        return out
    
    def get_latest_times_for_leads(self, leads: List[int]) -> Dict[int, Tuple[str, str]]:
        """
        各リード時間に対する最適なbasetime/validtimeペアを取得
        2つ目のコードの方式：lead=0はN1、lead>0はN2から選択
        """
        result = {}
        
        # N1取得（lead=0用）
        n1 = self._normalize(self._get_target_times("N1"))
        if n1:
            result[0] = (n1[0]["basetime"], n1[0]["validtime"])
        
        # N2取得（lead>0用）
        n2 = self._normalize(self._get_target_times("N2"))
        
        for lead in leads:
            if lead == 0:
                continue  # 既にN1で処理済み
                
            # 現在時刻からleadを足した時刻に最も近いvalidtimeを選択
            target_jst = datetime.now(JST) + timedelta(minutes=lead)
            target_utc = (target_jst - timedelta(hours=9)).strftime("%Y%m%d%H%M%S")
            
            best_match = None
            min_diff = float('inf')
            
            for item in n2:
                vt = item["validtime"]
                vt_dt = datetime.strptime(vt, "%Y%m%d%H%M%S")
                target_dt = datetime.strptime(target_utc, "%Y%m%d%H%M%S")
                diff = abs((vt_dt - target_dt).total_seconds())
                
                if diff < min_diff:
                    min_diff = diff
                    best_match = item
            
            if best_match:
                result[lead] = (best_match["basetime"], best_match["validtime"])
        
        return result

    def _deg2tile(self, lat, lon):
        z = self.zoom
        lat_rad = math.radians(lat)
        n = 2.0**z
        xtile = int((lon + 180.0) / 360.0 * n)
        ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return xtile, ytile

    def _pixel_in_tile(self, lat, lon):
        z = self.zoom
        lat_rad = math.radians(lat)
        n = 2.0**z
        fx = (lon + 180.0) / 360.0 * n
        fy = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n
        px = int((fx - math.floor(fx)) * 256)
        py = int((fy - math.floor(fy)) * 256)
        return px, py

    def _fetch_tile_png(self, basetime, validtime, x, y):
        patterns = [
            f"{self.BASE}/{basetime}/none/{validtime}/surf/hrpns/{self.zoom}/{x}/{y}.png",
            f"{self.BASE}/{basetime}/{validtime}/surf/hrpns/{self.zoom}/{x}/{y}.png",
            f"{self.BASE}/{basetime}/none/{validtime}/surf/rasrf/{self.zoom}/{x}/{y}.png",
        ]
        last_err = None
        for url in patterns:
            try:
                r = self.session.get(url, timeout=10)
                if r.status_code == 200:
                    return Image.open(BytesIO(r.content)), url
                elif r.status_code == 404:
                    continue
                else:
                    last_err = f"HTTP {r.status_code}"
            except Exception as e:
                last_err = str(e)
        raise RuntimeError(f"タイル取得失敗: {last_err}")

    @staticmethod
    def _alpha_at(img: Image.Image, x: int, y: int) -> int:
        return (img if img.mode == 'RGBA' else img.convert('RGBA')).getchannel('A').getpixel((x, y))

    @staticmethod
    def _rgb_at(img: Image.Image, x: int, y: int) -> Tuple[int, int, int]:
        return (img if img.mode == 'RGB' else img.convert('RGB')).getpixel((x, y))

    def _calc_step_at(self, img: Image.Image, x: int, y: int) -> int:
        a = self._alpha_at(img, x, y)
        if a == 0:
            return 0
        if img.mode == 'P':
            return int(img.getpixel((x, y)))
        return 1

    def _calc_step_in_window(self, img: Image.Image, px: int, py: int, size: int = 2) -> int:
        w, h = img.size
        half = size // 2
        sx = max(0, min(px - (half - 1), w - size))
        sy = max(0, min(py - (half - 1), h - size))
        m = 0
        for dx in range(size):
            for dy in range(size):
                x = sx + dx
                y = sy + dy
                s = self._calc_step_at(img, x, y)
                if s > m:
                    m = s
        return m

    def rainfall_mm_at(self, lat: float, lon: float, basetime: str, validtime: str, method="max_2x2"):
        xt, yt = self._deg2tile(lat, lon)
        px, py = self._pixel_in_tile(lat, lon)
        img, url = self._fetch_tile_png(basetime, validtime, xt, yt)

        # step 推定
        if method == "max_3x3":
            size = 3
        elif method == "max_4x4":
            size = 4
        elif method == "max_8x8":
            size = 8
        else:
            size = 2
        step = self._calc_step_in_window(img, px, py, size=size)

        # 色→代表値（優先）、ダメなら step→bins
        a = self._alpha_at(img, px, py)
        if a == 0:
            mmh = 0.0
        else:
            r, g, b = self._rgb_at(img, px, py)
            mmh_color = near_color_to_mmh(r, g, b, tol=2)
            mmh = mmh_color if (mmh_color is not None) else convert_step_to_mmh_jma_bins(step)

        vt_jst = datetime.strptime(validtime, "%Y%m%d%H%M%S") + timedelta(hours=9)
        return mmh, vt_jst, url, step

# ───────── 収集本体 ─────────
def run_once(cfg: Dict[str, Any]) -> None:
    global SUPPRESS_WARN
    SUPPRESS_WARN = bool(cfg.get("log", {}).get("suppress_warn", True))
    sqlite_path = cfg["storage"]["sqlite_path"]
    ensure_db(sqlite_path)
    purge_old_rows(sqlite_path, int(cfg["storage"].get("retention_days", 3)))

    api = JMANowcastAPI(zoom=10)
    leads = sorted(set(cfg.get("leads") or [0, 15, 30, 45, 60]))
    
    # 通知マネージャー初期化
    notifier = NotificationManager(cfg, sqlite_path)
    
    # 管理者への定期通知チェック
    notifier.send_admin_heartbeat()
    
    # 各リード時間に対する最適なbasetime/validtimeを取得
    times_for_leads = api.get_latest_times_for_leads(leads)
    
    if not times_for_leads:
        log_message("[WARN] targetTimesが空のためスキップ")
        return
    
    # 各地点を処理
    for loc in cfg.get("locations", []):
        if not loc.get("enabled", True):
            continue
            
        name = loc.get("name", "(無名)")
        lat = float(loc["lat"])
        lon = float(loc["lon"])
        
        saved = 0
        forecasts = {}
        
        for lead in leads:
            if lead not in times_for_leads:
                log_message(f"[WARN] {name} {lead}分後: 時刻情報なし")
                continue
                
            bt, vt = times_for_leads[lead]
            
            try:
                mmh, vt_jst, url, step = api.rainfall_mm_at(lat, lon, bt, vt, method="max_2x2")
                save_nowcast(sqlite_path, name, lat, lon, bt, vt, lead, mmh)
                forecasts[lead] = mmh
                saved += 1
                
                # ログ出力
                time_str = "現在" if lead == 0 else f"{lead}分後"
                log_message(f"[{name}] {time_str}: {mmh:.1f} mm/h (validtime: {vt_jst.strftime('%H:%M')})")
                
            except Exception as e:
                log_message(f"[WARN] {name} {lead}分後 保存失敗: {e}")
        
        log_message(f"[{name}] 保存完了: {saved}/{len(leads)} 件")
        
        # 閾値チェックと通知
        if cfg["notification"]["enabled"] and forecasts:
            notifier.check_and_notify(name, loc, forecasts)
    
    write_heartbeat(True, "")

# ───────── 常駐ループ/CLI ─────────
def main():
    parser = argparse.ArgumentParser(description="Nowcast monitor worker v2")
    parser.add_argument("--once", action="store_true", help="1回だけ収集して終了")
    parser.add_argument("--config", default="config.json", help="config.json のパス")
    parser.add_argument("--test-email", action="store_true", help="メール送信テスト")
    args = parser.parse_args()

    cfg = load_config(args.config)
    
    # メール送信テスト
    if args.test_email:
        print("=== メール送信テスト ===")
        if not WINDOWS_EMAIL:
            print("ERROR: Windows Outlook機能が利用できません")
            return
            
        admin_email = cfg["notification"].get("admin_email", "")
        if not admin_email:
            print("ERROR: 管理者メールアドレスが設定されていません")
            return
            
        mailer = OutlookMailer()
        subject = "[テスト] 降水監視システム - メール送信テスト"
        body = f"""
これはテストメールです。

送信時刻: {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')}
設定ファイル: {args.config}

メール送信機能は正常に動作しています。
"""
        
        if mailer.send(admin_email, subject, body):
            print(f"✅ テストメール送信成功: {admin_email}")
        else:
            print(f"❌ テストメール送信失敗")
        return
    
    print("=== monitor.py v2 起動 ===")
    print("Python:", sys.version)
    print("CWD   :", os.getcwd())
    print("CONFIG:", os.path.abspath(args.config))
    print("SQLite:", cfg["storage"]["sqlite_path"])
    print("Enabled:", cfg["monitoring"]["enabled"])
    print("Interval:", cfg["monitoring"]["interval_minutes"], "分")
    print("Outlook:", "有効" if WINDOWS_EMAIL else "無効")
    print("=======================")

    atexit.register(lambda: os.path.exists(LOCK_PATH) and os.remove(LOCK_PATH))
    os.makedirs("logs", exist_ok=True)
    with open(LOCK_PATH, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))

    if args.once:
        run_once(cfg)
        return

    while True:
        try:
            cfg = load_config(args.config)
            if cfg["monitoring"]["enabled"]:
                run_once(cfg)
                time.sleep(int(cfg["monitoring"]["interval_minutes"]) * 60)
            else:
                log_message("monitoring.enabled=False のため待機中")
                time.sleep(60)
        except KeyboardInterrupt:
            print("\nKeyboardInterrupt: 終了します。")
            break
        except Exception as e:
            log_message(f"[ERROR] ループエラー: {e}")
            write_heartbeat(False, str(e))
            time.sleep(60)

if __name__ == "__main__":
    main()