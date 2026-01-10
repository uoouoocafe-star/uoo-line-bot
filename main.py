# main.py
# UooUoo Cafe - LINE Dessert Order Bot (Stable All-in-One)
# - FastAPI webhook
# - Google Sheets: A/B (items rows), C (order summary), c_log (log)
# - Admin status buttons: PAID / READY / SHIPPED
# - Google Calendar create/update event (optional)
# - Menu from sheet "items" (you only edit the sheet in the future)
# - Cute buttons + cute cards
# - Includes your pickup/payment info (from your screenshot), with env override supported

from __future__ import annotations

import base64
import dataclasses
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

# Google
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# =============================================================================
# Env helpers (supports Render naming)
# =============================================================================

def env_first(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default

def env_int(*keys: str, default: int) -> int:
    raw = env_first(*keys, default=str(default)).strip()
    raw = raw.replace("(", "").replace(")", "")
    m = re.search(r"-?\d+", raw)
    return int(m.group(0)) if m else default

def env_csv(*keys: str) -> List[str]:
    s = env_first(*keys, default="")
    return [x.strip() for x in s.split(",") if x.strip()]


# =============================================================================
# LINE config
# =============================================================================

LINE_CHANNEL_ACCESS_TOKEN = env_first("LINE_CHANNEL_ACCESS_TOKEN", "CHANNEL_ACCESS_TOKEN", default="")
LINE_CHANNEL_SECRET = env_first("LINE_CHANNEL_SECRET", "CHANNEL_SECRET", default="")

LINE_API_BASE = "https://api.line.me/v2/bot"
LINE_REPLY_URL = f"{LINE_API_BASE}/message/reply"
LINE_PUSH_URL = f"{LINE_API_BASE}/message/push"
LINE_PROFILE_URL = f"{LINE_API_BASE}/profile"

REQ_TIMEOUT = 12  # seconds


# =============================================================================
# Admin
# =============================================================================

ADMIN_USER_IDS = set(env_csv("ADMIN_USER_IDS"))
ADMIN_TOKEN = env_first("ADMIN_TOKEN", default="")  # optional


# =============================================================================
# Google Sheets config (supports your env naming)
# =============================================================================

SPREADSHEET_ID = env_first("SPREADSHEET_ID", "GSHEET_ID", default="")

SHEET_A_NAME = env_first("SHEET_A_NAME", default="A")
SHEET_B_NAME = env_first("SHEET_B_NAME", default="B")
SHEET_C_NAME = env_first("SHEET_C_NAME", default="C")
SHEET_CLOG_NAME = env_first("SHEET_CLOG_NAME", default="c_log")

SHEET_ITEMS_NAME = env_first("SHEET_ITEMS_NAME", default="items")
SHEET_SETTINGS_NAME = env_first("SHEET_SETTINGS_NAME", default="settings")  # optional


# =============================================================================
# Google Service Account (Base64 supported)
# =============================================================================

GOOGLE_SERVICE_ACCOUNT_B64 = env_first("GOOGLE_SERVICE_ACCOUNT_B64", default="")
GOOGLE_SERVICE_ACCOUNT_JSON = env_first("GOOGLE_SERVICE_ACCOUNT_JSON", default="")
GOOGLE_SERVICE_ACCOUNT_FILE = env_first("GOOGLE_SERVICE_ACCOUNT_FILE", default="")


# =============================================================================
# Calendar (optional)
# =============================================================================

GCAL_CALENDAR_ID = env_first("GCAL_CALENDAR_ID", default="")
GCAL_TIMEZONE = env_first("GCAL_TIMEZONE", "TZ", default="Asia/Taipei")
ENABLE_CALENDAR = bool(GCAL_CALENDAR_ID)


# =============================================================================
# Rules
# =============================================================================

MIN_DAYS = env_int("MIN_DAYS", default=3)
MAX_DAYS = env_int("MAX_DAYS", default=14)

# closed weekdays: comma separated 0-6 (Mon=0)
CLOSED_WEEKDAYS = set()
for x in env_csv("CLOSED_WEEKDAYS"):
    try:
        CLOSED_WEEKDAYS.add(int(x))
    except:
        pass

# closed dates: comma separated YYYY-MM-DD
CLOSED_DATES = set(env_csv("CLOSED_DATES"))


# =============================================================================
# Store / payment info (env override, but defaults to your screenshot)
# =============================================================================

STORE_ADDRESS = env_first("STORE_ADDRESS", default="新竹縣竹北市隘口六街65號").strip()

BANK_NAME = env_first("BANK_NAME", default="台灣銀行").strip()
BANK_CORE = env_first("BANK_CORE", default="004").strip()
BANK_ACCOUNT = env_first("BANK_ACCOUNT", default="248-001-03430-6").strip()

# shipping
SHIP_PROVIDER = env_first("SHIP_PROVIDER", default="大榮").strip()
SHIP_FEE = env_int("SHIP_FEE", default=180)
SHIP_FREE_OVER = env_int("SHIP_FREE_OVER", default=2500)


# =============================================================================
# Basic sanity checks (do not crash on import)
# =============================================================================

if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_CHANNEL_SECRET:
    print("[WARN] Missing LINE channel token/secret env. Webhook will fail until env set.")

if not SPREADSHEET_ID:
    print("[WARN] Missing GSHEET_ID/SPREADSHEET_ID env. Sheets features will fail until set.")

if ENABLE_CALENDAR and not (GOOGLE_SERVICE_ACCOUNT_B64 or GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE):
    print("[WARN] Calendar enabled but service account env missing.")


# =============================================================================
# FastAPI
# =============================================================================

app = FastAPI(title="UooUoo Dessert Order Bot", version="stable-all-in-one")


# =============================================================================
# In-memory sessions (simple + stable)
# =============================================================================

@dataclasses.dataclass
class CartItem:
    item_id: str
    name: str
    unit_price: int
    qty: int = 1
    spec: str = ""
    flavor: str = ""
    min_qty: int = 1
    step_qty: int = 1

@dataclasses.dataclass
class Session:
    user_id: str
    display_name: str = ""

    cart: Dict[str, CartItem] = dataclasses.field(default_factory=dict)

    pickup_method: str = ""  # "店取" / "宅配"
    pickup_date: str = ""    # YYYY-MM-DD
    pickup_time: str = ""    # "12:00-14:00" etc (store pickup only)

    recipient: str = ""      # delivery
    phone: str = ""          # delivery
    address: str = ""        # delivery

    step: str = ""           # flow state
    last_active: float = dataclasses.field(default_factory=lambda: time.time())

SESSIONS: Dict[str, Session] = {}
SESSIONS_LOCK = threading.Lock()
SESSION_TTL = 60 * 60 * 6  # 6 hours

def get_session(user_id: str) -> Session:
    with SESSIONS_LOCK:
        s = SESSIONS.get(user_id)
        if not s:
            s = Session(user_id=user_id)
            SESSIONS[user_id] = s
        s.last_active = time.time()
        return s

def gc_sessions():
    now = time.time()
    with SESSIONS_LOCK:
        dead = [k for k, v in SESSIONS.items() if now - v.last_active > SESSION_TTL]
        for k in dead:
            del SESSIONS[k]


# =============================================================================
# LINE helpers
# =============================================================================

def verify_line_signature(body: bytes, x_line_signature: str) -> bool:
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, x_line_signature)

def line_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

def line_reply(reply_token: str, messages: List[Dict[str, Any]]) -> None:
    payload = {"replyToken": reply_token, "messages": messages}
    r = requests.post(LINE_REPLY_URL, headers=line_headers(), json=payload, timeout=REQ_TIMEOUT)
    if r.status_code >= 300:
        print("[LINE][reply] failed:", r.status_code, r.text)

def line_push(to_user_id: str, messages: List[Dict[str, Any]]) -> None:
    payload = {"to": to_user_id, "messages": messages}
    r = requests.post(LINE_PUSH_URL, headers=line_headers(), json=payload, timeout=REQ_TIMEOUT)
    if r.status_code >= 300:
        print("[LINE][push] failed:", r.status_code, r.text)

def push_to_admins(messages: List[Dict[str, Any]]) -> None:
    for uid in ADMIN_USER_IDS:
        try:
            line_push(uid, messages)
        except Exception as e:
            print("[ADMIN push] error:", e)

def get_profile(user_id: str) -> Dict[str, Any]:
    try:
        r = requests.get(f"{LINE_PROFILE_URL}/{user_id}", headers=line_headers(), timeout=REQ_TIMEOUT)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print("[LINE profile] error:", e)
    return {}

def is_admin(user_id: str) -> bool:
    return user_id in ADMIN_USER_IDS


# =============================================================================
# Google API (Sheets + Calendar) - lazy init
# =============================================================================

_GOOGLE_SERVICE = None
_GOOGLE_LOCK = threading.Lock()

def get_google_credentials() -> Credentials:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    if ENABLE_CALENDAR:
        scopes.append("https://www.googleapis.com/auth/calendar")

    if GOOGLE_SERVICE_ACCOUNT_B64:
        info = json.loads(base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64).decode("utf-8"))
        return Credentials.from_service_account_info(info, scopes=scopes)
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        return Credentials.from_service_account_info(info, scopes=scopes)
    if GOOGLE_SERVICE_ACCOUNT_FILE:
        return Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_FILE, scopes=scopes)

    raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_B64 / GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_SERVICE_ACCOUNT_FILE")

def get_google_services():
    global _GOOGLE_SERVICE
    with _GOOGLE_LOCK:
        if _GOOGLE_SERVICE is None:
            creds = get_google_credentials()
            sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
            cal = build("calendar", "v3", credentials=creds, cache_discovery=False) if ENABLE_CALENDAR else None
            _GOOGLE_SERVICE = (sheets, cal)
        return _GOOGLE_SERVICE


# -------------------- Sheets helpers --------------------

def sheets_read_range(sheet_name: str, a1: str) -> List[List[Any]]:
    sheets, _ = get_google_services()
    rng = f"{sheet_name}!{a1}"
    resp = sheets.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=rng).execute()
    return resp.get("values", [])

def sheets_write_range(sheet_name: str, a1: str, values: List[List[Any]]) -> None:
    sheets, _ = get_google_services()
    rng = f"{sheet_name}!{a1}"
    body = {"values": values}
    sheets.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=rng,
        valueInputOption="RAW",
        body=body,
    ).execute()

def sheets_append_row(sheet_name: str, row: List[Any]) -> None:
    sheets, _ = get_google_services()
    rng = f"{sheet_name}!A1"
    body = {"values": [row]}
    sheets.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=rng,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()

def sheets_get_sheet_titles() -> List[str]:
    sheets, _ = get_google_services()
    meta = sheets.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    return [sh["properties"]["title"] for sh in meta.get("sheets", [])]

def sheets_create_sheet_if_missing(sheet_name: str) -> None:
    titles = sheets_get_sheet_titles()
    if sheet_name in titles:
        return
    sheets, _ = get_google_services()
    req = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
    sheets.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=req).execute()

def ensure_headers(sheet_name: str, required_headers: List[str]) -> Dict[str, int]:
    """
    Ensure header row exists and contains required headers.
    Returns mapping: header -> column index (0-based)
    """
    sheets_create_sheet_if_missing(sheet_name)
    values = sheets_read_range(sheet_name, "1:1")
    header = values[0] if values else []
    header = [str(x).strip() for x in header]

    changed = False
    if not header:
        header = required_headers[:]
        changed = True
    else:
        for h in required_headers:
            if h not in header:
                header.append(h)
                changed = True

    if changed:
        sheets_write_range(sheet_name, "1:1", [header])

    return {h: i for i, h in enumerate(header)}

def col_to_a1(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def find_rows_by_value(sheet_name: str, header_map: Dict[str, int], key_header: str, key_value: str, max_scan: int = 5000) -> List[int]:
    if key_header not in header_map:
        return []
    col_idx = header_map[key_header]
    data = sheets_read_range(sheet_name, f"A2:ZZ{max_scan+1}")
    hits: List[int] = []
    for i, row in enumerate(data, start=2):
        if col_idx < len(row) and str(row[col_idx]).strip() == key_value:
            hits.append(i)
    return hits

def update_cells(sheet_name: str, row_num: int, updates: Dict[str, Any], header_map: Dict[str, int]) -> None:
    sheets, _ = get_google_services()
    reqs = []
    # ensure headers exist
    for header in list(updates.keys()):
        if header not in header_map:
            header_map = ensure_headers(sheet_name, list(header_map.keys()) + [header])

    for header, value in updates.items():
        col_idx = header_map[header]
        col_letters = col_to_a1(col_idx + 1)
        rng = f"{sheet_name}!{col_letters}{row_num}:{col_letters}{row_num}"
        reqs.append({"range": rng, "values": [[value]]})

    body = {"valueInputOption": "RAW", "data": reqs}
    sheets.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()


# -------------------- Calendar helpers --------------------

def ensure_calendar_event(
    order_id: str,
    pickup_method: str,
    pickup_date: str,
    pickup_time: str,
    note: str,
    existing_event_id: str = "",
) -> str:
    if not ENABLE_CALENDAR:
        return ""

    _, cal = get_google_services()
    if cal is None:
        return ""

    title = f"UooUoo 甜點訂單 {order_id}（{pickup_method}）"
    desc = note or ""

    try:
        date_obj = dt.date.fromisoformat(pickup_date)
    except Exception:
        return existing_event_id or ""

    def tz_dt(d: dt.date, hhmm: str) -> str:
        t = dt.datetime.combine(d, dt.time.fromisoformat(hhmm))
        return t.isoformat()

    event: Dict[str, Any] = {"summary": title, "description": desc}

    # 店取：用時段；宅配：全天（期望到貨日）
    if pickup_method == "店取" and pickup_time and "-" in pickup_time:
        start_s, end_s = pickup_time.split("-", 1)
        start_s = start_s.strip()
        end_s = end_s.strip()
        if re.match(r"^\d{2}:\d{2}$", start_s) and re.match(r"^\d{2}:\d{2}$", end_s):
            event["start"] = {"dateTime": tz_dt(date_obj, start_s), "timeZone": GCAL_TIMEZONE}
            event["end"] = {"dateTime": tz_dt(date_obj, end_s), "timeZone": GCAL_TIMEZONE}
        else:
            event["start"] = {"date": pickup_date}
            event["end"] = {"date": (date_obj + dt.timedelta(days=1)).isoformat()}
    else:
        event["start"] = {"date": pickup_date}
        event["end"] = {"date": (date_obj + dt.timedelta(days=1)).isoformat()}

    try:
        if existing_event_id:
            updated = cal.events().update(calendarId=GCAL_CALENDAR_ID, eventId=existing_event_id, body=event).execute()
            return updated.get("id", existing_event_id)
        created = cal.events().insert(calendarId=GCAL_CALENDAR_ID, body=event).execute()
        return created.get("id", "")
    except Exception as e:
        print("[GCAL] event error:", e)
        return existing_event_id or ""


# =============================================================================
# Menu / Items (from sheet "items" with fallback)
# =============================================================================

@dataclasses.dataclass
class MenuItem:
    item_id: str
    name: str
    price: int
    spec: str = ""
    flavor: str = ""
    enabled: bool = True
    min_qty: int = 1
    step_qty: int = 1

def _to_int(v: str, default: int) -> int:
    try:
        m = re.search(r"-?\d+", str(v))
        return int(m.group(0)) if m else default
    except:
        return default

def load_menu_items() -> List[MenuItem]:
    """
    Reads from SHEET_ITEMS_NAME with headers:
    item_id, name, price, spec, flavor, enabled, min_qty, step_qty
    """
    try:
        header = ensure_headers(
            SHEET_ITEMS_NAME,
            ["item_id", "name", "price", "spec", "flavor", "enabled", "min_qty", "step_qty"],
        )
        data = sheets_read_range(SHEET_ITEMS_NAME, "A2:ZZ500")
        out: List[MenuItem] = []

        for row in data:
            def g(h: str, default=""):
                idx = header.get(h, -1)
                return str(row[idx]).strip() if idx >= 0 and idx < len(row) else default

            item_id = g("item_id")
            name = g("name")
            if not item_id or not name:
                continue

            price = _to_int(g("price", "0"), 0)
            enabled = g("enabled", "TRUE").upper() not in ("FALSE", "0", "NO", "N")
            min_qty = max(1, _to_int(g("min_qty", "1"), 1))
            step_qty = max(1, _to_int(g("step_qty", "1"), 1))

            out.append(MenuItem(
                item_id=item_id,
                name=name,
                price=price,
                spec=g("spec"),
                flavor=g("flavor"),
                enabled=enabled,
                min_qty=min_qty,
                step_qty=step_qty,
            ))

        out = [x for x in out if x.enabled]
        if out:
            return out
    except Exception as e:
        print("[MENU] load failed:", e)

    # Fallback (if items sheet not ready yet)
    return [
        MenuItem(item_id="canele_box6", name="可麗露｜6顆/盒", price=490, spec="6顆/盒", flavor="", enabled=True, min_qty=1, step_qty=1),
        MenuItem(item_id="dacq_original", name="達克瓦茲｜原味", price=95, spec="至少 2 顆起", flavor="原味", enabled=True, min_qty=2, step_qty=1),
        MenuItem(item_id="scone_original", name="原味司康", price=65, spec="", flavor="原味", enabled=True, min_qty=1, step_qty=1),
        MenuItem(item_id="toast_original", name="伊思尼奶酥厚片｜原味", price=85, spec="", flavor="原味", enabled=True, min_qty=1, step_qty=1),
    ]

def get_menu_item(item_id: str) -> Optional[MenuItem]:
    for it in load_menu_items():
        if it.item_id == item_id:
            return it
    return None


# =============================================================================
# Cute Flex templates
# =============================================================================

THEME_BG = "#FFF7FB"
THEME_CARD = "#FFFFFF"
THEME_STROKE = "#F0D7E6"
THEME_TEXT = "#4A3B44"
THEME_SUB = "#7A6873"
BTN_PRIMARY = "#FF7FB0"
BTN_SECONDARY = "#FFE1EE"
BTN_TEXT = "#4A3B44"

def text_msg(text: str) -> Dict[str, Any]:
    return {"type": "text", "text": text}

def flex_menu(items: List[MenuItem]) -> Dict[str, Any]:
    cards: List[Dict[str, Any]] = []
    for it in items:
        qty_label = f"＋{it.min_qty}" if it.min_qty > 1 else "＋1"
        add_qty = it.min_qty

        badge = []
        if it.spec:
            badge.append({"type": "text", "text": it.spec, "size": "xs", "color": THEME_SUB, "wrap": True})
        if it.flavor:
            badge.append({"type": "text", "text": f"口味：{it.flavor}", "size": "xs", "color": THEME_SUB, "wrap": True})

        cards.append({
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "paddingAll": "14px",
            "backgroundColor": THEME_CARD,
            "cornerRadius": "16px",
            "borderWidth": "1px",
            "borderColor": THEME_STROKE,
            "contents": [
                {"type": "text", "text": it.name, "weight": "bold", "size": "md", "wrap": True, "color": THEME_TEXT},
                {"type": "text", "text": f"NT${it.price}", "size": "sm", "color": THEME_SUB},
                *badge,
                {
                    "type": "button",
                    "style": "primary",
                    "height": "sm",
                    "color": BTN_PRIMARY,
                    "action": {"type": "postback", "label": f"🧁 {qty_label} 加入", "data": f"PB:ADD|{it.item_id}|{add_qty}"},
                },
            ],
        })

    return {
        "type": "flex",
        "altText": "甜點選單",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "backgroundColor": THEME_BG,
                "contents": [
                    {"type": "text", "text": "UooUoo 甜點選單", "weight": "bold", "size": "xl", "color": THEME_TEXT},
                    {"type": "text", "text": "點「加入」放進購物車，再前往結帳。", "size": "sm", "color": THEME_SUB, "wrap": True},
                    {"type": "box", "layout": "vertical", "spacing": "sm", "contents": cards},
                ],
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "backgroundColor": THEME_BG,
                "contents": [
                    {
                        "type": "button",
                        "style": "primary",
                        "color": BTN_PRIMARY,
                        "action": {"type": "postback", "label": "🧾 前往結帳", "data": "PB:CHECKOUT"},
                    },
                    {
                        "type": "button",
                        "style": "secondary",
                        "color": BTN_SECONDARY,
                        "action": {"type": "postback", "label": "🧺 清空購物車", "data": "PB:CLEAR"},
                    },
                    {
                        "type": "button",
                        "style": "secondary",
                        "color": BTN_SECONDARY,
                        "action": {"type": "postback", "label": "📌 取貨說明", "data": "PB:INFO|PICKUP"},
                    },
                    {
                        "type": "button",
                        "style": "secondary",
                        "color": BTN_SECONDARY,
                        "action": {"type": "postback", "label": "💳 付款說明", "data": "PB:INFO|PAY"},
                    },
                ],
            },
        },
    }

def flex_receipt(session: Session, order_id: str = "", show_admin_buttons: bool = False, shipping_fee: int = 0, grand_total: int = 0) -> Dict[str, Any]:
    subtotal = sum(ci.unit_price * ci.qty for ci in session.cart.values())
    if grand_total <= 0:
        grand_total = subtotal + shipping_fee

    item_lines: List[Dict[str, Any]] = []
    for ci in session.cart.values():
        line_total = ci.unit_price * ci.qty
        item_lines.append({
            "type": "box",
            "layout": "baseline",
            "contents": [
                {"type": "text", "text": f"{ci.name} ×{ci.qty}", "size": "sm", "color": THEME_TEXT, "wrap": True, "flex": 5},
                {"type": "text", "text": f"NT${line_total}", "size": "sm", "color": THEME_TEXT, "align": "end", "flex": 2},
            ],
        })

    method_line = session.pickup_method or "—"
    date_line = session.pickup_date or "—"
    time_line = session.pickup_time if session.pickup_method == "店取" else "—"

    top_info = [
        {"type": "text", "text": "🧾 結帳小卡", "weight": "bold", "size": "xl", "color": THEME_TEXT},
        {"type": "text", "text": f"取貨方式：{method_line}", "size": "sm", "color": THEME_SUB, "wrap": True},
        {"type": "text", "text": f"日期：{date_line}", "size": "sm", "color": THEME_SUB, "wrap": True},
        {"type": "text", "text": f"時段：{time_line}", "size": "sm", "color": THEME_SUB, "wrap": True},
        {"type": "separator", "margin": "md"},
    ]

    items_block = (item_lines if item_lines else [{"type": "text", "text": "（購物車是空的）", "size": "sm", "color": THEME_SUB}])

    totals_block = [
        {"type": "separator", "margin": "md"},
        {"type": "box", "layout": "baseline", "contents": [
            {"type": "text", "text": "小計", "size": "sm", "color": THEME_SUB, "flex": 3},
            {"type": "text", "text": f"NT${subtotal}", "size": "sm", "color": THEME_TEXT, "align": "end", "flex": 2},
        ]},
        {"type": "box", "layout": "baseline", "contents": [
            {"type": "text", "text": "運費", "size": "sm", "color": THEME_SUB, "flex": 3},
            {"type": "text", "text": f"NT${shipping_fee}", "size": "sm", "color": THEME_TEXT, "align": "end", "flex": 2},
        ]},
        {"type": "separator", "margin": "md"},
        {"type": "box", "layout": "baseline", "contents": [
            {"type": "text", "text": "總計", "size": "md", "weight": "bold", "color": THEME_TEXT, "flex": 3},
            {"type": "text", "text": f"NT${grand_total}", "size": "md", "weight": "bold", "color": THEME_TEXT, "align": "end", "flex": 2},
        ]},
    ]

    body_contents: List[Dict[str, Any]] = [
        {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "paddingAll": "14px",
            "backgroundColor": THEME_CARD,
            "cornerRadius": "18px",
            "borderWidth": "1px",
            "borderColor": THEME_STROKE,
            "contents": [
                *top_info,
                *items_block,
                *totals_block,
            ],
        }
    ]

    if show_admin_buttons and order_id:
        footer_buttons = [
            {"type": "button", "style": "primary", "color": "#6FE3B2",
             "action": {"type": "postback", "label": "💰 已收款", "data": f"PB:STATUS|{order_id}|PAID"}},
            {"type": "button", "style": "primary", "color": "#86B7FF",
             "action": {"type": "postback", "label": "📣 已完成", "data": f"PB:STATUS|{order_id}|READY"}},
            {"type": "button", "style": "primary", "color": "#C8A2FF",
             "action": {"type": "postback", "label": "📦 已出貨", "data": f"PB:STATUS|{order_id}|SHIPPED"}},
        ]
    else:
        footer_buttons = [
            {"type": "button", "style": "secondary", "color": BTN_SECONDARY,
             "action": {"type": "postback", "label": "➕ 繼續加購", "data": "PB:MENU"}},
            {"type": "button", "style": "secondary", "color": BTN_SECONDARY,
             "action": {"type": "postback", "label": "🛠 修改/重選", "data": "PB:MENU"}},
        ]
        if order_id:
            footer_buttons.insert(0, {
                "type": "button",
                "style": "primary",
                "color": BTN_PRIMARY,
                "action": {"type": "postback", "label": "✅ 下一步（付款/取貨說明）", "data": f"PB:NEXT|{order_id}"},
            })

    return {
        "type": "flex",
        "altText": "結帳小卡",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "backgroundColor": THEME_BG,
                "contents": body_contents,
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "backgroundColor": THEME_BG,
                "contents": footer_buttons,
            },
        },
    }


# =============================================================================
# Business logic: dates / times
# =============================================================================

def today_tz() -> dt.date:
    return dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8))).date()

def valid_pickup_dates() -> List[str]:
    base = today_tz()
    out = []
    for d in range(MIN_DAYS, MAX_DAYS + 1):
        day = base + dt.timedelta(days=d)
        iso = day.isoformat()
        if iso in CLOSED_DATES:
            continue
        if day.weekday() in CLOSED_WEEKDAYS:
            continue
        out.append(iso)
    return out

PICKUP_TIME_OPTIONS = ["11:00-12:00", "12:00-14:00", "14:00-16:00"]

def quickreply_dates() -> Dict[str, Any]:
    dates = valid_pickup_dates()
    items = []
    for iso in dates[:13]:
        items.append({"type": "action", "action": {"type": "postback", "label": iso, "data": f"PB:DATE|{iso}"}})
    return {"items": items}

def quickreply_times() -> Dict[str, Any]:
    items = []
    for t in PICKUP_TIME_OPTIONS:
        items.append({"type": "action", "action": {"type": "postback", "label": t, "data": f"PB:TIME|{t}"}})
    return {"items": items}


# =============================================================================
# Order Id / Totals
# =============================================================================

def new_order_id() -> str:
    now = dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8)))
    return f"UOO-{now.strftime('%Y%m%d')}-{now.strftime('%H%M%S')}"

def calc_subtotal(session: Session) -> int:
    return sum(ci.unit_price * ci.qty for ci in session.cart.values())

def calc_shipping_fee(session: Session) -> int:
    if session.pickup_method != "宅配":
        return 0
    subtotal = calc_subtotal(session)
    return 0 if subtotal >= SHIP_FREE_OVER else SHIP_FEE


# =============================================================================
# Sheets schema (header-based)
# =============================================================================

C_HEADERS = [
    "created_at",
    "user_id",
    "display_name",
    "order_id",
    "items_json",
    "pickup_method",
    "pickup_date",
    "pickup_time",
    "recipient",
    "phone",
    "address",
    "note",
    "amount",
    "pay_status",
    "ship_status",
    "calendar_event_id",
]

AB_HEADERS = [
    "created_at",
    "order_id",
    "item_name",
    "spec",
    "flavor",
    "qty",
    "unit_price",
    "subtotal",
    "pickup_method",
    "pickup_date",
    "pickup_time",
    "recipient",
    "phone",
    "address",
    "status",
]

CLOG_HEADERS = [
    "created_at",
    "order_id",
    "flow_type",      # ORDER / STATUS
    "method",         # 店取 / 宅配
    "amount",         # subtotal
    "shipping_fee",
    "grand_total",
    "status",         # ORDER / PAID / READY / SHIPPED
    "note",
]

def items_json_from_session(session: Session) -> str:
    data = []
    for ci in session.cart.values():
        data.append({
            "item_id": ci.item_id,
            "name": ci.name,
            "unit_price": ci.unit_price,
            "qty": ci.qty,
            "spec": ci.spec,
            "flavor": ci.flavor,
        })
    return json.dumps(data, ensure_ascii=False)

def ensure_all_sheets():
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID/GSHEET_ID")

    ensure_headers(SHEET_C_NAME, C_HEADERS)
    ensure_headers(SHEET_CLOG_NAME, CLOG_HEADERS)
    ensure_headers(SHEET_A_NAME, AB_HEADERS)
    ensure_headers(SHEET_B_NAME, AB_HEADERS)
    ensure_headers(SHEET_ITEMS_NAME, ["item_id", "name", "price", "spec", "flavor", "enabled", "min_qty", "step_qty"])

def upsert_order_to_c(session: Session, order_id: str, pay_status: str, ship_status: str, note: str, calendar_event_id: str = ""):
    header = ensure_headers(SHEET_C_NAME, C_HEADERS)
    rows = find_rows_by_value(SHEET_C_NAME, header, "order_id", order_id)
    now = dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8))).isoformat(sep=" ", timespec="seconds")

    amount = calc_subtotal(session) + calc_shipping_fee(session)

    row_data = {
        "created_at": now,
        "user_id": session.user_id,
        "display_name": session.display_name or "",
        "order_id": order_id,
        "items_json": items_json_from_session(session),
        "pickup_method": session.pickup_method,
        "pickup_date": session.pickup_date,
        "pickup_time": session.pickup_time,
        "recipient": session.recipient,
        "phone": session.phone,
        "address": session.address,
        "note": note,
        "amount": amount,
        "pay_status": pay_status,
        "ship_status": ship_status,
        "calendar_event_id": calendar_event_id,
    }

    if rows:
        update_cells(SHEET_C_NAME, rows[0], row_data, header)
    else:
        row = [row_data.get(h, "") for h in C_HEADERS]
        sheets_append_row(SHEET_C_NAME, row)

def append_log(order_id: str, flow_type: str, method: str, amount: int, shipping_fee: int, grand_total: int, status: str, note: str):
    ensure_headers(SHEET_CLOG_NAME, CLOG_HEADERS)
    now = dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8))).isoformat(sep=" ", timespec="seconds")
    row_map = {
        "created_at": now,
        "order_id": order_id,
        "flow_type": flow_type,
        "method": method,
        "amount": amount,
        "shipping_fee": shipping_fee,
        "grand_total": grand_total,
        "status": status,
        "note": note,
    }
    row = [row_map.get(h, "") for h in CLOG_HEADERS]
    sheets_append_row(SHEET_CLOG_NAME, row)

def write_items_to_ab(session: Session, order_id: str, status: str):
    target = SHEET_A_NAME if session.pickup_method == "宅配" else SHEET_B_NAME
    ensure_headers(target, AB_HEADERS)

    now = dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8))).isoformat(sep=" ", timespec="seconds")

    for ci in session.cart.values():
        row_map = {
            "created_at": now,
            "order_id": order_id,
            "item_name": ci.name,
            "spec": ci.spec,
            "flavor": ci.flavor,
            "qty": ci.qty,
            "unit_price": ci.unit_price,
            "subtotal": ci.unit_price * ci.qty,
            "pickup_method": session.pickup_method,
            "pickup_date": session.pickup_date,
            "pickup_time": session.pickup_time,
            "recipient": session.recipient,
            "phone": session.phone,
            "address": session.address,
            "status": status,
        }
        row = [row_map.get(h, "") for h in AB_HEADERS]
        sheets_append_row(target, row)

def update_ab_status(order_id: str, pickup_method: str, status: str):
    target = SHEET_A_NAME if pickup_method == "宅配" else SHEET_B_NAME
    header = ensure_headers(target, AB_HEADERS)
    rows = find_rows_by_value(target, header, "order_id", order_id)
    for r in rows:
        update_cells(target, r, {"status": status}, header)


# =============================================================================
# Notifications (no debug text)
# =============================================================================

def notify_customer_paid(user_id: str, order_id: str):
    line_push(user_id, [text_msg(f"💰 已收到款項，我們會開始製作。\n訂單編號：{order_id}")])

def notify_customer_ready(user_id: str, order_id: str):
    line_push(user_id, [text_msg(f"📣 你的訂單已完成，可以來取貨了。\n訂單編號：{order_id}\n如需更改取貨時間，請直接回覆訊息。")])

def notify_customer_shipped(user_id: str, order_id: str):
    line_push(user_id, [text_msg(f"📦 宅配已出貨。\n訂單編號：{order_id}\n（到貨時間依物流為準）")])


# =============================================================================
# Info text (your screenshot content)
# =============================================================================

def build_pickup_info() -> str:
    return "\n".join([
        f"店取地址：\n{STORE_ADDRESS}",
        "",
        "提醒：所有甜點需提前 3 天預訂。",
        "",
        f"宅配：一律冷凍宅配（{SHIP_PROVIDER}）",
        f"運費 {SHIP_FEE} 元／滿 {SHIP_FREE_OVER} 免運",
        "",
        "注意事項：",
        "• 保持電話暢通（避免退件）",
        "• 收到後立即開箱確認並儘快冷凍/冷藏",
        "• 若嚴重損壞請拍照（含原箱）並當日聯繫",
        "• 未處理完前請保留原狀勿丟棄/食用",
        "",
        "風險認知：",
        "• 易碎品運送中輕微位移/裝飾掉落通常不在理賠範圍",
        "• 天災可能導致延遲或停送，無法保證準時",
    ])

def build_payment_info() -> str:
    return "\n".join([
        "付款方式：轉帳（對帳後依訂單號安排出貨/取貨）",
        f"{BANK_NAME} {BANK_CORE}",
        f"帳號：{BANK_ACCOUNT}",
        "",
        "轉帳後請回傳：",
        "「已轉帳 訂單編號 末五碼12345」",
    ])


# =============================================================================
# Flow helpers
# =============================================================================

def reset_flow(session: Session):
    session.pickup_method = ""
    session.pickup_date = ""
    session.pickup_time = ""
    session.recipient = ""
    session.phone = ""
    session.address = ""
    session.step = ""

def looks_like_phone(s: str) -> bool:
    s2 = re.sub(r"\D", "", s)
    return len(s2) >= 8

def looks_like_address(s: str) -> bool:
    return ("縣" in s or "市" in s) and (len(s) >= 6)

def normalize_phone(s: str) -> str:
    return re.sub(r"\s+", "", s).strip()


# =============================================================================
# Core: create order
# =============================================================================

def create_order_and_reply(reply_token: str, session: Session) -> None:
    if not session.cart:
        line_reply(reply_token, [text_msg("購物車是空的喔～先去甜點選單加購吧！")])
        return

    # enforce min_qty (達克瓦茲至少 2 顆)
    for ci in session.cart.values():
        if ci.qty < ci.min_qty:
            ci.qty = ci.min_qty

    order_id = new_order_id()
    shipping_fee = calc_shipping_fee(session)
    grand_total = calc_subtotal(session) + shipping_fee

    if session.pickup_method == "店取":
        note = f"店取 {session.pickup_date} {session.pickup_time} | {session.display_name}"
    else:
        note = f"宅配 期望到貨:{session.pickup_date} | 收件人:{session.recipient} | {session.phone} | {session.address} | {session.display_name}"

    # try write sheets + calendar; even if failed, still reply customer (no flied)
    try:
        ensure_all_sheets()

        calendar_event_id = ""
        try:
            if ENABLE_CALENDAR:
                calendar_event_id = ensure_calendar_event(
                    order_id=order_id,
                    pickup_method=session.pickup_method,
                    pickup_date=session.pickup_date,
                    pickup_time=session.pickup_time,
                    note=note,
                    existing_event_id="",
                )
        except Exception as e:
            print("[GCAL] create skipped:", e)

        upsert_order_to_c(session, order_id, pay_status="UNPAID", ship_status="UNPAID", note=note, calendar_event_id=calendar_event_id)
        write_items_to_ab(session, order_id, status="UNPAID")

        append_log(
            order_id=order_id,
            flow_type="ORDER",
            method=session.pickup_method,
            amount=calc_subtotal(session),
            shipping_fee=shipping_fee,
            grand_total=grand_total,
            status="ORDER",
            note=note,
        )

        push_to_admins([text_msg(f"🆕 新訂單\n{order_id}\n{note}\n總計：NT${grand_total}")])

    except Exception as e:
        print("[ORDER] sheets failed:", e)

    receipt = flex_receipt(session, order_id=order_id, show_admin_buttons=False, shipping_fee=shipping_fee, grand_total=grand_total)
    line_reply(reply_token, [receipt, text_msg(f"✅ 訂單已建立（待轉帳）\n訂單編號：{order_id}")])
    reset_flow(session)


# =============================================================================
# Webhook handler
# =============================================================================

@app.get("/")
def health():
    return {"ok": True, "service": "uoo_uoo_order_bot"}

@app.post("/callback")
async def callback(request: Request, x_line_signature: str = Header(default="")):
    body = await request.body()

    if not x_line_signature:
        raise HTTPException(status_code=400, detail="Missing X-Line-Signature")
    if not verify_line_signature(body, x_line_signature):
        raise HTTPException(status_code=400, detail="Bad signature")

    try:
        payload = json.loads(body.decode("utf-8"))
    except:
        raise HTTPException(status_code=400, detail="Bad JSON")

    events = payload.get("events", [])
    for ev in events:
        try:
            handle_event(ev)
        except Exception as e:
            print("[EVENT] error:", e)

    gc_sessions()
    return JSONResponse({"ok": True})


def handle_event(ev: Dict[str, Any]) -> None:
    ev_type = ev.get("type")
    reply_token = ev.get("replyToken", "")
    source = ev.get("source", {})
    user_id = source.get("userId", "")

    if not user_id:
        return

    session = get_session(user_id)
    if not session.display_name:
        prof = get_profile(user_id)
        session.display_name = prof.get("displayName", "") or ""

    if ev_type == "message":
        msg = ev.get("message", {})
        if msg.get("type") == "text":
            text = (msg.get("text") or "").strip()
            on_text(reply_token, session, text)
        else:
            if reply_token:
                line_reply(reply_token, [text_msg("目前只支援文字操作喔～")])

    elif ev_type == "postback":
        data = ev.get("postback", {}).get("data", "")
        on_postback(reply_token, session, data)


def on_text(reply_token: str, session: Session, text: str) -> None:
    # user commands
    if text in ("甜點", "我要下單", "點餐", "點甜點", "選單"):
        line_reply(reply_token, [flex_menu(load_menu_items())])
        return

    if text in ("取貨說明", "取件說明"):
        line_reply(reply_token, [text_msg(build_pickup_info())])
        return

    if text in ("付款說明", "付款資訊"):
        line_reply(reply_token, [text_msg(build_payment_info())])
        return

    # flow inputs
    if session.step == "WAIT_RECIPIENT":
        session.recipient = text.strip()
        session.step = "WAIT_PHONE"
        line_reply(reply_token, [text_msg("✅ 收到～請輸入聯絡電話：")])
        return

    if session.step == "WAIT_PHONE":
        if not looks_like_phone(text):
            line_reply(reply_token, [text_msg("電話格式好像不太對～再輸入一次（例如 09xx-xxx-xxx）")])
            return
        session.phone = normalize_phone(text)
        session.step = "WAIT_ADDRESS"
        line_reply(reply_token, [text_msg("✅ 收到～請輸入宅配地址（完整地址）：")])
        return

    if session.step == "WAIT_ADDRESS":
        if not looks_like_address(text):
            # still allow, but gently nudge
            session.address = text.strip()
        else:
            session.address = text.strip()

        # create order now (宅配流程完成)
        create_order_and_reply(reply_token, session)
        return

    # If user typed address without being in step (rare), we can accept if宅配已選日期
    if session.pickup_method == "宅配" and session.pickup_date and not session.address and looks_like_address(text):
        session.address = text.strip()
        if not session.recipient:
            session.recipient = session.display_name or ""
        if not session.phone:
            session.phone = ""
        create_order_and_reply(reply_token, session)
        return

    # fallback
    line_reply(reply_token, [text_msg("我收到囉～\n你可以點「甜點 / 我要下單」開始，或點「取貨說明 / 付款說明」。")])


def on_postback(reply_token: str, session: Session, data: str) -> None:
    if not data.startswith("PB:"):
        return

    parts = data.split("|")
    action = parts[0].replace("PB:", "", 1)

    if action == "MENU":
        line_reply(reply_token, [flex_menu(load_menu_items())])
        return

    if action == "CLEAR":
        session.cart.clear()
        reset_flow(session)
        line_reply(reply_token, [text_msg("🧺 已清空購物車")])
        return

    if action == "INFO":
        if len(parts) >= 2 and parts[1] == "PICKUP":
            line_reply(reply_token, [text_msg(build_pickup_info())])
        else:
            line_reply(reply_token, [text_msg(build_payment_info())])
        return

    if action == "ADD":
        if len(parts) < 3:
            line_reply(reply_token, [text_msg("加入失敗：資料不完整")])
            return
        item_id = parts[1].strip()
        qty = int(parts[2]) if parts[2].isdigit() else 1

        mi = get_menu_item(item_id)
        if not mi:
            line_reply(reply_token, [text_msg("找不到這個品項，請回到選單重選。")])
            return

        if item_id in session.cart:
            session.cart[item_id].qty += qty
        else:
            session.cart[item_id] = CartItem(
                item_id=item_id,
                name=mi.name,
                unit_price=mi.price,
                qty=qty,
                spec=mi.spec,
                flavor=mi.flavor,
                min_qty=mi.min_qty,
                step_qty=mi.step_qty,
            )

        line_reply(reply_token, [text_msg(f"已加入：{mi.name} ×{qty}")])
        return

    if action == "CHECKOUT":
        if not session.cart:
            line_reply(reply_token, [text_msg("購物車是空的喔～先去甜點選單加購吧！")])
            return

        session.step = ""
        msg = {
            "type": "text",
            "text": "請選擇取貨方式：",
            "quickReply": {
                "items": [
                    {"type": "action", "action": {"type": "postback", "label": "店取", "data": "PB:METHOD|店取"}},
                    {"type": "action", "action": {"type": "postback", "label": "宅配", "data": "PB:METHOD|宅配"}},
                ]
            }
        }
        line_reply(reply_token, [msg])
        return

    if action == "METHOD":
        if len(parts) < 2:
            line_reply(reply_token, [text_msg("取貨方式資料不完整")])
            return
        session.pickup_method = parts[1].strip()
        session.pickup_date = ""
        session.pickup_time = ""
        session.recipient = ""
        session.phone = ""
        session.address = ""
        session.step = ""

        if session.pickup_method == "宅配":
            msg = {"type": "text", "text": f"請選擇希望到貨日期（{MIN_DAYS}～{MAX_DAYS}天內）：", "quickReply": quickreply_dates()}
        else:
            msg = {"type": "text", "text": f"請選擇取貨日期（{MIN_DAYS}～{MAX_DAYS}天內）：", "quickReply": quickreply_dates()}
        line_reply(reply_token, [msg])
        return

    if action == "DATE":
        if len(parts) < 2:
            line_reply(reply_token, [text_msg("日期資料不完整")])
            return
        session.pickup_date = parts[1].strip()

        if session.pickup_method == "店取":
            msg = {"type": "text", "text": "請選擇取貨時段：", "quickReply": quickreply_times()}
            line_reply(reply_token, [msg])
        else:
            session.step = "WAIT_RECIPIENT"
            line_reply(reply_token, [text_msg("請輸入收件人姓名：")])
        return

    if action == "TIME":
        if len(parts) < 2:
            line_reply(reply_token, [text_msg("時段資料不完整")])
            return
        session.pickup_time = parts[1].strip()
        # create order now (店取流程完成)
        create_order_and_reply(reply_token, session)
        return

    if action == "NEXT":
        if len(parts) < 2:
            line_reply(reply_token, [text_msg("資料不完整")])
            return
        order_id = parts[1].strip()
        msgs = [
            text_msg(build_payment_info()),
            text_msg(build_pickup_info()),
            text_msg(f"訂單編號：{order_id}\n（轉帳完成後可直接回覆截圖或訊息告知）"),
        ]
        line_reply(reply_token, msgs)
        return

    if action == "STATUS":
        if len(parts) < 3:
            line_reply(reply_token, [text_msg("狀態資料不完整")])
            return

        order_id = parts[1].strip()
        status = parts[2].strip().upper()

        if not is_admin(session.user_id):
            line_reply(reply_token, [text_msg("此功能僅限商家使用。")])
            return

        try:
            ensure_all_sheets()

            c_header = ensure_headers(SHEET_C_NAME, C_HEADERS)
            data_rows = sheets_read_range(SHEET_C_NAME, "A2:ZZ5000")

            row_num = None
            order_row = None
            idx_order = c_header.get("order_id", -1)

            for i, row in enumerate(data_rows, start=2):
                if idx_order >= 0 and idx_order < len(row) and str(row[idx_order]).strip() == order_id:
                    row_num = i
                    order_row = row
                    break

            if not row_num or not order_row:
                line_reply(reply_token, [text_msg("找不到這筆訂單（C表）")])
                return

            def cval(h: str) -> str:
                idx = c_header.get(h, -1)
                return str(order_row[idx]).strip() if idx >= 0 and idx < len(order_row) else ""

            target_user_id = cval("user_id")
            pickup_method = cval("pickup_method")
            pickup_date = cval("pickup_date")
            pickup_time = cval("pickup_time")
            note = cval("note")
            calendar_event_id = cval("calendar_event_id")
            amount_str = cval("amount")

            updates: Dict[str, Any] = {}
            if status == "PAID":
                updates["pay_status"] = "PAID"
            elif status == "READY":
                updates["ship_status"] = "READY"
            elif status == "SHIPPED":
                updates["ship_status"] = "SHIPPED"

            # calendar update
            try:
                if ENABLE_CALENDAR:
                    new_eid = ensure_calendar_event(
                        order_id=order_id,
                        pickup_method=pickup_method,
                        pickup_date=pickup_date,
                        pickup_time=pickup_time,
                        note=note,
                        existing_event_id=calendar_event_id,
                    )
                    if new_eid and new_eid != calendar_event_id:
                        updates["calendar_event_id"] = new_eid
            except Exception as e:
                print("[GCAL] update error:", e)

            if updates:
                update_cells(SHEET_C_NAME, row_num, updates, c_header)

            # update A/B status rows
            update_ab_status(order_id, pickup_method, status)

            # log
            shipping_fee = 0
            if pickup_method == "宅配":
                shipping_fee = SHIP_FEE
            grand_total = _to_int(amount_str, 0)
            subtotal = max(0, grand_total - shipping_fee)

            append_log(
                order_id=order_id,
                flow_type="STATUS",
                method=pickup_method,
                amount=subtotal,
                shipping_fee=shipping_fee,
                grand_total=grand_total,
                status=status,
                note=note,
            )

            # notify customer
            if target_user_id:
                if status == "PAID":
                    notify_customer_paid(target_user_id, order_id)
                elif status == "READY":
                    notify_customer_ready(target_user_id, order_id)
                elif status == "SHIPPED":
                    notify_customer_shipped(target_user_id, order_id)

            line_reply(reply_token, [text_msg(f"✅ 已更新：{order_id} → {status}")])
            return

        except Exception as e:
            print("[STATUS] error:", e)
            line_reply(reply_token, [text_msg("更新失敗：請看 Render logs")])
            return

    line_reply(reply_token, [text_msg("我收到指令了～但我看不懂這個按鈕資料。")])
