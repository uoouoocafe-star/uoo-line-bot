# main.py
# UooUoo Cafe - LINE Dessert Order Bot (Stable "All-in-One" Version)
# - FastAPI webhook
# - Google Sheets: A/B detail, C order summary, C_LOG log
# - Admin status buttons: PAID / READY / SHIPPED
# - Optional Google Calendar event creation/update
# - Env alias support for Render variables (your screenshot)

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
# Env helpers (supports your Render naming)
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

# LINE
LINE_CHANNEL_ACCESS_TOKEN = env_first("LINE_CHANNEL_ACCESS_TOKEN", "CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = env_first("LINE_CHANNEL_SECRET", "CHANNEL_SECRET")

# Admin
ADMIN_USER_IDS = set(env_csv("ADMIN_USER_IDS"))
ADMIN_TOKEN = env_first("ADMIN_TOKEN", default="")  # optional

# Google Sheet
SPREADSHEET_ID = env_first("SPREADSHEET_ID", "GSHEET_ID")
# Main sheets names (you can keep your current ones)
SHEET_A_NAME = env_first("SHEET_A_NAME", "GSHEET_TAB", "GSHEET_SHEET_NAME", default="A")
SHEET_B_NAME = env_first("SHEET_B_NAME", default="B")
SHEET_C_NAME = env_first("SHEET_C_NAME", default="C")
SHEET_CLOG_NAME = env_first("SHEET_CLOG_NAME", default="c_log")
SHEET_ITEMS_NAME = env_first("SHEET_ITEMS_NAME", default="items")
SHEET_SETTINGS_NAME = env_first("SHEET_SETTINGS_NAME", default="settings")

# Google Service Account (Base64 supported)
GOOGLE_SERVICE_ACCOUNT_B64 = env_first("GOOGLE_SERVICE_ACCOUNT_B64", default="")
GOOGLE_SERVICE_ACCOUNT_JSON = env_first("GOOGLE_SERVICE_ACCOUNT_JSON", default="")
GOOGLE_SERVICE_ACCOUNT_FILE = env_first("GOOGLE_SERVICE_ACCOUNT_FILE", default="")

# Calendar
GCAL_CALENDAR_ID = env_first("GCAL_CALENDAR_ID", default="")
GCAL_TIMEZONE = env_first("GCAL_TIMEZONE", "TZ", default="Asia/Taipei")
ENABLE_CALENDAR = bool(GCAL_CALENDAR_ID)

# Rules
MIN_DAYS = env_int("MIN_DAYS", default=3)
MAX_DAYS = env_int("MAX_DAYS", default=14)
ORDER_CUTOFF_HOURS = env_int("ORDER_CUTOFF_HOURS", default=0)

# Closed days
# CLOSED_WEEKDAYS: comma separated 0-6 (Mon=0)
CLOSED_WEEKDAYS = set()
for x in env_csv("CLOSED_WEEKDAYS"):
    try:
        CLOSED_WEEKDAYS.add(int(x))
    except:
        pass

# CLOSED_DATES: comma separated YYYY-MM-DD
CLOSED_DATES = set(env_csv("CLOSED_DATES"))

# Store / payment info
STORE_ADDRESS = env_first("STORE_ADDRESS", default="").strip()
BANK_NAME = env_first("BANK_NAME", default="").strip()
BANK_CORE = env_first("BANK_CORE", default="").strip()
BANK_ACCOUNT = env_first("BANK_ACCOUNT", default="").strip()

# Basic sanity checks
if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_CHANNEL_SECRET:
    # Still allow app to boot for debugging on Render; webhook will fail until env set
    print("[WARN] Missing LINE channel token/secret env.")

if not SPREADSHEET_ID:
    print("[WARN] Missing GSHEET_ID/SPREADSHEET_ID env. Sheets features will fail until set.")

# =============================================================================
# FastAPI
# =============================================================================

app = FastAPI(title="UooUoo Dessert Order Bot", version="stable-all-in-one")

LINE_API_BASE = "https://api.line.me/v2/bot"
LINE_REPLY_URL = f"{LINE_API_BASE}/message/reply"
LINE_PUSH_URL = f"{LINE_API_BASE}/message/push"
LINE_PROFILE_URL = f"{LINE_API_BASE}/profile"

REQ_TIMEOUT = 12  # seconds

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

@dataclasses.dataclass
class Session:
    user_id: str
    display_name: str = ""
    cart: Dict[str, CartItem] = dataclasses.field(default_factory=dict)
    pickup_method: str = ""  # "店取" / "宅配"
    pickup_date: str = ""    # YYYY-MM-DD
    pickup_time: str = ""    # "12:00-14:00" or empty for delivery
    phone: str = ""
    address: str = ""
    recipient: str = ""
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
    # LINE requires altText for flex; ensure any flex has altText
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
# Google API (Sheets + Calendar)
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
    # IMPORTANT: do NOT wrap sheet name in quotes; that caused your 400 earlier
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
    titles = []
    for sh in meta.get("sheets", []):
        titles.append(sh["properties"]["title"])
    return titles

def sheets_create_sheet_if_missing(sheet_name: str) -> None:
    sheets, _ = get_google_services()
    titles = sheets_get_sheet_titles()
    if sheet_name in titles:
        return
    req = {
        "requests": [{
            "addSheet": {
                "properties": {"title": sheet_name}
            }
        }]
    }
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
    for h in required_headers:
        if h not in header:
            header.append(h)
            changed = True

    if not header:
        header = required_headers[:]
        changed = True

    if changed:
        sheets_write_range(sheet_name, "1:1", [header])

    return {h: i for i, h in enumerate(header)}

def find_rows_by_value(sheet_name: str, header_map: Dict[str, int], key_header: str, key_value: str, max_scan: int = 5000) -> List[int]:
    """
    Scan sheet rows and return row numbers (1-based) where column key_header equals key_value.
    """
    if key_header not in header_map:
        return []
    col_idx = header_map[key_header]
    # read a rectangular range enough to cover max_scan rows
    # We'll read A1:Z... but in a stable way: read all rows up to max_scan in values.get
    # If the sheet is wide, values.get returns only existing cells; we handle missing.
    data = sheets_read_range(sheet_name, f"A2:ZZ{max_scan+1}")  # skip header row
    hits = []
    for i, row in enumerate(data, start=2):  # actual row number
        if col_idx < len(row) and str(row[col_idx]).strip() == key_value:
            hits.append(i)
    return hits

def update_cells(sheet_name: str, row_num: int, updates: Dict[str, Any], header_map: Dict[str, int]) -> None:
    """
    Update specific columns in a given row.
    """
    # Build range updates as one row array (we update each cell individually to avoid shifting)
    sheets, _ = get_google_services()
    reqs = []
    for header, value in updates.items():
        if header not in header_map:
            # add missing header
            header_map = ensure_headers(sheet_name, list(header_map.keys()) + [header])
        col_idx = header_map[header]
        # Convert col_idx to A1 column letters
        col_letters = col_to_a1(col_idx + 1)
        rng = f"{sheet_name}!{col_letters}{row_num}:{col_letters}{row_num}"
        reqs.append({"range": rng, "values": [[value]]})

    body = {"valueInputOption": "RAW", "data": reqs}
    sheets.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()

def col_to_a1(n: int) -> str:
    # 1-based index to letters
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# -------------------- Calendar helpers --------------------

def ensure_calendar_event(order_id: str, pickup_method: str, pickup_date: str, pickup_time: str, note: str, existing_event_id: str = "") -> str:
    """
    Create or update a calendar event. Returns eventId.
    If calendar not enabled, return "".
    """
    if not ENABLE_CALENDAR:
        return ""

    _, cal = get_google_services()
    if cal is None:
        return ""

    title = f"UooUoo 甜點訂單 {order_id}（{pickup_method}）"
    desc = note or ""

    # Determine time
    # - Delivery: all-day on pickup_date (expected arrival)
    # - Pickup: timed if pickup_time like "12:00-14:00"
    try:
        date_obj = dt.date.fromisoformat(pickup_date)
    except Exception:
        return existing_event_id or ""

    def tz_dt(d: dt.date, hhmm: str) -> str:
        # RFC3339 with timezone offset via TZ name is ok if using "dateTime" + "timeZone"
        t = dt.datetime.combine(d, dt.time.fromisoformat(hhmm))
        return t.isoformat()

    event: Dict[str, Any] = {
        "summary": title,
        "description": desc,
    }

    if pickup_method == "店取" and pickup_time and "-" in pickup_time:
        start_s, end_s = pickup_time.split("-", 1)
        start_s = start_s.strip()
        end_s = end_s.strip()
        # fallback if end missing
        if not re.match(r"^\d{2}:\d{2}$", start_s):
            # If format unexpected, use all-day
            event["start"] = {"date": pickup_date}
            event["end"] = {"date": (date_obj + dt.timedelta(days=1)).isoformat()}
        else:
            if not re.match(r"^\d{2}:\d{2}$", end_s):
                end_s = (dt.datetime.strptime(start_s, "%H:%M") + dt.timedelta(hours=2)).strftime("%H:%M")
            event["start"] = {"dateTime": tz_dt(date_obj, start_s), "timeZone": GCAL_TIMEZONE}
            event["end"] = {"dateTime": tz_dt(date_obj, end_s), "timeZone": GCAL_TIMEZONE}
    else:
        # all-day
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

def load_menu_items() -> List[MenuItem]:
    """
    Reads from SHEET_ITEMS_NAME with headers:
    item_id, name, price, spec, flavor, enabled
    If sheet missing or empty, fallback to a small default.
    """
    try:
        header = ensure_headers(SHEET_ITEMS_NAME, ["item_id", "name", "price", "spec", "flavor", "enabled"])
        data = sheets_read_range(SHEET_ITEMS_NAME, "A2:ZZ500")
        out: List[MenuItem] = []
        for row in data:
            def g(h: str, default=""):
                idx = header.get(h, -1)
                return str(row[idx]).strip() if idx >= 0 and idx < len(row) else default

            item_id = g("item_id")
            name = g("name")
            price_raw = g("price", "0")
            if not item_id or not name:
                continue
            try:
                price = int(re.search(r"\d+", price_raw).group(0)) if re.search(r"\d+", price_raw) else 0
            except:
                price = 0
            enabled = g("enabled", "TRUE").upper() not in ("FALSE", "0", "NO", "N")
            out.append(MenuItem(item_id=item_id, name=name, price=price, spec=g("spec"), flavor=g("flavor"), enabled=enabled))
        out = [x for x in out if x.enabled]
        if out:
            return out
    except Exception as e:
        print("[MENU] load failed:", e)

    # fallback
    return [
        MenuItem(item_id="canele_box6", name="可麗露 6顆/盒", price=490, spec="6顆/盒", flavor=""),
        MenuItem(item_id="toast_original", name="原味司康", price=65, spec="", flavor=""),
    ]

def get_menu_item(item_id: str) -> Optional[MenuItem]:
    for it in load_menu_items():
        if it.item_id == item_id:
            return it
    return None


# =============================================================================
# Flex templates (simple, pure-color style)
# =============================================================================

def flex_menu(items: List[MenuItem]) -> Dict[str, Any]:
    # Pure color, minimal
    contents = []
    for it in items:
        contents.append({
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "paddingAll": "12px",
            "backgroundColor": "#FFFFFF",
            "cornerRadius": "12px",
            "borderWidth": "1px",
            "borderColor": "#EDEDED",
            "contents": [
                {"type": "text", "text": it.name, "weight": "bold", "size": "md", "wrap": True, "color": "#333333"},
                {"type": "text", "text": f"NT${it.price}", "size": "sm", "color": "#666666"},
                {
                    "type": "button",
                    "style": "primary",
                    "height": "sm",
                    "color": "#4CAF50",
                    "action": {"type": "postback", "label": "➕ 加入購物車", "data": f"PB:ADD|{it.item_id}|1"},
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
                "contents": [
                    {"type": "text", "text": "甜點選單", "weight": "bold", "size": "xl", "color": "#333333"},
                    {"type": "text", "text": "點選加入購物車，再前往結帳。", "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "box", "layout": "vertical", "spacing": "sm", "contents": contents},
                ],
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "contents": [
                    {
                        "type": "button",
                        "style": "primary",
                        "color": "#2E7D32",
                        "action": {"type": "postback", "label": "🧾 前往結帳", "data": "PB:CHECKOUT"},
                    },
                    {
                        "type": "button",
                        "style": "secondary",
                        "action": {"type": "postback", "label": "🧺 清空購物車", "data": "PB:CLEAR"},
                    },
                ],
            },
        },
    }

def flex_cart_receipt(session: Session, order_id: str = "", shipping_fee: int = 0, grand_total: int = 0, show_admin_buttons: bool = False) -> Dict[str, Any]:
    # Build item lines
    item_lines = []
    subtotal = 0
    for ci in session.cart.values():
        line_total = ci.unit_price * ci.qty
        subtotal += line_total
        item_lines.append({
            "type": "box",
            "layout": "baseline",
            "contents": [
                {"type": "text", "text": f"{ci.name} ×{ci.qty}", "size": "sm", "color": "#333333", "wrap": True, "flex": 5},
                {"type": "text", "text": f"NT${line_total}", "size": "sm", "color": "#333333", "align": "end", "flex": 2},
            ],
        })

    if grand_total <= 0:
        grand_total = subtotal + shipping_fee

    # Method / date / time lines
    method_line = session.pickup_method or "—"
    date_line = session.pickup_date or "—"
    time_line = session.pickup_time or ("—" if session.pickup_method == "店取" else "—")

    summary_lines = [
        {"type": "box", "layout": "baseline", "contents": [
            {"type": "text", "text": "小計", "size": "sm", "color": "#666666", "flex": 3},
            {"type": "text", "text": f"NT${subtotal}", "size": "sm", "color": "#333333", "align": "end", "flex": 2},
        ]},
        {"type": "box", "layout": "baseline", "contents": [
            {"type": "text", "text": "運費", "size": "sm", "color": "#666666", "flex": 3},
            {"type": "text", "text": f"NT${shipping_fee}", "size": "sm", "color": "#333333", "align": "end", "flex": 2},
        ]},
        {"type": "separator", "margin": "md"},
        {"type": "box", "layout": "baseline", "contents": [
            {"type": "text", "text": "總計", "size": "md", "weight": "bold", "color": "#333333", "flex": 3},
            {"type": "text", "text": f"NT${grand_total}", "size": "md", "weight": "bold", "color": "#333333", "align": "end", "flex": 2},
        ]},
    ]

    body_contents: List[Dict[str, Any]] = [
        {"type": "text", "text": "結帳內容", "weight": "bold", "size": "xl", "color": "#333333"},
        {"type": "text", "text": f"取貨方式：{method_line}", "size": "sm", "color": "#666666", "wrap": True},
        {"type": "text", "text": f"日期：{date_line}", "size": "sm", "color": "#666666", "wrap": True},
        {"type": "text", "text": f"時段：{time_line}", "size": "sm", "color": "#666666", "wrap": True},
        {"type": "separator", "margin": "md"},
        *item_lines if item_lines else [{"type": "text", "text": "（購物車是空的）", "size": "sm", "color": "#666666"}],
        {"type": "separator", "margin": "md"},
        *summary_lines,
    ]

    footer_buttons: List[Dict[str, Any]] = [
        {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "🛠 修改品項", "data": "PB:MENU"}},
        {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "➕ 繼續加購", "data": "PB:MENU"}},
    ]

    if order_id:
        footer_buttons.insert(0, {
            "type": "button",
            "style": "primary",
            "color": "#2E7D32",
            "action": {"type": "postback", "label": "✅ 下一步", "data": f"PB:NEXT|{order_id}"},
        })

    if show_admin_buttons and order_id:
        footer_buttons = [
            {"type": "button", "style": "primary", "color": "#2E7D32",
             "action": {"type": "postback", "label": "💰 已收款", "data": f"PB:STATUS|{order_id}|PAID"}},
            {"type": "button", "style": "primary", "color": "#1976D2",
             "action": {"type": "postback", "label": "📣 已做好/通知取貨", "data": f"PB:STATUS|{order_id}|READY"}},
            {"type": "button", "style": "primary", "color": "#6D4C41",
             "action": {"type": "postback", "label": "📦 已出貨/通知", "data": f"PB:STATUS|{order_id}|SHIPPED"}},
        ]

    return {
        "type": "flex",
        "altText": "結帳內容",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": body_contents},
            "footer": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": footer_buttons},
        },
    }

def text_msg(text: str) -> Dict[str, Any]:
    return {"type": "text", "text": text}

# =============================================================================
# Business logic: date options
# =============================================================================

def today_tz() -> dt.date:
    # Taiwan timezone by date; keep simple
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

PICKUP_TIME_OPTIONS = ["11:00-12:00", "12:00-14:00", "14:00-16:00", "10:00-12:00"]

def quickreply_dates() -> Dict[str, Any]:
    dates = valid_pickup_dates()
    items = []
    for iso in dates[:13]:
        items.append({
            "type": "action",
            "action": {"type": "postback", "label": iso, "data": f"PB:DATE|{iso}"}
        })
    return {"items": items}

def quickreply_times() -> Dict[str, Any]:
    items = []
    for t in PICKUP_TIME_OPTIONS:
        items.append({
            "type": "action",
            "action": {"type": "postback", "label": t, "data": f"PB:TIME|{t}"}
        })
    return {"items": items}

# =============================================================================
# Order Id / Totals
# =============================================================================

def new_order_id() -> str:
    now = dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8)))
    return f"UOO-{now.strftime('%Y%m%d')}-{now.strftime('%H%M')}"

def calc_subtotal(session: Session) -> int:
    return sum(ci.unit_price * ci.qty for ci in session.cart.values())

def calc_shipping_fee(session: Session) -> int:
    # You can later move this to settings sheet; keep stable default
    if session.pickup_method == "宅配":
        return 180
    return 0

# =============================================================================
# Sheets schema (header-based, prevents "亂掉")
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
    "phone",
    "status",
]

CLOG_HEADERS = [
    "created_at",
    "order_id",
    "flow_type",  # ORDER / STATUS
    "method",
    "amount",
    "shipping_fee",
    "grand_total",
    "status",
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
    ensure_headers(SHEET_ITEMS_NAME, ["item_id", "name", "price", "spec", "flavor", "enabled"])

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
        "note": note,
        "amount": amount,
        "pay_status": pay_status,
        "ship_status": ship_status,
        "calendar_event_id": calendar_event_id,
    }

    if rows:
        # update first match
        update_cells(SHEET_C_NAME, rows[0], row_data, header)
    else:
        # append by header order
        row = [row_data.get(h, "") for h in C_HEADERS]
        sheets_append_row(SHEET_C_NAME, row)

def append_log(order_id: str, flow_type: str, method: str, amount: int, shipping_fee: int, grand_total: int, status: str, note: str):
    header = ensure_headers(SHEET_CLOG_NAME, CLOG_HEADERS)
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
    """
    Write each item row into A or B depending on pickup_method.
    Also uses header-based mapping to prevent "亂掉".
    """
    target = SHEET_A_NAME if session.pickup_method == "宅配" else SHEET_B_NAME
    header = ensure_headers(target, AB_HEADERS)
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
            "phone": session.phone,
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
# Webhook handler
# =============================================================================

@app.get("/")
def health():
    return {"ok": True, "service": "uoo_uoo_order_bot"}

@app.post("/callback")
async def callback(request: Request, x_line_signature: str = Header(default="")):
    body = await request.body()

    # Basic guard
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
            # IMPORTANT: do not crash webhook, otherwise "no response" happens
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

    # profile caching in session
    session = get_session(user_id)
    if not session.display_name:
        prof = get_profile(user_id)
        session.display_name = prof.get("displayName", "") or ""

    if ev_type == "message":
        msg = ev.get("message", {})
        mtype = msg.get("type")
        if mtype == "text":
            text = (msg.get("text") or "").strip()
            on_text(reply_token, session, text)
        else:
            # ignore
            if reply_token:
                line_reply(reply_token, [text_msg("目前只支援文字操作喔～")])

    elif ev_type == "postback":
        data = ev.get("postback", {}).get("data", "")
        on_postback(reply_token, session, data)

def on_text(reply_token: str, session: Session, text: str) -> None:
    # Richmenu text mapping
    if text in ("甜點", "我要下單", "點餐", "點甜點"):
        items = load_menu_items()
        line_reply(reply_token, [flex_menu(items)])
        return

    if text in ("取貨說明", "取件說明"):
        line_reply(reply_token, [text_msg(build_pickup_info())])
        return

    if text in ("付款說明", "付款資訊"):
        line_reply(reply_token, [text_msg(build_payment_info())])
        return

    # If user is currently inputting address / phone / name in flow, accept heuristics
    # (Simple, stable: if they already chose宅配 and address empty -> treat as address)
    if session.pickup_method == "宅配" and session.address == "" and looks_like_address(text):
        session.address = text
        if reply_token:
            line_reply(reply_token, [text_msg("✅ 已收到宅配地址")])
        return

    # fallback
    if reply_token:
        line_reply(reply_token, [text_msg("我收到囉～\n你可以點「甜點 / 我要下單」開始，或點「取貨說明 / 付款資訊」。")])

def looks_like_address(s: str) -> bool:
    # Very simple heuristic
    return ("縣" in s or "市" in s) and (len(s) >= 6)

def on_postback(reply_token: str, session: Session, data: str) -> None:
    # Format: PB:ACTION|...
    if not data.startswith("PB:"):
        return

    parts = data.split("|")
    head = parts[0]  # PB:XXX
    action = head.replace("PB:", "", 1)

    if action == "MENU":
        line_reply(reply_token, [flex_menu(load_menu_items())])
        return

    if action == "CLEAR":
        session.cart.clear()
        line_reply(reply_token, [text_msg("🧺 已清空購物車")])
        return

    if action == "ADD":
        # PB:ADD|item_id|qty
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
            session.cart[item_id] = CartItem(item_id=item_id, name=mi.name, unit_price=mi.price, qty=qty, spec=mi.spec, flavor=mi.flavor)
        line_reply(reply_token, [text_msg(f"已加入：{mi.name} ×{qty}")])
        return

    if action == "CHECKOUT":
        if not session.cart:
            line_reply(reply_token, [text_msg("購物車是空的喔～先去甜點選單加購吧！")])
            return
        # ask method
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
        if session.pickup_method == "宅配":
            # ask date first (expected arrival)
            msg = {"type": "text", "text": "請選擇希望到貨日期（3～14天內）：", "quickReply": quickreply_dates()}
            line_reply(reply_token, [msg])
        else:
            msg = {"type": "text", "text": "請選擇取貨日期（3～14天內）：", "quickReply": quickreply_dates()}
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
            # delivery: ask address
            session.address = ""
            line_reply(reply_token, [text_msg("請輸入宅配地址（完整地址）：")])
        return

    if action == "TIME":
        if len(parts) < 2:
            line_reply(reply_token, [text_msg("時段資料不完整")])
            return
        session.pickup_time = parts[1].strip()

        # Confirm order creation
        order_id = new_order_id()
        shipping_fee = calc_shipping_fee(session)
        grand_total = calc_subtotal(session) + shipping_fee

        # Build note
        note = ""
        if session.pickup_method == "店取":
            note = f"店取 {session.pickup_date} {session.pickup_time} | {session.display_name}"
        else:
            note = f"宅配 期望到貨:{session.pickup_date} | {session.display_name} | {session.address}"

        # Write to sheets (stable: try/except but still reply)
        try:
            ensure_all_sheets()

            # Calendar
            calendar_event_id = ""
            try:
                calendar_event_id = ensure_calendar_event(
                    order_id=order_id,
                    pickup_method=session.pickup_method,
                    pickup_date=session.pickup_date,
                    pickup_time=session.pickup_time,
                    note=note,
                    existing_event_id="",
                )
            except Exception as e:
                print("[GCAL] skipped:", e)

            # C: create order summary
            upsert_order_to_c(session, order_id, pay_status="UNPAID", ship_status="UNPAID", note=note, calendar_event_id=calendar_event_id)

            # A/B: item rows
            write_items_to_ab(session, order_id, status="UNPAID")

            # C_LOG: order
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

            # Admin notification (new order)
            push_to_admins([
                text_msg(f"🆕 新訂單通知\n{order_id}\n{note}\n總計：NT${grand_total}")
            ])

        except Exception as e:
            print("[ORDER] sheets failed:", e)

        # Customer receipt (no debug)
        receipt = flex_cart_receipt(session, order_id=order_id, shipping_fee=shipping_fee, grand_total=grand_total, show_admin_buttons=False)
        line_reply(reply_token, [receipt, text_msg(f"✅ 訂單已建立（待轉帳）\n訂單編號：{order_id}")])
        return

    if action == "NEXT":
        # PB:NEXT|order_id
        if len(parts) < 2:
            line_reply(reply_token, [text_msg("資料不完整")])
            return
        order_id = parts[1].strip()
        # Show payment info + pickup info
        msgs = [text_msg(build_payment_info()), text_msg(build_pickup_info()), text_msg(f"訂單編號：{order_id}\n（轉帳完成後可直接回覆截圖或訊息告知）")]
        line_reply(reply_token, msgs)
        return

    if action == "STATUS":
        # PB:STATUS|order_id|PAID/READY/SHIPPED
        if len(parts) < 3:
            line_reply(reply_token, [text_msg("狀態資料不完整")])
            return
        order_id = parts[1].strip()
        status = parts[2].strip().upper()

        if not is_admin(session.user_id):
            line_reply(reply_token, [text_msg("此功能僅限商家使用。")])
            return

        # We need to locate order in C to know user_id & pickup_method
        try:
            ensure_all_sheets()
            c_header = ensure_headers(SHEET_C_NAME, C_HEADERS)
            data = sheets_read_range(SHEET_C_NAME, "A2:ZZ5000")
            row_num = None
            order_row = None
            for i, row in enumerate(data, start=2):
                idx = c_header.get("order_id", -1)
                if idx >= 0 and idx < len(row) and str(row[idx]).strip() == order_id:
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

            # Update statuses in C
            updates = {}
            pay_status = cval("pay_status") or "UNPAID"
            ship_status = cval("ship_status") or "UNPAID"

            if status == "PAID":
                pay_status = "PAID"
                updates["pay_status"] = "PAID"
            elif status == "READY":
                ship_status = "READY"
                updates["ship_status"] = "READY"
            elif status == "SHIPPED":
                ship_status = "SHIPPED"
                updates["ship_status"] = "SHIPPED"

            # Calendar update (optional)
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

            # Update A/B status for all item rows
            if status == "PAID":
                update_ab_status(order_id, pickup_method, "PAID")
            elif status == "READY":
                update_ab_status(order_id, pickup_method, "READY")
            elif status == "SHIPPED":
                update_ab_status(order_id, pickup_method, "SHIPPED")

            # Log
            shipping_fee = 180 if pickup_method == "宅配" else 0
            grand_total = 0
            try:
                grand_total = int(re.search(r"\d+", cval("amount") or "0").group(0)) if re.search(r"\d+", cval("amount") or "0") else 0
            except:
                grand_total = 0

            append_log(
                order_id=order_id,
                flow_type="STATUS",
                method=pickup_method,
                amount=grand_total - shipping_fee if grand_total else 0,
                shipping_fee=shipping_fee,
                grand_total=grand_total,
                status=status,
                note=note,
            )

            # Customer notifications (no debug line)
            if target_user_id:
                try:
                    if status == "PAID":
                        notify_customer_paid(target_user_id, order_id)
                    elif status == "READY":
                        notify_customer_ready(target_user_id, order_id)
                    elif status == "SHIPPED":
                        notify_customer_shipped(target_user_id, order_id)
                except Exception as e:
                    print("[Notify customer] error:", e)

            # Reply admin (short, no debug)
            line_reply(reply_token, [text_msg(f"✅ 已更新：{order_id} → {status}")])
            return

        except Exception as e:
            print("[STATUS] error:", e)
            line_reply(reply_token, [text_msg("更新失敗：請看 Render logs")])
            return

    # default
    line_reply(reply_token, [text_msg("我收到指令了～但我看不懂這個按鈕資料。")])

# =============================================================================
# Info text
# =============================================================================

def build_payment_info() -> str:
    lines = ["付款資訊"]
    if BANK_NAME or BANK_CORE or BANK_ACCOUNT:
        if BANK_NAME:
            lines.append(f"銀行：{BANK_NAME}")
        if BANK_CORE:
            lines.append(f"代碼：{BANK_CORE}")
        if BANK_ACCOUNT:
            lines.append(f"帳號：{BANK_ACCOUNT}")
        lines.append("轉帳完成後，請回覆轉帳末五碼或截圖，我們會幫你對帳。")
    else:
        lines.append("（尚未設定匯款資訊，請至 Render 環境變數設定 BANK_*）")
    return "\n".join(lines)

def build_pickup_info() -> str:
    lines = ["取貨/宅配說明"]
    lines.append(f"可選日期：{MIN_DAYS}～{MAX_DAYS} 天內")
    if STORE_ADDRESS:
        lines.append(f"店取地址：{STORE_ADDRESS}")
    lines.append("如需更改取貨時間，請直接回覆訊息。")
    return "\n".join(lines)
