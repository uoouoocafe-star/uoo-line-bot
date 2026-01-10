# main.py
# UooUoo Cafe - LINE Dessert Order Bot (Stable Full Version)
# - FastAPI webhook (LINE Messaging API)
# - Google Sheets:
#     A: orders summary
#     B: item details (one row per item)
#     C: status log (append)
#     c_log: raw log (append)
# - Admin buttons: PAID / READY / SHIPPED (and push customer)
# - Optional Google Calendar: create event on order created, update on status changes
# - No settings sheet dependency (uses env only)

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# =============================================================================
# Env helpers
# =============================================================================

def env_first(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default

def env_int(*keys: str, default: int) -> int:
    raw = env_first(*keys, default=str(default)).strip()
    # accept "(3)" / "MIN=3" etc
    m = re.search(r"-?\d+", raw.replace("(", "").replace(")", ""))
    return int(m.group(0)) if m else default

def env_csv(*keys: str) -> List[str]:
    s = env_first(*keys, default="")
    return [x.strip() for x in s.split(",") if x.strip()]


# =============================================================================
# Config (LINE)
# =============================================================================

LINE_CHANNEL_ACCESS_TOKEN = env_first("LINE_CHANNEL_ACCESS_TOKEN", "CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = env_first("LINE_CHANNEL_SECRET", "CHANNEL_SECRET")

LINE_API_BASE = "https://api.line.me/v2/bot"
LINE_REPLY_URL = f"{LINE_API_BASE}/message/reply"
LINE_PUSH_URL = f"{LINE_API_BASE}/message/push"
LINE_PROFILE_URL = f"{LINE_API_BASE}/profile"

REQ_TIMEOUT = env_int("REQ_TIMEOUT", default=12)

# Admin
ADMIN_USER_IDS = set(env_csv("ADMIN_USER_IDS"))
ADMIN_TOKEN = env_first("ADMIN_TOKEN", default="")

# =============================================================================
# Config (Sheets)
# =============================================================================

SPREADSHEET_ID = env_first("SPREADSHEET_ID", "GSHEET_ID")

# If you already use orders/orders_items etc, set envs accordingly.
# Defaults are conservative (you can change env without touching code).
SHEET_A_NAME = env_first("SHEET_A_NAME", default="orders")          # summary
SHEET_B_NAME = env_first("SHEET_B_NAME", default="orders_items")    # item rows
SHEET_C_NAME = env_first("SHEET_C_NAME", default="C")               # status log
SHEET_CLOG_NAME = env_first("SHEET_CLOG_NAME", default="c_log")     # raw log
SHEET_ITEMS_NAME = env_first("SHEET_ITEMS_NAME", default="items")   # menu items

GOOGLE_SERVICE_ACCOUNT_B64 = env_first("GOOGLE_SERVICE_ACCOUNT_B64", default="")
GOOGLE_SERVICE_ACCOUNT_JSON = env_first("GOOGLE_SERVICE_ACCOUNT_JSON", default="")
GOOGLE_SERVICE_ACCOUNT_FILE = env_first("GOOGLE_SERVICE_ACCOUNT_FILE", default="")

# =============================================================================
# Config (Business Rules via ENV)
# =============================================================================

MIN_DAYS = env_int("MIN_DAYS", default=3)
MAX_DAYS = env_int("MAX_DAYS", default=14)

# Closed weekdays: "2" means Wednesday if Mon=0, Tue=1, Wed=2 ...
CLOSED_WEEKDAYS = set()
for x in env_csv("CLOSED_WEEKDAYS"):
    try:
        CLOSED_WEEKDAYS.add(int(x))
    except Exception:
        pass

# Closed dates: "2026-01-13,2026-01-14"
CLOSED_DATES = set(env_csv("CLOSED_DATES"))

# Pickup time options (env override)
# Example: "11:00-12:00,12:00-14:00,14:00-16:00"
PICKUP_TIME_OPTIONS = env_csv("PICKUP_TIME_OPTIONS")
if not PICKUP_TIME_OPTIONS:
    PICKUP_TIME_OPTIONS = ["11:00-12:00", "12:00-14:00", "14:00-16:00"]

# Shipping
DEFAULT_SHIPPING_FEE = env_int("DEFAULT_SHIPPING_FEE", default=180)

# Store/payment info
STORE_ADDRESS = env_first("STORE_ADDRESS", default="").strip()
BANK_NAME = env_first("BANK_NAME", default="").strip()
BANK_CORE = env_first("BANK_CORE", default="").strip()
BANK_ACCOUNT = env_first("BANK_ACCOUNT", default="").strip()

# =============================================================================
# Config (Calendar)
# =============================================================================

GCAL_CALENDAR_ID = env_first("GCAL_CALENDAR_ID", default="")
GCAL_TIMEZONE = env_first("GCAL_TIMEZONE", "TZ", default="Asia/Taipei")
ENABLE_CALENDAR = bool(GCAL_CALENDAR_ID)


# =============================================================================
# FastAPI app
# =============================================================================

app = FastAPI(title="UooUoo Dessert Order Bot", version="stable-full")


# =============================================================================
# Simple dedup (LINE may resend webhook)
# =============================================================================

_DEDUP_LOCK = threading.Lock()
_DEDUP_TTL = 60 * 10
_DEDUP: Dict[str, float] = {}

def dedup_seen(key: str) -> bool:
    now = time.time()
    with _DEDUP_LOCK:
        # cleanup
        expired = [k for k, exp in _DEDUP.items() if exp <= now]
        for k in expired:
            _DEDUP.pop(k, None)

        if key in _DEDUP:
            return True
        _DEDUP[key] = now + _DEDUP_TTL
        return False


# =============================================================================
# Session store (memory)
# =============================================================================

@dataclass
class CartItem:
    item_id: str
    name: str
    unit_price: int
    qty: int = 1
    spec: str = ""
    flavor: str = ""

    @property
    def subtotal(self) -> int:
        return int(self.unit_price) * int(self.qty)

@dataclass
class Session:
    user_id: str
    display_name: str = ""
    cart: Dict[str, CartItem] = field(default_factory=dict)

    pickup_method: str = ""   # 店取 / 宅配
    pickup_date: str = ""     # YYYY-MM-DD
    pickup_time: str = ""     # for 店取

    receiver_name: str = ""
    phone: str = ""
    address: str = ""         # for 宅配

    awaiting: str = ""        # receiver_name / phone / address
    last_active: float = field(default_factory=lambda: time.time())

SESSIONS: Dict[str, Session] = {}
SESS_LOCK = threading.Lock()
SESS_TTL = 60 * 60 * 6  # 6 hours

def get_session(user_id: str) -> Session:
    with SESS_LOCK:
        s = SESSIONS.get(user_id)
        if not s:
            s = Session(user_id=user_id)
            SESSIONS[user_id] = s
        s.last_active = time.time()
        return s

def gc_sessions() -> None:
    now = time.time()
    with SESS_LOCK:
        dead = [uid for uid, s in SESSIONS.items() if now - s.last_active > SESS_TTL]
        for uid in dead:
            SESSIONS.pop(uid, None)


# =============================================================================
# LINE helpers
# =============================================================================

def line_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

def verify_line_signature(body: bytes, x_line_signature: str) -> bool:
    if not LINE_CHANNEL_SECRET:
        return False
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, x_line_signature)

def line_reply(reply_token: str, messages: List[Dict[str, Any]]) -> None:
    if not reply_token:
        return
    payload = {"replyToken": reply_token, "messages": messages}
    r = requests.post(LINE_REPLY_URL, headers=line_headers(), json=payload, timeout=REQ_TIMEOUT)
    if r.status_code >= 300:
        print("[LINE][reply] failed:", r.status_code, r.text[:500])

def line_push(to_user_id: str, messages: List[Dict[str, Any]]) -> None:
    if not to_user_id:
        return
    payload = {"to": to_user_id, "messages": messages}
    r = requests.post(LINE_PUSH_URL, headers=line_headers(), json=payload, timeout=REQ_TIMEOUT)
    if r.status_code >= 300:
        print("[LINE][push] failed:", r.status_code, r.text[:500])

def get_profile(user_id: str) -> Dict[str, Any]:
    try:
        r = requests.get(f"{LINE_PROFILE_URL}/{user_id}", headers=line_headers(), timeout=REQ_TIMEOUT)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print("[LINE profile] error:", repr(e))
    return {}

def is_admin(user_id: str, token: str = "") -> bool:
    if token and ADMIN_TOKEN and token == ADMIN_TOKEN:
        return True
    return user_id in ADMIN_USER_IDS

def text_msg(s: str) -> Dict[str, Any]:
    return {"type": "text", "text": s}


# =============================================================================
# Google clients
# =============================================================================

_GOOGLE_LOCK = threading.Lock()
_SHEETS = None
_CAL = None

def get_google_credentials() -> Credentials:
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
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

    raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_* env")

def sheets_service():
    global _SHEETS
    with _GOOGLE_LOCK:
        if _SHEETS is None:
            creds = get_google_credentials()
            _SHEETS = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return _SHEETS

def cal_service():
    global _CAL
    if not ENABLE_CALENDAR:
        return None
    with _GOOGLE_LOCK:
        if _CAL is None:
            creds = get_google_credentials()
            _CAL = build("calendar", "v3", credentials=creds, cache_discovery=False)
        return _CAL


# =============================================================================
# Sheets helpers (header-based append/update)
# =============================================================================

def _escape_sheet_name(name: str) -> str:
    # Safe quoting for Sheets A1 notation
    n = (name or "").strip()
    if n == "":
        return n
    needs_quote = bool(re.search(r"[ \[\]\(\)\-!@#$%^&*+=,./\\;:]", n))
    n2 = n.replace("'", "''")
    return f"'{n2}'" if needs_quote else n2

def a1(sheet: str, rng: str) -> str:
    s = _escape_sheet_name(sheet)
    r = (rng or "").strip()
    if "!" in r:
        return r
    return f"{s}!{r}"

def sheets_get_values(sheet: str, rng: str) -> List[List[Any]]:
    svc = sheets_service()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=a1(sheet, rng),
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    return resp.get("values", [])

def sheets_update_values(sheet: str, rng: str, values: List[List[Any]]) -> None:
    svc = sheets_service()
    svc.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=a1(sheet, rng),
        valueInputOption="RAW",
        body={"values": values},
    ).execute()

def sheets_append_values(sheet: str, values: List[List[Any]]) -> None:
    svc = sheets_service()
    # append to A:ZZ (no need to know last row)
    svc.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=a1(sheet, "A:ZZ"),
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()

def ensure_headers(sheet: str, required: List[str]) -> Dict[str, int]:
    # Read first row; if missing, write required
    try:
        rows = sheets_get_values(sheet, "1:1")
        header = [str(x).strip() for x in (rows[0] if rows else [])]
    except Exception:
        header = []

    changed = False
    if not header:
        header = required[:]
        changed = True
    else:
        for h in required:
            if h not in header:
                header.append(h)
                changed = True

    if changed:
        sheets_update_values(sheet, "1:1", [header])

    return {h: i for i, h in enumerate(header)}

def col_to_letter(n: int) -> str:
    # 1-based
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def find_first_row_by(sheet: str, header_map: Dict[str, int], key: str, value: str, max_rows: int = 5000) -> Optional[int]:
    if key not in header_map:
        return None
    data = sheets_get_values(sheet, f"A2:ZZ{max_rows+1}")
    kidx = header_map[key]
    for i, row in enumerate(data, start=2):
        if kidx < len(row) and str(row[kidx]).strip() == str(value).strip():
            return i
    return None

def batch_update_row(sheet: str, header_map: Dict[str, int], row_num: int, updates: Dict[str, Any]) -> None:
    svc = sheets_service()
    data = []
    for k, v in updates.items():
        if k not in header_map:
            # add header and refresh map
            header_map = ensure_headers(sheet, list(header_map.keys()) + [k])
        cidx = header_map[k] + 1
        col = col_to_letter(cidx)
        rng = a1(sheet, f"{col}{row_num}:{col}{row_num}")
        data.append({"range": rng, "values": [[v]]})

    if not data:
        return

    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


# =============================================================================
# Menu items (from items sheet)
# =============================================================================

@dataclass
class MenuItem:
    item_id: str
    name: str
    price: int
    spec: str = ""
    flavor: str = ""
    active: bool = True

def load_menu_items() -> List[MenuItem]:
    """
    items sheet headers supported:
      item_id, name, price, spec, flavor, active
    active: 1/0 or TRUE/FALSE
    """
    fallback = [
        MenuItem(item_id="canele_box6", name="可麗露 6顆/盒", price=490, spec="6顆/盒"),
    ]

    if not SPREADSHEET_ID:
        return fallback

    try:
        hm = ensure_headers(SHEET_ITEMS_NAME, ["item_id", "name", "price", "spec", "flavor", "active"])
        rows = sheets_get_values(SHEET_ITEMS_NAME, "A2:ZZ500")
        out: List[MenuItem] = []
        for r in rows:
            def g(h: str, default: str = "") -> str:
                idx = hm.get(h, -1)
                return str(r[idx]).strip() if idx >= 0 and idx < len(r) else default

            item_id = g("item_id")
            name = g("name")
            if not item_id or not name:
                continue

            price_raw = g("price", "0")
            m = re.search(r"-?\d+", price_raw)
            price = int(m.group(0)) if m else 0

            active_raw = g("active", "1").strip().lower()
            active = active_raw not in ("0", "false", "n", "no")

            out.append(
                MenuItem(
                    item_id=item_id,
                    name=name,
                    price=price,
                    spec=g("spec"),
                    flavor=g("flavor"),
                    active=active,
                )
            )
        out = [x for x in out if x.active]
        return out if out else fallback
    except Exception as e:
        print("[MENU] load failed:", repr(e))
        return fallback

def get_menu_item(item_id: str) -> Optional[MenuItem]:
    for it in load_menu_items():
        if it.item_id == item_id:
            return it
    return None


# =============================================================================
# Business date rules
# =============================================================================

def today_tw() -> dt.date:
    # Taiwan date
    return dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8))).date()

def is_closed(d: dt.date) -> bool:
    if d.isoformat() in CLOSED_DATES:
        return True
    if d.weekday() in CLOSED_WEEKDAYS:
        return True
    return False

def available_dates() -> List[str]:
    base = today_tw()
    out: List[str] = []
    for delta in range(MIN_DAYS, MAX_DAYS + 1):
        d = base + dt.timedelta(days=delta)
        if is_closed(d):
            continue
        out.append(d.isoformat())
    return out


# =============================================================================
# Flex builders
# =============================================================================

def flex_main_menu() -> Dict[str, Any]:
    return {
        "type": "flex",
        "altText": "UooUoo 甜點下單選單",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "contents": [
                    {"type": "text", "text": "UooUoo 甜點下單", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "text", "text": "請選擇：", "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "button", "style": "primary",
                     "action": {"type": "postback", "label": "🍰 甜點選單", "data": "PB:MENU"}},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": "🛒 查看購物車", "data": "PB:CART"}},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": "📦 取貨說明", "data": "PB:INFO_PICKUP"}},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": "💳 付款資訊", "data": "PB:INFO_PAY"}},
                ],
            },
        },
    }

def flex_menu(items: List[MenuItem]) -> Dict[str, Any]:
    blocks: List[Dict[str, Any]] = []
    if not items:
        blocks.append({"type": "text", "text": "目前沒有可下單品項。", "wrap": True})
    else:
        for it in items[:12]:
            blocks.append({
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "paddingAll": "12px",
                "borderWidth": "1px",
                "borderColor": "#EDEDED",
                "cornerRadius": "12px",
                "contents": [
                    {"type": "text", "text": it.name, "weight": "bold", "size": "md", "wrap": True},
                    {"type": "text", "text": f"NT${it.price}", "size": "sm", "color": "#666666"},
                    *(
                        [{"type": "text", "text": it.spec, "size": "sm", "color": "#666666", "wrap": True}]
                        if it.spec else []
                    ),
                    {"type": "button", "style": "primary", "height": "sm",
                     "action": {"type": "postback", "label": "➕ 加入購物車", "data": f"PB:ADD:{it.item_id}"}},
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
                    {"type": "text", "text": "甜點選單", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "text", "text": "加入購物車後再結帳。", "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "separator"},
                    *blocks,
                ],
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "contents": [
                    {"type": "button", "style": "primary",
                     "action": {"type": "postback", "label": "🧾 前往結帳", "data": "PB:CHECKOUT"}},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": "🛒 查看購物車", "data": "PB:CART"}},
                ],
            },
        },
    }

def flex_cart(sess: Session) -> Dict[str, Any]:
    lines: List[Dict[str, Any]] = []
    subtotal = 0
    for ci in sess.cart.values():
        subtotal += ci.subtotal
        lines.append({
            "type": "box",
            "layout": "horizontal",
            "contents": [
                {"type": "text", "text": f"{ci.name} ×{ci.qty}", "flex": 1, "wrap": True},
                {"type": "text", "text": f"NT${ci.subtotal}", "align": "end", "wrap": True},
            ],
        })

    if not lines:
        lines = [{"type": "text", "text": "（購物車是空的）", "size": "sm", "color": "#666666", "wrap": True}]

    method = sess.pickup_method or "—"
    date = sess.pickup_date or "—"
    time_ = sess.pickup_time or "—"

    return {
        "type": "flex",
        "altText": "購物車",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "contents": [
                    {"type": "text", "text": "購物車", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "text", "text": f"取貨方式：{method}", "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "text", "text": f"日期：{date}", "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "text", "text": f"時段：{time_}", "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "separator"},
                    *lines,
                    {"type": "separator"},
                    {"type": "box", "layout": "horizontal", "contents": [
                        {"type": "text", "text": "小計", "flex": 1, "wrap": True},
                        {"type": "text", "text": f"NT${subtotal}", "align": "end", "wrap": True},
                    ]},
                ],
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "contents": [
                    {"type": "button", "style": "primary",
                     "action": {"type": "postback", "label": "🧾 前往結帳", "data": "PB:CHECKOUT"}},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": "🧺 清空購物車", "data": "PB:CLEAR_CART"}},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback",": {"type": "postback", "label": "➕ 繼續加購", "data": "PB:MENU"}},
                ],
            },
        },
    }

def flex_admin_panel(order_id: str) -> Dict[str, Any]:
    return {
        "type": "flex",
        "altText": "商家管理",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "contents": [
                    {"type": "text", "text": "商家管理", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "text", "text": f"訂單：{order_id}", "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "separator"},
                    {"type": "button", "style": "primary",
                     "action": {"type": "postback", "label": "✅ 已收款 (PAID)", "data": f"PB:ADMIN_STATUS:{order_id}:PAID"}},
                    {"type": "button", "style": "primary",
                     "action": {"type": "postback", "label": "📣 已做好 (READY)", "data": f"PB:ADMIN_STATUS:{order_id}:READY"}},
                    {"type": "button", "style": "primary",
                     "action": {"type": "postback", "label": "🚚 已出貨 (SHIPPED)", "data": f"PB:ADMIN_STATUS:{order_id}:SHIPPED"}},
                ],
            },
        },
    }


# =============================================================================
# Order helpers
# =============================================================================

def new_order_id() -> str:
    now = dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8)))
    tail = str(uuid.uuid4().int)[-4:]
    return f"UOO-{now.strftime('%Y%m%d')}-{tail}"

def calc_subtotal(sess: Session) -> int:
    return sum(ci.subtotal for ci in sess.cart.values())

def calc_shipping_fee(sess: Session) -> int:
    if sess.pickup_method == "宅配":
        return int(DEFAULT_SHIPPING_FEE)
    return 0

def items_json(sess: Session) -> str:
    arr = []
    for ci in sess.cart.values():
        arr.append({
            "item_id": ci.item_id,
            "name": ci.name,
            "qty": ci.qty,
            "unit_price": ci.unit_price,
            "subtotal": ci.subtotal,
            "spec": ci.spec,
            "flavor": ci.flavor,
        })
    return json.dumps(arr, ensure_ascii=False)


# =============================================================================
# Sheets schema
# =============================================================================

A_HEADERS = [
    "created_at", "updated_at",
    "order_id",
    "user_id", "display_name",
    "pickup_method", "pickup_date", "pickup_time",
    "receiver_name", "phone", "address",
    "items_json",
    "amount", "shipping_fee", "grand_total",
    "payment_status", "ship_status", "status",
    "calendar_event_id",
    "note",
]

B_HEADERS = [
    "created_at",
    "order_id",
    "item_id", "item_name",
    "spec", "flavor",
    "qty", "unit_price", "subtotal",
    "pickup_method", "pickup_date", "pickup_time",
    "receiver_name", "phone", "address",
    "status",
]

C_HEADERS = [
    "created_at",
    "order_id",
    "flow_type",     # ORDER / STATUS
    "status",        # ORDER / PAID / READY / SHIPPED
    "note",
]

CLOG_HEADERS = [
    "created_at",
    "order_id",
    "event",
    "payload",
]

def ensure_sheets_headers() -> None:
    ensure_headers(SHEET_A_NAME, A_HEADERS)
    ensure_headers(SHEET_B_NAME, B_HEADERS)
    ensure_headers(SHEET_C_NAME, C_HEADERS)
    ensure_headers(SHEET_CLOG_NAME, CLOG_HEADERS)

def append_c_log(order_id: str, event: str, payload: str) -> None:
    now = dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8))).isoformat(sep=" ", timespec="seconds")
    ensure_headers(SHEET_CLOG_NAME, CLOG_HEADERS)
    row = [now, order_id, event, payload]
    sheets_append_values(SHEET_CLOG_NAME, [row])

def append_c_status(order_id: str, flow_type: str, status: str, note: str) -> None:
    now = dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8))).isoformat(sep=" ", timespec="seconds")
    ensure_headers(SHEET_C_NAME, C_HEADERS)
    row = [now, order_id, flow_type, status, note]
    sheets_append_values(SHEET_C_NAME, [row])

def write_order_A_B(sess: Session, order_id: str, calendar_event_id: str = "", status: str = "UNPAID", note: str = "") -> None:
    ensure_sheets_headers()

    now = dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8))).isoformat(sep=" ", timespec="seconds")
    subtotal = calc_subtotal(sess)
    ship_fee = calc_shipping_fee(sess)
    grand = subtotal + ship_fee

    # A summary
    a_row_map = {
        "created_at": now,
        "updated_at": now,
        "order_id": order_id,
        "user_id": sess.user_id,
        "display_name": sess.display_name,
        "pickup_method": sess.pickup_method,
        "pickup_date": sess.pickup_date,
        "pickup_time": sess.pickup_time,
        "receiver_name": sess.receiver_name,
        "phone": sess.phone,
        "address": sess.address,
        "items_json": items_json(sess),
        "amount": subtotal,
        "shipping_fee": ship_fee,
        "grand_total": grand,
        "payment_status": status if status in ("PAID",) else "UNPAID",
        "ship_status": "" if sess.pickup_method == "店取" else "",
        "status": "ORDER",
        "calendar_event_id": calendar_event_id,
        "note": note,
    }
    a_row = [a_row_map.get(h, "") for h in A_HEADERS]
    sheets_append_values(SHEET_A_NAME, [a_row])

    # B items
    for ci in sess.cart.values():
        b_row_map = {
            "created_at": now,
            "order_id": order_id,
            "item_id": ci.item_id,
            "item_name": ci.name,
            "spec": ci.spec,
            "flavor": ci.flavor,
            "qty": ci.qty,
            "unit_price": ci.unit_price,
            "subtotal": ci.subtotal,
            "pickup_method": sess.pickup_method,
            "pickup_date": sess.pickup_date,
            "pickup_time": sess.pickup_time,
            "receiver_name": sess.receiver_name,
            "phone": sess.phone,
            "address": sess.address,
            "status": "ORDER",
        }
        b_row = [b_row_map.get(h, "") for h in B_HEADERS]
        sheets_append_values(SHEET_B_NAME, [b_row])

    # C status
    append_c_status(order_id, "ORDER", "ORDER", note)

    # c_log raw
    append_c_log(order_id, "ORDER_CREATED", items_json(sess))


# =============================================================================
# Calendar
# =============================================================================

def ensure_calendar_event_create(sess: Session, order_id: str, note: str) -> str:
    if not ENABLE_CALENDAR:
        return ""
    cal = cal_service()
    if cal is None:
        return ""

    title = f"UooUoo 訂單 {order_id}（{sess.pickup_method}）"
    description = note

    # time rules
    # 店取：用 pickup_time
    # 宅配：做 all-day (期望到貨)
    try:
        d = dt.date.fromisoformat(sess.pickup_date)
    except Exception:
        return ""

    event: Dict[str, Any] = {
        "summary": title,
        "description": description,
        "location": STORE_ADDRESS if sess.pickup_method == "店取" else (sess.address or ""),
    }

    if sess.pickup_method == "店取" and sess.pickup_time and "-" in sess.pickup_time:
        st, et = sess.pickup_time.split("-", 1)
        st = st.strip()
        et = et.strip()
        if re.match(r"^\d{2}:\d{2}$", st) and re.match(r"^\d{2}:\d{2}$", et):
            start_dt = dt.datetime.combine(d, dt.time.fromisoformat(st)).isoformat()
            end_dt = dt.datetime.combine(d, dt.time.fromisoformat(et)).isoformat()
            event["start"] = {"dateTime": start_dt, "timeZone": GCAL_TIMEZONE}
            event["end"] = {"dateTime": end_dt, "timeZone": GCAL_TIMEZONE}
        else:
            event["start"] = {"date": sess.pickup_date}
            event["end"] = {"date": (d + dt.timedelta(days=1)).isoformat()}
    else:
        event["start"] = {"date": sess.pickup_date}
        event["end"] = {"date": (d + dt.timedelta(days=1)).isoformat()}

    try:
        created = cal.events().insert(calendarId=GCAL_CALENDAR_ID, body=event).execute()
        return created.get("id", "")
    except Exception as e:
        print("[GCAL] create failed:", repr(e))
        return ""

def calendar_update_event(event_id: str, summary: str = "", description: str = "") -> None:
    if not ENABLE_CALENDAR or not event_id:
        return
    cal = cal_service()
    if cal is None:
        return
    try:
        ev = cal.events().get(calendarId=GCAL_CALENDAR_ID, eventId=event_id).execute()
        if summary:
            ev["summary"] = summary
        if description:
            ev["description"] = description
        cal.events().update(calendarId=GCAL_CALENDAR_ID, eventId=event_id, body=ev).execute()
    except Exception as e:
        print("[GCAL] update failed:", repr(e))


# =============================================================================
# User-facing texts
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
        lines.append("匯款後請回覆：訂單編號＋末五碼（或截圖）。")
    else:
        lines.append("（尚未設定匯款資訊：請在 Render 環境變數填 BANK_NAME / BANK_CORE / BANK_ACCOUNT）")
    return "\n".join(lines)

def build_pickup_info() -> str:
    lines = ["取貨 / 宅配說明"]
    lines.append(f"可選日期：{MIN_DAYS}～{MAX_DAYS} 天內（會排除公休日/休假日）")
    lines.append(f"店取時段：{', '.join(PICKUP_TIME_OPTIONS)}")
    if STORE_ADDRESS:
        lines.append(f"店址：{STORE_ADDRESS}")
    lines.append(f"宅配運費：NT${DEFAULT_SHIPPING_FEE}（目前固定）")
    return "\n".join(lines)


# =============================================================================
# Flow builders (quick reply)
# =============================================================================

def qr_method() -> Dict[str, Any]:
    return {
        "items": [
            {"type": "action", "action": {"type": "postback", "label": "店取", "data": "PB:METHOD:店取"}},
            {"type": "action", "action": {"type": "postback", "label": "宅配", "data": "PB:METHOD:宅配"}},
        ]
    }

def qr_dates() -> Dict[str, Any]:
    ds = available_dates()
    items = []
    for d in ds[:13]:
        items.append({"type": "action", "action": {"type": "postback", "label": d, "data": f"PB:DATE:{d}"}})
    return {"items": items}

def qr_times() -> Dict[str, Any]:
    items = []
    for t in PICKUP_TIME_OPTIONS[:13]:
        items.append({"type": "action", "action": {"type": "postback", "label": t, "data": f"PB:TIME:{t}"}})
    return {"items": items}


# =============================================================================
# Admin: order lookup by order_id in A sheet
# =============================================================================

def find_order_in_A(order_id: str, max_rows: int = 5000) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    Return (row_num, row_map) from A sheet, row_num is 1-based.
    """
    hm = ensure_headers(SHEET_A_NAME, A_HEADERS)
    data = sheets_get_values(SHEET_A_NAME, f"A2:ZZ{max_rows+1}")
    oid_idx = hm.get("order_id", -1)
    if oid_idx < 0:
        return None, {}

    for i, row in enumerate(data, start=2):
        if oid_idx < len(row) and str(row[oid_idx]).strip() == order_id:
            # build row_map
            row_map: Dict[str, Any] = {}
            for k, idx in hm.items():
                row_map[k] = row[idx] if idx < len(row) else ""
            return i, row_map

    return None, {}

def update_status_all(order_id: str, status: str) -> None:
    """
    Update:
      - A: payment_status/ship_status/status/updated_at
      - B: status for all rows with order_id
      - C: append status row
      - c_log: append raw status
    """
    ensure_sheets_headers()

    now = dt.datetime.utcnow().astimezone(dt.timezone(dt.timedelta(hours=8))).isoformat(sep=" ", timespec="seconds")

    # A update
    a_hm = ensure_headers(SHEET_A_NAME, A_HEADERS)
    row_num, row_map = find_order_in_A(order_id)
    if not row_num:
        raise RuntimeError("Order not found in A")

    updates: Dict[str, Any] = {"updated_at": now, "status": status}
    if status == "PAID":
        updates["payment_status"] = "PAID"
    elif status in ("READY", "SHIPPED"):
        updates["ship_status"] = status

    batch_update_row(SHEET_A_NAME, a_hm, row_num, updates)

    # B update all rows
    b_hm = ensure_headers(SHEET_B_NAME, B_HEADERS)
    # scan for order_id
    data = sheets_get_values(SHEET_B_NAME, "A2:ZZ5000")
    oid_idx = b_hm.get("order_id", -1)
    if oid_idx >= 0:
        for i, r in enumerate(data, start=2):
            if oid_idx < len(r) and str(r[oid_idx]).strip() == order_id:
                batch_update_row(SHEET_B_NAME, b_hm, i, {"status": status})

    # C append
    note = row_map.get("note", "") if row_map else ""
    append_c_status(order_id, "STATUS", status, str(note))

    # c_log
    append_c_log(order_id, f"STATUS_{status}", "")


# =============================================================================
# Webhook handlers
# =============================================================================

def handle_text(reply_token: str, sess: Session, text: str) -> None:
    t = (text or "").strip()

    # If in follow-up input flow:
    if sess.awaiting:
        if sess.awaiting == "receiver_name":
            sess.receiver_name = t
            sess.awaiting = "phone"
            line_reply(reply_token, [text_msg("請輸入電話：")])
            return

        if sess.awaiting == "phone":
            sess.phone = t
            if sess.pickup_method == "宅配":
                sess.awaiting = "address"
                line_reply(reply_token, [text_msg("請輸入宅配地址（完整地址）：")])
            else:
                sess.awaiting = ""
                finalize_order(reply_token, sess)
            return

        if sess.awaiting == "address":
            sess.address = t
            sess.awaiting = ""
            finalize_order(reply_token, sess)
            return

    # Admin quick access by typing order id
    if t.startswith("UOO-") and is_admin(sess.user_id):
        line_reply(reply_token, [flex_admin_panel(t)])
        return

    if t in ("開始", "start", "選單", "menu"):
        line_reply(reply_token, [flex_main_menu()])
        return

    if t in ("甜點", "我要下單", "點餐", "點甜點", "甜點選單"):
        line_reply(reply_token, [flex_menu(load_menu_items())])
        return

    if t in ("取貨說明", "取件說明"):
        line_reply(reply_token, [text_msg(build_pickup_info())])
        return

    if t in ("付款資訊", "付款說明"):
        line_reply(reply_token, [text_msg(build_payment_info())])
        return

    # fallback
    line_reply(reply_token, [flex_main_menu()])


def handle_postback(reply_token: str, sess: Session, data: str) -> None:
    d = (data or "").strip()

    if d == "PB:MENU":
        line_reply(reply_token, [flex_menu(load_menu_items())])
        return

    if d == "PB:CART":
        line_reply(reply_token, [flex_cart(sess)])
        return

    if d == "PB:INFO_PICKUP":
        line_reply(reply_token, [text_msg(build_pickup_info())])
        return

    if d == "PB:INFO_PAY":
        line_reply(reply_token, [text_msg(build_payment_info())])
        return

    if d == "PB:CLEAR_CART":
        sess.cart.clear()
        line_reply(reply_token, [text_msg("🧺 已清空購物車"), flex_main_menu()])
        return

    if d.startswith("PB:ADD:"):
        item_id = d.split("PB:ADD:", 1)[1].strip()
        mi = get_menu_item(item_id)
        if not mi:
            line_reply(reply_token, [text_msg("找不到這個品項，請回到選單重選。"), flex_menu(load_menu_items())])
            return

        if item_id in sess.cart:
            sess.cart[item_id].qty += 1
        else:
            sess.cart[item_id] = CartItem(
                item_id=item_id,
                name=mi.name,
                unit_price=mi.price,
                qty=1,
                spec=mi.spec,
                flavor=mi.flavor,
            )
        line_reply(reply_token, [text_msg(f"已加入購物車：{mi.name} ×1"), flex_main_menu()])
        return

    if d == "PB:CHECKOUT":
        if not sess.cart:
            line_reply(reply_token, [text_msg("購物車是空的喔～先去甜點選單加購吧！"), flex_menu(load_menu_items())])
            return

        msg = {
            "type": "text",
            "text": "請選擇取貨方式：",
            "quickReply": qr_method(),
        }
        line_reply(reply_token, [msg])
        return

    if d.startswith("PB:METHOD:"):
        sess.pickup_method = d.split("PB:METHOD:", 1)[1].strip()
        sess.pickup_date = ""
        sess.pickup_time = ""
        sess.receiver_name = ""
        sess.phone = ""
        sess.address = ""
        sess.awaiting = ""

        msg = {
            "type": "text",
            "text": "請選擇日期（3～14 天內）：",
            "quickReply": qr_dates(),
        }
        line_reply(reply_token, [msg])
        return

    if d.startswith("PB:DATE:"):
        sess.pickup_date = d.split("PB:DATE:", 1)[1].strip()
        if sess.pickup_method == "店取":
            msg = {
                "type": "text",
                "text": "請選擇店取時段：",
                "quickReply": qr_times(),
            }
            line_reply(reply_token, [msg])
        else:
            # delivery: skip time
            sess.pickup_time = ""
            sess.awaiting = "receiver_name"
            line_reply(reply_token, [text_msg("請輸入收件人姓名：")])
        return

    if d.startswith("PB:TIME:"):
        sess.pickup_time = d.split("PB:TIME:", 1)[1].strip()
        sess.awaiting = "receiver_name"
        line_reply(reply_token, [text_msg("請輸入取件人姓名：")])
        return

    # Admin status buttons
    if d.startswith("PB:ADMIN_STATUS:"):
        # PB:ADMIN_STATUS:ORDERID:PAID
        if not is_admin(sess.user_id):
            line_reply(reply_token, [text_msg("此功能僅限商家使用。")])
            return

        try:
            _, rest = d.split("PB:ADMIN_STATUS:", 1)
            order_id, status = rest.split(":", 1)
            order_id = order_id.strip()
            status = status.strip().upper()

            update_status_all(order_id, status)

            # push customer
            # find order again to get user_id + calendar_event_id
            row_num, row_map = find_order_in_A(order_id)
            user_id = str(row_map.get("user_id", "")).strip() if row_map else ""
            pickup_method = str(row_map.get("pickup_method", "")).strip() if row_map else ""
            cal_id = str(row_map.get("calendar_event_id", "")).strip() if row_map else ""

            # Update calendar title with status (optional)
            if cal_id:
                calendar_update_event(
                    cal_id,
                    summary=f"UooUoo 訂單 {order_id}（{pickup_method} / {status}）",
                    description=str(row_map.get("note", "")) if row_map else "",
                )

            if user_id:
                if status == "PAID":
                    line_push(user_id, [text_msg(f"💰 已收到款項，開始製作中。\n訂單編號：{order_id}")])
                elif status == "READY":
                    line_push(user_id, [text_msg(f"📣 你的訂單已完成，可前往取貨。\n訂單編號：{order_id}")])
                elif status == "SHIPPED":
                    line_push(user_id, [text_msg(f"📦 宅配已出貨。\n訂單編號：{order_id}\n（到貨以物流為準）")])

            line_reply(reply_token, [text_msg(f"✅ 已更新狀態：{order_id} → {status}")])
            return

        except Exception as e:
            print("[ADMIN_STATUS] failed:", repr(e))
            line_reply(reply_token, [text_msg("更新失敗（請看 Render logs）")])
            return

    # fallback
    line_reply(reply_token, [flex_main_menu()])


def finalize_order(reply_token: str, sess: Session) -> None:
    if not sess.cart:
        line_reply(reply_token, [text_msg("購物車是空的，無法建立訂單。")])
        return
    if not sess.pickup_method or not sess.pickup_date:
        line_reply(reply_token, [text_msg("請先完成取貨方式與日期選擇。"), flex_main_menu()])
        return
    if not sess.receiver_name or not sess.phone:
        line_reply(reply_token, [text_msg("請先填寫姓名與電話。")])
        return
    if sess.pickup_method == "宅配" and not sess.address:
        line_reply(reply_token, [text_msg("請先填寫宅配地址。")])
        return

    order_id = new_order_id()
    subtotal = calc_subtotal(sess)
    ship_fee = calc_shipping_fee(sess)
    grand = subtotal + ship_fee

    # note
    if sess.pickup_method == "宅配":
        note = f"宅配 期望到貨:{sess.pickup_date} | {sess.receiver_name} | {sess.phone} | {sess.address}"
    else:
        note = f"店取 {sess.pickup_date} {sess.pickup_time} | {sess.receiver_name} | {sess.phone}"

    # Calendar create (optional)
    calendar_event_id = ""
    try:
        if ENABLE_CALENDAR:
            calendar_event_id = ensure_calendar_event_create(sess, order_id, note)
    except Exception as e:
        print("[GCAL] create skipped:", repr(e))

    # Sheets write
    try:
        write_order_A_B(sess, order_id, calendar_event_id=calendar_event_id, status="UNPAID", note=note)
    except Exception as e:
        print("[ORDER] sheets failed:", repr(e))
        # still reply customer (do not crash)
        line_reply(reply_token, [text_msg("系統忙碌中，訂單建立失敗，請稍後再試。")])
        return

    # Notify admins (simple push)
    try:
        if ADMIN_USER_IDS:
            for uid in ADMIN_USER_IDS:
                line_push(uid, [text_msg(f"🆕 新訂單\n{order_id}\n{note}\n總計：NT${grand}")])
    except Exception as e:
        print("[ADMIN push] failed:", repr(e))

    # Customer reply
    pay = build_payment_info()
    pickup = build_pickup_info()
    line_reply(reply_token, [
        text_msg(f"✅ 訂單已建立（待轉帳）\n訂單編號：{order_id}\n總計：NT${grand}（含運費NT${ship_fee}）"),
        text_msg(pay),
        text_msg(pickup),
    ])

    # clear cart but keep last selections optional
    sess.cart.clear()
    sess.awaiting = ""
    # (你要保留 pickup_method/date/time 也可以；我先不清空，讓客人下次更快)
    # sess.pickup_method = ""
    # sess.pickup_date = ""
    # sess.pickup_time = ""


# =============================================================================
# Webhook routes
# =============================================================================

@app.get("/")
def health():
    return {
        "ok": True,
        "service": "uoo-order-bot",
        "min_days": MIN_DAYS,
        "max_days": MAX_DAYS,
        "sheets": bool(SPREADSHEET_ID),
        "calendar": bool(ENABLE_CALENDAR),
    }

@app.post("/callback")
async def callback(request: Request, x_line_signature: str = Header(default="")):
    body = await request.body()

    if not x_line_signature:
        return PlainTextResponse("missing signature", status_code=400)
    if not verify_line_signature(body, x_line_signature):
        return PlainTextResponse("bad signature", status_code=400)

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        return PlainTextResponse("bad json", status_code=400)

    events = payload.get("events", []) or []
    for ev in events:
        try:
            # dedup
            ev_id = ev.get("webhookEventId") or ""
            if not ev_id:
                ev_id = hashlib.sha1(json.dumps(ev, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
            if dedup_seen(ev_id):
                continue

            etype = ev.get("type")
            user_id = (ev.get("source") or {}).get("userId", "")
            reply_token = ev.get("replyToken", "")

            if not user_id:
                continue

            sess = get_session(user_id)
            if not sess.display_name:
                prof = get_profile(user_id)
                sess.display_name = prof.get("displayName", "") or ""

            if etype == "follow":
                line_reply(reply_token, [flex_main_menu()])
                continue

            if etype == "message":
                msg = ev.get("message") or {}
                if msg.get("type") == "text":
                    handle_text(reply_token, sess, msg.get("text", ""))
                else:
                    line_reply(reply_token, [flex_main_menu()])
                continue

            if etype == "postback":
                pb = ev.get("postback") or {}
                handle_postback(reply_token, sess, pb.get("data", ""))
                continue

        except Exception as e:
            # never crash webhook
            print("[WEBHOOK] event error:", repr(e))

    gc_sessions()
    return JSONResponse({"ok": True})
