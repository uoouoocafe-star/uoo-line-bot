# main.py
# UooUoo Cafe - LINE Dessert Order Bot (Stable Full Version)
# - FastAPI webhook
# - Google Sheets: A(orders summary), B(items/detail), C(status log), c_log(raw log)
# - Menu from items sheet (editable): item_id, name, price, flavor_list, spec, enabled, min_qty
# - Flow: menu -> choose item -> choose flavor (if any) -> choose qty -> cart -> checkout -> method/date/time -> name/phone/address -> write sheets
# - Rule A: Dacquoise (and any item with min_qty>1) enforces minimum total qty across flavors
# - Rule B: Flavor first, then qty
# - Optional Google Calendar event create
# - Webhook dedup to avoid double-writes
# - Customer-facing debug removed

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

import httpx
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# =============================================================================
# App
# =============================================================================
app = FastAPI(title="UooUoo LINE Dessert Order Bot", version="stable-v1")


# =============================================================================
# Env helpers
# =============================================================================
def _env_first(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default

def _env_int(*keys: str, default: int) -> int:
    raw = _env_first(*keys, default=str(default))
    raw = str(raw).strip()
    m = re.search(r"-?\d+", raw)
    return int(m.group(0)) if m else default

def _env_csv(*keys: str) -> List[str]:
    s = _env_first(*keys, default="")
    return [x.strip() for x in str(s).split(",") if x.strip()]


# =============================================================================
# LINE config
# =============================================================================
CHANNEL_ACCESS_TOKEN = _env_first("CHANNEL_ACCESS_TOKEN", "LINE_CHANNEL_ACCESS_TOKEN")
CHANNEL_SECRET = _env_first("CHANNEL_SECRET", "LINE_CHANNEL_SECRET")

ADMIN_TOKEN = _env_first("ADMIN_TOKEN", default="")
ADMIN_USER_IDS = set(_env_csv("ADMIN_USER_IDS"))


# =============================================================================
# Sheets config
# =============================================================================
GSHEET_ID = _env_first("GSHEET_ID", "SPREADSHEET_ID")

# Preferred explicit names
SHEET_A_NAME = _env_first("SHEET_A_NAME", "GSHEET_SHEET_NAME", default="orders")  # summary
SHEET_B_NAME = _env_first("SHEET_B_NAME", "GSHEET_TAB", default="orders_items")    # items detail
SHEET_C_NAME = _env_first("SHEET_C_NAME", default="C")                            # status log
SHEET_CLOG_NAME = _env_first("SHEET_CLOG_NAME", "SHEET_C_LOG_NAME", default="c_log")  # raw log
SHEET_ITEMS_NAME = _env_first("SHEET_ITEMS_NAME", default="items")                # menu items

# Business rules
MIN_DAYS = _env_int("MIN_DAYS", default=3)
MAX_DAYS = _env_int("MAX_DAYS", default=14)
ORDER_CUTOFF_HOURS = _env_int("ORDER_CUTOFF_HOURS", default=0)

CLOSED_DATES = set(_env_csv("CLOSED_DATES"))            # "2026-01-01,2026-01-02"
CLOSED_WEEKDAYS = set(int(x) for x in _env_csv("CLOSED_WEEKDAYS") if x.isdigit())  # "0,1" Mon=0

STORE_ADDRESS = _env_first("STORE_ADDRESS", default="").strip()

BANK_NAME = _env_first("BANK_NAME", default="").strip()
BANK_CORE = _env_first("BANK_CORE", default="").strip()
BANK_ACCOUNT = _env_first("BANK_ACCOUNT", default="").strip()


# =============================================================================
# Google Service Account
# =============================================================================
GOOGLE_SERVICE_ACCOUNT_B64 = _env_first("GOOGLE_SERVICE_ACCOUNT_B64", default="")
# Optional alternative forms (keep compatibility)
GOOGLE_SERVICE_ACCOUNT_JSON = _env_first("GOOGLE_SERVICE_ACCOUNT_JSON", default="")
GOOGLE_SERVICE_ACCOUNT_FILE = _env_first("GOOGLE_SERVICE_ACCOUNT_FILE", default="")

# Calendar (optional)
GCAL_CALENDAR_ID = _env_first("GCAL_CALENDAR_ID", default="")
GCAL_TIMEZONE = _env_first("GCAL_TIMEZONE", "TZ", default="Asia/Taipei")


# =============================================================================
# Constants
# =============================================================================
LINE_REPLY_ENDPOINT = "https://api.line.me/v2/bot/message/reply"
LINE_PUSH_ENDPOINT = "https://api.line.me/v2/bot/message/push"
LINE_PROFILE_ENDPOINT = "https://api.line.me/v2/bot/profile"

SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CAL_SCOPES = ["https://www.googleapis.com/auth/calendar"]


# =============================================================================
# Webhook dedup
# =============================================================================
_DEDUP_LOCK = threading.Lock()
_DEDUP_TTL_SEC = 60 * 10
_DEDUP: Dict[str, float] = {}  # key -> expires_at

def _dedup_seen(key: str) -> bool:
    now = time.time()
    with _DEDUP_LOCK:
        expired = [k for k, exp in _DEDUP.items() if exp <= now]
        for k in expired:
            _DEDUP.pop(k, None)
        if key in _DEDUP:
            return True
        _DEDUP[key] = now + _DEDUP_TTL_SEC
        return False


# =============================================================================
# Simple timezone helpers
# =============================================================================
def _now_tw() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).astimezone(dt.timezone(dt.timedelta(hours=8)))

def _today_tw() -> dt.date:
    return _now_tw().date()

def _date_to_str(d: dt.date) -> str:
    return d.isoformat()

def _fmt_currency(n: int) -> str:
    try:
        return f"NT${int(n):,}"
    except Exception:
        return f"NT${n}"


# =============================================================================
# LINE helpers
# =============================================================================
def _line_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"}

def _hmac_sha256(secret: str, body: bytes) -> str:
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")

def _verify_line_signature(body: bytes, signature: str) -> bool:
    if not CHANNEL_SECRET or not signature:
        return False
    expected = _hmac_sha256(CHANNEL_SECRET, body)
    return hmac.compare_digest(expected, signature)

async def line_reply(reply_token: str, messages: List[Dict[str, Any]]) -> None:
    if not reply_token:
        return
    payload = {"replyToken": reply_token, "messages": messages}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(LINE_REPLY_ENDPOINT, headers=_line_headers(), json=payload)
        if r.status_code >= 400:
            print("[LINE] reply failed:", r.status_code, r.text[:300])

async def line_push(to_user_id: str, messages: List[Dict[str, Any]]) -> None:
    if not to_user_id:
        return
    payload = {"to": to_user_id, "messages": messages}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(LINE_PUSH_ENDPOINT, headers=_line_headers(), json=payload)
        if r.status_code >= 400:
            print("[LINE] push failed:", r.status_code, r.text[:300])

async def get_profile(user_id: str) -> Tuple[str, str]:
    if not user_id:
        return "", ""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{LINE_PROFILE_ENDPOINT}/{user_id}", headers=_line_headers())
            if r.status_code == 200:
                j = r.json()
                return j.get("displayName", "") or "", j.get("pictureUrl", "") or ""
    except Exception:
        pass
    return "", ""

def is_admin(user_id: str, token: str = "") -> bool:
    if token and ADMIN_TOKEN and token == ADMIN_TOKEN:
        return True
    return user_id in ADMIN_USER_IDS


# =============================================================================
# Google clients
# =============================================================================
_google_lock = threading.Lock()
_sheets_svc = None
_cal_svc = None

def _load_sa_credentials(scopes: List[str]) -> Credentials:
    if GOOGLE_SERVICE_ACCOUNT_B64:
        info = json.loads(base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64).decode("utf-8"))
        return Credentials.from_service_account_info(info, scopes=scopes)
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        return Credentials.from_service_account_info(info, scopes=scopes)
    if GOOGLE_SERVICE_ACCOUNT_FILE:
        return Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_FILE, scopes=scopes)
    raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_* (B64/JSON/FILE)")

def sheets_service():
    global _sheets_svc
    with _google_lock:
        if _sheets_svc is None:
            creds = _load_sa_credentials(SHEETS_SCOPES)
            _sheets_svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return _sheets_svc

def cal_service():
    global _cal_svc
    with _google_lock:
        if _cal_svc is None:
            creds = _load_sa_credentials(CAL_SCOPES)
            _cal_svc = build("calendar", "v3", credentials=creds, cache_discovery=False)
        return _cal_svc


# =============================================================================
# Sheets repo (header-driven)
# =============================================================================
class SheetRepo:
    def __init__(self, spreadsheet_id: str):
        self.sid = spreadsheet_id

    def get_values(self, sheet: str, a1: str) -> List[List[Any]]:
        rng = f"{sheet}!{a1}"
        resp = sheets_service().spreadsheets().values().get(
            spreadsheetId=self.sid,
            range=rng,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
        return resp.get("values", [])

    def update_values(self, sheet: str, a1: str, values: List[List[Any]]) -> None:
        rng = f"{sheet}!{a1}"
        sheets_service().spreadsheets().values().update(
            spreadsheetId=self.sid,
            range=rng,
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

    def append_values(self, sheet: str, values: List[List[Any]]) -> None:
        rng = f"{sheet}!A:ZZ"
        sheets_service().spreadsheets().values().append(
            spreadsheetId=self.sid,
            range=rng,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()

    def ensure_headers(self, sheet: str, required: List[str]) -> Dict[str, int]:
        # Read first row
        rows = self.get_values(sheet, "1:1")
        header = [str(x).strip() for x in (rows[0] if rows else [])]
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
            self.update_values(sheet, "1:1", [header])

        return {h: i for i, h in enumerate(header)}

    def append_by_header(self, sheet: str, header_order: List[str], row_dict: Dict[str, Any]) -> None:
        row = []
        for h in header_order:
            v = row_dict.get(h, "")
            row.append("" if v is None else v)
        self.append_values(sheet, [row])

    def find_first_row(self, sheet: str, header_map: Dict[str, int], key_header: str, key_value: str, max_scan: int = 5000) -> Optional[int]:
        if key_header not in header_map:
            return None
        idx = header_map[key_header]
        data = self.get_values(sheet, f"A2:ZZ{max_scan+1}")
        for rno, row in enumerate(data, start=2):
            if idx < len(row) and str(row[idx]).strip() == str(key_value).strip():
                return rno
        return None

    def update_cells_by_header(self, sheet: str, row_index_1based: int, header_map: Dict[str, int], updates: Dict[str, Any]) -> None:
        data = []
        for k, v in updates.items():
            if k not in header_map:
                # extend header
                header_map = self.ensure_headers(sheet, list(header_map.keys()) + [k])
            col_idx_1 = header_map[k] + 1
            col_letter = self._col_to_letter(col_idx_1)
            rng = f"{sheet}!{col_letter}{row_index_1based}:{col_letter}{row_index_1based}"
            data.append({"range": rng, "values": [[("" if v is None else str(v))]]})
        if not data:
            return
        sheets_service().spreadsheets().values().batchUpdate(
            spreadsheetId=self.sid,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()

    @staticmethod
    def _col_to_letter(n: int) -> str:
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s


repo = SheetRepo(GSHEET_ID) if GSHEET_ID else None


# =============================================================================
# Data models
# =============================================================================
@dataclass
class MenuItem:
    item_id: str
    name: str
    price: int
    flavor_list: List[str] = field(default_factory=list)
    spec: str = ""
    enabled: bool = True
    min_qty: int = 1


@dataclass
class CartItem:
    base_item_id: str
    name: str
    unit_price: int
    qty: int = 1
    flavor: str = ""
    spec: str = ""

    @property
    def key(self) -> str:
        # separate by flavor
        return f"{self.base_item_id}||{self.flavor}".strip()

    @property
    def subtotal(self) -> int:
        return int(self.unit_price) * int(self.qty)


@dataclass
class OrderDraft:
    user_id: str
    display_name: str
    order_id: str
    pickup_method: str  # 店取 / 宅配
    pickup_date: str
    pickup_time: str
    receiver_name: str
    phone: str
    address: str
    items: List[CartItem]
    shipping_fee: int
    note: str
    calendar_event_id: str = ""

    @property
    def amount(self) -> int:
        return sum(i.subtotal for i in self.items)

    @property
    def grand_total(self) -> int:
        return self.amount + int(self.shipping_fee)


# =============================================================================
# Session store
# =============================================================================
_SESS_LOCK = threading.Lock()
_SESS: Dict[str, Dict[str, Any]] = {}
_SESS_TTL = 60 * 60  # 1 hr

def _sess_get(user_id: str) -> Dict[str, Any]:
    now = time.time()
    with _SESS_LOCK:
        dead = [uid for uid, s in _SESS.items() if s.get("_exp", 0) <= now]
        for uid in dead:
            _SESS.pop(uid, None)
        s = _SESS.get(user_id)
        if not s:
            s = {"_exp": now + _SESS_TTL, "cart": {}}
            _SESS[user_id] = s
        else:
            s["_exp"] = now + _SESS_TTL
        return s

def _sess_clear(user_id: str) -> None:
    with _SESS_LOCK:
        _SESS.pop(user_id, None)


# =============================================================================
# Items/menu loading
# =============================================================================
ITEMS_HEADERS = ["item_id", "name", "price", "flavor_list", "spec", "enabled", "min_qty"]

def _parse_flavors(s: Any) -> List[str]:
    if s is None:
        return []
    text = str(s).strip()
    if not text:
        return []
    return [x.strip() for x in text.split(",") if x.strip()]

def load_menu_items() -> List[MenuItem]:
    # If items sheet not built yet, return fallback (your current list)
    fallback = [
        MenuItem(
            item_id="dacquoise",
            name="達克瓦茲（95/顆）",
            price=95,
            flavor_list=["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"],
            spec="一律95元/顆",
            enabled=True,
            min_qty=2,
        ),
        MenuItem(item_id="scone_original", name="原味司康（65/顆）", price=65, enabled=True, min_qty=1),
        MenuItem(item_id="canele_original", name="原味可麗露（90/顆）", price=90, enabled=True, min_qty=1),
        MenuItem(
            item_id="toast_cream",
            name="伊思尼奶酥厚片（85/片）",
            price=85,
            flavor_list=["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"],
            spec="一律85元/片",
            enabled=True,
            min_qty=1,
        ),
    ]

    if not repo:
        return fallback

    try:
        # Ensure headers exist (so you可以直接貼資料)
        hm = repo.ensure_headers(SHEET_ITEMS_NAME, ITEMS_HEADERS)
        values = repo.get_values(SHEET_ITEMS_NAME, "A1:ZZ500")
        if not values or len(values) < 2:
            return fallback

        header = [str(x).strip() for x in values[0]]
        idx = {h: header.index(h) for h in header if h}

        items: List[MenuItem] = []
        for row in values[1:]:
            def g(key: str, default: str = "") -> str:
                i = idx.get(key, -1)
                if i < 0 or i >= len(row):
                    return default
                return "" if row[i] is None else str(row[i]).strip()

            item_id = g("item_id")
            name = g("name")
            if not item_id or not name:
                continue

            price_raw = g("price", "0")
            m = re.search(r"\d+", price_raw)
            price = int(m.group(0)) if m else 0

            enabled_raw = g("enabled", "TRUE").upper()
            enabled = enabled_raw not in ("FALSE", "0", "NO", "N")

            min_qty_raw = g("min_qty", "1")
            m2 = re.search(r"\d+", min_qty_raw)
            min_qty = int(m2.group(0)) if m2 else 1
            if min_qty < 1:
                min_qty = 1

            flavors = _parse_flavors(g("flavor_list", ""))
            spec = g("spec", "")

            if enabled:
                items.append(MenuItem(
                    item_id=item_id,
                    name=name,
                    price=price,
                    flavor_list=flavors,
                    spec=spec,
                    enabled=True,
                    min_qty=min_qty,
                ))

        return items if items else fallback

    except Exception as e:
        print("[Items] load failed:", repr(e))
        return fallback

def get_menu_item(item_id: str) -> Optional[MenuItem]:
    for it in load_menu_items():
        if it.item_id == item_id:
            return it
    return None


# =============================================================================
# Business date rules
# =============================================================================
def _is_closed_date(d: dt.date) -> bool:
    if _date_to_str(d) in CLOSED_DATES:
        return True
    if d.weekday() in CLOSED_WEEKDAYS:
        return True
    return False

def list_available_dates() -> List[str]:
    today = _today_tw()
    out = []
    for delta in range(MIN_DAYS, MAX_DAYS + 1):
        d = today + dt.timedelta(days=delta)
        if _is_closed_date(d):
            continue
        out.append(_date_to_str(d))
    return out

PICKUP_TIMES = ["11:00-12:00", "12:00-14:00", "14:00-16:00"]


# =============================================================================
# Money
# =============================================================================
def calc_shipping_fee(pickup_method: str, amount: int) -> int:
    return 180 if pickup_method == "宅配" else 0


# =============================================================================
# Flex Builders
# =============================================================================
def _text_message(text: str) -> Dict[str, Any]:
    return {"type": "text", "text": text}

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
                    {"type": "text", "text": "請選擇要做什麼：", "size": "md", "color": "#666666", "wrap": True},
                    {"type": "button", "style": "primary",
                     "action": {"type": "postback", "label": "🍰 甜點選單", "data": "PB:MENU"}},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": "🛒 查看購物車", "data": "PB:CART"}},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": "📦 取貨說明", "data": "PB:HOW_PICKUP"}},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": "💳 付款資訊", "data": "PB:PAY_INFO"}},
                ],
            },
        },
    }

def flex_items_list(items: List[MenuItem]) -> Dict[str, Any]:
    blocks = []
    if not items:
        blocks.append({"type": "text", "text": "目前沒有可下單的品項。", "wrap": True})
    else:
        for it in items[:12]:
            blocks.append({
                "type": "box",
                "layout": "horizontal",
                "spacing": "md",
                "contents": [
                    {
                        "type": "box", "layout": "vertical", "flex": 1,
                        "contents": [
                            {"type": "text", "text": it.name, "weight": "bold", "size": "md", "wrap": True},
                            {"type": "text", "text": _fmt_currency(it.price), "size": "sm", "color": "#666666", "wrap": True},
                            {"type": "text", "text": f"最低購買：{it.min_qty}", "size": "xs", "color": "#999999", "wrap": True} if it.min_qty > 1 else {"type":"text","text":"", "size":"xs", "color":"#FFFFFF"},
                        ],
                    },
                    {
                        "type": "button", "style": "primary", "height": "sm",
                        "action": {"type": "postback", "label": "選擇", "data": f"PB:SELECT_ITEM:{it.item_id}"},
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
                "type": "box", "layout": "vertical", "spacing": "md",
                "contents": [
                    {"type": "text", "text": "甜點選單", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "separator"},
                    *blocks,
                    {"type": "separator"},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": "🛒 查看購物車", "data": "PB:CART"}},
                ],
            },
        },
    }

def flex_choose_flavor(item: MenuItem) -> Dict[str, Any]:
    btns = []
    for fl in item.flavor_list[:12]:
        btns.append({
            "type": "button", "style": "secondary",
            "action": {"type": "postback", "label": fl, "data": f"PB:FLAVOR:{item.item_id}:{fl}"},
        })
    return {
        "type": "flex",
        "altText": "選擇口味",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box", "layout": "vertical", "spacing": "md",
                "contents": [
                    {"type": "text", "text": "選擇口味", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "text", "text": item.name, "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "separator"},
                    *btns,
                ],
            },
        },
    }

def flex_choose_qty(item_id: str, flavor: str, name: str) -> Dict[str, Any]:
    # Quick qty buttons: 1..6
    btns = []
    for q in [1, 2, 3, 4, 5, 6]:
        btns.append({
            "type": "button", "style": "secondary",
            "action": {"type": "postback", "label": str(q), "data": f"PB:QTY:{item_id}:{flavor}:{q}"},
        })
    title = "選擇盒數"
subtitle = f"{name}" + (f"（{flavor}）" if flavor else "") + "｜1 = 1盒"
    return {
        "type": "flex",
        "altText": "選擇數量",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box", "layout": "vertical", "spacing": "md",
                "contents": [
                    {"type": "text", "text": title, "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "text", "text": subtitle, "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "separator"},
                    *btns,
                ],
            },
        },
    }

def flex_cart(cart: List[CartItem], pickup_method: str = "", pickup_date: str = "", pickup_time: str = "", shipping_fee: int = 0) -> Dict[str, Any]:
    lines = []
    total = 0
    for ci in cart:
        total += ci.subtotal
        label = ci.name + (f"（{ci.flavor}）" if ci.flavor else "")
        lines.append({
            "type": "box",
            "layout": "horizontal",
            "contents": [
                {"type": "text", "text": f"{label} ×{ci.qty}", "flex": 1, "wrap": True},
                {"type": "text", "text": _fmt_currency(ci.subtotal), "align": "end", "wrap": True},
            ],
        })
    if not lines:
        lines.append({"type": "text", "text": "（購物車是空的）", "size": "sm", "color": "#666666", "wrap": True})

    grand_total = total + int(shipping_fee)

    meta = []
    if pickup_method:
        meta.append({"type": "text", "text": f"取貨方式：{pickup_method}", "size": "sm", "color": "#666666", "wrap": True})
    if pickup_date:
        meta.append({"type": "text", "text": f"日期：{pickup_date}", "size": "sm", "color": "#666666", "wrap": True})
    if pickup_time:
        meta.append({"type": "text", "text": f"時段：{pickup_time}", "size": "sm", "color": "#666666", "wrap": True})

    return {
        "type": "flex",
        "altText": "結帳內容",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box", "layout": "vertical", "spacing": "md",
                "contents": [
                    {"type": "text", "text": "結帳內容", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "separator"},
                    *lines,
                    {"type": "separator"},
                    *meta,
                    {
                        "type": "box", "layout": "vertical", "spacing": "xs",
                        "contents": [
                            {"type": "box", "layout": "horizontal", "contents": [
                                {"type": "text", "text": "小計", "flex": 1, "wrap": True},
                                {"type": "text", "text": _fmt_currency(total), "align": "end", "wrap": True},
                            ]},
                            {"type": "box", "layout": "horizontal", "contents": [
                                {"type": "text", "text": "運費", "flex": 1, "wrap": True},
                                {"type": "text", "text": _fmt_currency(shipping_fee), "align": "end", "wrap": True},
                            ]},
                            {"type": "box", "layout": "horizontal", "contents": [
                                {"type": "text", "text": "合計", "flex": 1, "weight": "bold", "wrap": True},
                                {"type": "text", "text": _fmt_currency(grand_total), "align": "end", "weight": "bold", "wrap": True},
                            ]},
                        ],
                    },
                    {"type": "separator"},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": "🔧 修改品項", "data": "PB:EDIT_CART"}},
                    {"type": "button", "style": "primary",
                     "action": {"type": "postback", "label": "🧾 前往結帳", "data": "PB:CHECKOUT"}},
                    {"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": "➕ 繼續加購", "data": "PB:MENU"}},
                ],
            },
        },
    }

def flex_payment_info() -> Dict[str, Any]:
    lines = ["付款資訊"]
    if BANK_NAME or BANK_CORE or BANK_ACCOUNT:
        if BANK_NAME:
            lines.append(f"銀行：{BANK_NAME}")
        if BANK_CORE:
            lines.append(f"代碼：{BANK_CORE}")
        if BANK_ACCOUNT:
            lines.append(f"帳號：{BANK_ACCOUNT}")
        lines.append("匯款後請回覆：訂單編號＋末五碼（或截圖）")
    else:
        lines.append("（尚未設定匯款資訊）")
    return {"type": "flex", "altText": "付款資訊", "contents": {"type": "bubble", "size": "giga",
            "body": {"type":"box","layout":"vertical","spacing":"md","contents":[
                {"type":"text","text":"付款資訊","weight":"bold","size":"xl","wrap":True},
                {"type":"separator"},
                {"type":"text","text":"\n".join(lines),"wrap":True},
            ]}}}

def flex_pickup_info() -> Dict[str, Any]:
    txt = [
        f"店取：下單後可選 {MIN_DAYS}–{MAX_DAYS} 天內日期與時段。",
        f"宅配：選擇希望到貨日期（我們會依製作進度安排出貨）。",
    ]
    if STORE_ADDRESS:
        txt.append(f"店址：{STORE_ADDRESS}")
    return {"type": "flex", "altText": "取貨說明", "contents": {"type":"bubble","size":"giga",
            "body":{"type":"box","layout":"vertical","spacing":"md","contents":[
                {"type":"text","text":"取貨說明","weight":"bold","size":"xl","wrap":True},
                {"type":"separator"},
                {"type":"text","text":"\n".join(txt),"wrap":True},
            ]}}}

def flex_admin_actions(order_id: str) -> Dict[str, Any]:
    return {
        "type": "flex",
        "altText": "商家管理",
        "contents": {
            "type": "bubble", "size": "giga",
            "body": {
                "type": "box", "layout": "vertical", "spacing": "md",
                "contents": [
                    {"type": "text", "text": "商家管理", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "text", "text": f"訂單編號：{order_id}", "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "separator"},
                    {"type": "button", "style": "primary",
                     "action": {"type": "postback", "label": "✅ 已收款 (PAID)", "data": f"PB:ADMIN_PAID:{order_id}"}},
                    {"type": "button", "style": "primary",
                     "action": {"type": "postback", "label": "📣 已做好 (READY)", "data": f"PB:ADMIN_READY:{order_id}"}},
                    {"type": "button", "style": "primary",
                     "action": {"type": "postback", "label": "🚚 已出貨 (SHIPPED)", "data": f"PB:ADMIN_SHIPPED:{order_id}"}},
                ],
            },
        },
    }


# =============================================================================
# Order id
# =============================================================================
def new_order_id() -> str:
    # UOO-YYYYMMDD-XXXX
    ymd = _today_tw().strftime("%Y%m%d")
    tail = str(uuid.uuid4().int)[-4:]
    return f"UOO-{ymd}-{tail}"


# =============================================================================
# Cart operations (flavor-aware)
# =============================================================================
def _get_cart(sess: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    # cart_key -> dict
    if "cart" not in sess or not isinstance(sess["cart"], dict):
        sess["cart"] = {}
    return sess["cart"]

def cart_list(sess: Dict[str, Any]) -> List[CartItem]:
    out = []
    cart = _get_cart(sess)
    for k, v in cart.items():
        out.append(CartItem(
            base_item_id=str(v.get("base_item_id","")),
            name=str(v.get("name","")),
            unit_price=int(v.get("unit_price",0) or 0),
            qty=int(v.get("qty",0) or 0),
            flavor=str(v.get("flavor","")),
            spec=str(v.get("spec","")),
        ))
    return out

def cart_add_or_set(sess: Dict[str, Any], base_item: MenuItem, flavor: str, qty: int) -> None:
    cart = _get_cart(sess)
    key = f"{base_item.item_id}||{flavor}".strip()
    if key in cart:
        cart[key]["qty"] = int(cart[key].get("qty", 0) or 0) + int(qty)
    else:
        cart[key] = {
            "base_item_id": base_item.item_id,
            "name": base_item.name,
            "unit_price": base_item.price,
            "qty": int(qty),
            "flavor": flavor,
            "spec": base_item.spec,
        }

def cart_set_qty(sess: Dict[str, Any], key: str, new_qty: int) -> None:
    cart = _get_cart(sess)
    if key not in cart:
        return
    if new_qty <= 0:
        cart.pop(key, None)
    else:
        cart[key]["qty"] = int(new_qty)

def cart_clear(sess: Dict[str, Any]) -> None:
    sess["cart"] = {}

def cart_totals(sess: Dict[str, Any]) -> Tuple[int, int]:
    cart = cart_list(sess)
    amount = sum(i.subtotal for i in cart)
    shipping_fee = calc_shipping_fee(sess.get("pickup_method",""), amount)
    return amount, shipping_fee


# =============================================================================
# Rule A: min_qty enforcement (across flavors per base item)
# =============================================================================
def validate_min_qty(sess: Dict[str, Any]) -> Tuple[bool, str]:
    cart = cart_list(sess)
    if not cart:
        return False, "購物車是空的，請先加入品項。"

    # sum qty by base_item_id
    sums: Dict[str, int] = {}
    for ci in cart:
        sums[ci.base_item_id] = sums.get(ci.base_item_id, 0) + int(ci.qty)

    # check against menu min_qty
    menu = {it.item_id: it for it in load_menu_items()}
    for base_id, total_qty in sums.items():
        it = menu.get(base_id)
        if not it:
            continue
        if it.min_qty > 1 and total_qty < it.min_qty:
            return False, f"「{it.name}」最低購買數量為 {it.min_qty}，目前只有 {total_qty}。"

    return True, ""


# =============================================================================
# Sheets writing (A/B/C/C_LOG)
# =============================================================================
A_HEADERS = [
    "created_at","user_id","display_name","order_id","items_json",
    "pickup_method","pickup_date","pickup_time","receiver_name","phone","address",
    "note","amount","shipping_fee","grand_total","payment_status","ship_status","status"
]
B_HEADERS = [
    "created_at","order_id","item_id","item_name","spec","flavor","qty","unit_price","subtotal",
    "pickup_method","pickup_date","pickup_time","receiver_name","phone","address","status"
]
C_HEADERS = [
    "created_at","order_id","flow_type","method","amount","shipping_fee","grand_total","status","note"
]
CLOG_HEADERS = ["created_at","order_id","event","payload"]

def _items_json(order_items: List[CartItem]) -> str:
    return json.dumps([
        {
            "item_id": i.base_item_id,
            "name": i.name,
            "qty": i.qty,
            "unit_price": i.unit_price,
            "subtotal": i.subtotal,
            "flavor": i.flavor,
            "spec": i.spec,
        } for i in order_items
    ], ensure_ascii=False)

def ensure_sheets_headers() -> None:
    if not repo:
        return
    repo.ensure_headers(SHEET_A_NAME, A_HEADERS)
    repo.ensure_headers(SHEET_B_NAME, B_HEADERS)
    repo.ensure_headers(SHEET_C_NAME, C_HEADERS)
    repo.ensure_headers(SHEET_CLOG_NAME, CLOG_HEADERS)
    repo.ensure_headers(SHEET_ITEMS_NAME, ITEMS_HEADERS)

def write_order_to_sheets(order: OrderDraft) -> None:
    if not repo:
        return
    ensure_sheets_headers()
    created_at = _now_tw().replace(microsecond=0).isoformat(sep=" ", timespec="seconds")
    items_json = _items_json(order.items)

    # A summary
    a_row = {
        "created_at": created_at,
        "user_id": order.user_id,
        "display_name": order.display_name,
        "order_id": order.order_id,
        "items_json": items_json,
        "pickup_method": order.pickup_method,
        "pickup_date": order.pickup_date,
        "pickup_time": order.pickup_time,
        "receiver_name": order.receiver_name,
        "phone": order.phone,
        "address": order.address,
        "note": order.note,
        "amount": str(order.amount),
        "shipping_fee": str(order.shipping_fee),
        "grand_total": str(order.grand_total),
        "payment_status": "UNPAID",
        "ship_status": "",
        "status": "ORDER",
    }
    repo.append_by_header(SHEET_A_NAME, A_HEADERS, a_row)

    # B detail rows
    for i in order.items:
        b_row = {
            "created_at": created_at,
            "order_id": order.order_id,
            "item_id": i.base_item_id,
            "item_name": i.name,
            "spec": i.spec,
            "flavor": i.flavor,
            "qty": str(i.qty),
            "unit_price": str(i.unit_price),
            "subtotal": str(i.subtotal),
            "pickup_method": order.pickup_method,
            "pickup_date": order.pickup_date,
            "pickup_time": order.pickup_time,
            "receiver_name": order.receiver_name,
            "phone": order.phone,
            "address": order.address,
            "status": "UNPAID",
        }
        repo.append_by_header(SHEET_B_NAME, B_HEADERS, b_row)

    # C status log
    c_row = {
        "created_at": created_at,
        "order_id": order.order_id,
        "flow_type": "ORDER",
        "method": order.pickup_method,
        "amount": str(order.amount),
        "shipping_fee": str(order.shipping_fee),
        "grand_total": str(order.grand_total),
        "status": "ORDER",
        "note": order.note,
    }
    repo.append_by_header(SHEET_C_NAME, C_HEADERS, c_row)

    # c_log raw
    clog_row = {
        "created_at": created_at,
        "order_id": order.order_id,
        "event": "ORDER_CREATED",
        "payload": items_json,
    }
    repo.append_by_header(SHEET_CLOG_NAME, CLOG_HEADERS, clog_row)

def update_order_status(order_id: str, status: str) -> None:
    if not repo:
        return
    ensure_sheets_headers()
    now_s = _now_tw().replace(microsecond=0).isoformat(sep=" ", timespec="seconds")

    # Update A row by order_id
    hm = repo.ensure_headers(SHEET_A_NAME, A_HEADERS)
    row_idx = repo.find_first_row(SHEET_A_NAME, hm, "order_id", order_id)
    if row_idx:
        updates = {"status": status}
        if status == "PAID":
            updates["payment_status"] = "PAID"
        elif status in ("READY","SHIPPED"):
            updates["ship_status"] = status
        updates["updated_at"] = now_s
        repo.update_cells_by_header(SHEET_A_NAME, row_idx, hm, updates)

    # Append status to C
    c_row = {
        "created_at": now_s,
        "order_id": order_id,
        "flow_type": "STATUS",
        "method": "",
        "amount": "",
        "shipping_fee": "",
        "grand_total": "",
        "status": status,
        "note": "",
    }
    repo.append_by_header(SHEET_C_NAME, C_HEADERS, c_row)

    # c_log
    clog_row = {"created_at": now_s, "order_id": order_id, "event": f"STATUS_{status}", "payload": ""}
    repo.append_by_header(SHEET_CLOG_NAME, CLOG_HEADERS, clog_row)


# =============================================================================
# Optional: Google Calendar event creation
# =============================================================================
def create_calendar_event_for_order(order: OrderDraft) -> str:
    if not GCAL_CALENDAR_ID:
        return ""
    try:
        tz = GCAL_TIMEZONE or "Asia/Taipei"
        date = order.pickup_date
        if not date:
            return ""

        if order.pickup_method == "店取" and order.pickup_time:
            m = re.match(r"(\d{2}:\d{2})-(\d{2}:\d{2})", order.pickup_time.strip())
            if m:
                st, et = m.group(1), m.group(2)
            else:
                st, et = "12:00", "12:30"
            start_dt = f"{date}T{st}:00"
            end_dt = f"{date}T{et}:00"
            start_obj = {"dateTime": start_dt, "timeZone": tz}
            end_obj = {"dateTime": end_dt, "timeZone": tz}
        else:
            # delivery: all-day on that date
            d = dt.date.fromisoformat(date)
            start_obj = {"date": date}
            end_obj = {"date": (d + dt.timedelta(days=1)).isoformat()}

        title = f"UooUoo 訂單 {order.order_id} ({order.pickup_method})"
        desc = "\n".join([
            f"訂單：{order.order_id}",
            f"方式：{order.pickup_method}",
            f"日期：{order.pickup_date}",
            f"時段：{order.pickup_time}" if order.pickup_time else "",
            f"客人：{order.receiver_name} {order.phone}",
            f"地址：{order.address}" if order.address else "",
            f"金額：{_fmt_currency(order.grand_total)}",
        ]).strip()

        location = STORE_ADDRESS if order.pickup_method == "店取" else (order.address or "")
        event = {
            "summary": title,
            "description": desc,
            "location": location,
            "start": start_obj,
            "end": end_obj,
        }
        svc = cal_service()
        created = svc.events().insert(calendarId=GCAL_CALENDAR_ID, body=event).execute()
        return created.get("id", "") or ""
    except Exception as e:
        print("[GCAL] create event failed:", repr(e))
        return ""


# =============================================================================
# Checkout flow (text follow-ups)
# =============================================================================
async def handle_followup_text(user_id: str, reply_token: str, text: str) -> bool:
    sess = _sess_get(user_id)
    awaiting = sess.get("awaiting", "")

    if awaiting == "receiver_name":
        sess["receiver_name"] = (text or "").strip()
        sess["awaiting"] = "phone"
        await line_reply(reply_token, [_text_message("請輸入電話：")])
        return True

    if awaiting == "phone":
        sess["phone"] = (text or "").strip()
        if sess.get("pickup_method") == "宅配":
            sess["awaiting"] = "address"
            await line_reply(reply_token, [_text_message("請輸入宅配地址（完整地址）：")])
        else:
            sess["awaiting"] = ""
            await finalize_order(user_id, reply_token)
        return True

    if awaiting == "address":
        sess["address"] = (text or "").strip()
        sess["awaiting"] = ""
        await finalize_order(user_id, reply_token)
        return True

    return False


# =============================================================================
# Finalize order
# =============================================================================
async def finalize_order(user_id: str, reply_token: str) -> None:
    sess = _sess_get(user_id)
    ok, msg = validate_min_qty(sess)
    if not ok:
        await line_reply(reply_token, [_text_message(msg), flex_items_list(load_menu_items())])
        return

    cart = cart_list(sess)
    if not cart:
        await line_reply(reply_token, [_text_message("購物車是空的，無法建立訂單。")])
        return

    display_name, _ = await get_profile(user_id)

    pm = sess.get("pickup_method", "")
    pickup_date = sess.get("pickup_date", "")
    pickup_time = sess.get("pickup_time", "")
    receiver_name = sess.get("receiver_name", "") or display_name
    phone = sess.get("phone", "")
    address = sess.get("address", "") if pm == "宅配" else ""
    amount, shipping_fee = cart_totals(sess)

    oid = new_order_id()

    if pm == "宅配":
        note = f"期望到貨:{pickup_date} | 收件人:{receiver_name} | 電話:{phone} | 地址:{address}"
    else:
        note = f"店取 {pickup_date} {pickup_time} | {receiver_name} | {phone}"

    order = OrderDraft(
        user_id=user_id,
        display_name=display_name,
        order_id=oid,
        pickup_method=pm,
        pickup_date=pickup_date,
        pickup_time=pickup_time,
        receiver_name=receiver_name,
        phone=phone,
        address=address,
        items=cart,
        shipping_fee=shipping_fee,
        note=note,
        calendar_event_id="",
    )

    # Calendar (optional)
    if GCAL_CALENDAR_ID:
        order.calendar_event_id = create_calendar_event_for_order(order)

    # Sheets
    try:
        write_order_to_sheets(order)
    except Exception as e:
        print("[Order] write failed:", repr(e))
        await line_reply(reply_token, [_text_message("系統忙碌中，訂單建立失敗，請稍後再試。")])
        return

    # Customer receipt
    receipt_msgs = [
        flex_cart(cart, pm, pickup_date, pickup_time, shipping_fee),
        _text_message(f"✅ 訂單已建立（待轉帳）\n訂單編號：{oid}\n匯款後請回覆：訂單編號＋末五碼"),
    ]
    await line_reply(reply_token, receipt_msgs)

    # Clear cart + checkout fields
    cart_clear(sess)
    for k in ("awaiting", "receiver_name", "phone", "address"):
        sess.pop(k, None)


# =============================================================================
# Postback handlers
# =============================================================================
async def handle_text(user_id: str, reply_token: str, text: str) -> None:
    t = (text or "").strip()

    # Admin: type order id to get admin panel
    if t.startswith("UOO-") and is_admin(user_id):
        await line_reply(reply_token, [flex_admin_actions(t)])
        return

    # common entry words
    if t in ("選單", "menu", "開始", "start", "甜點", "我要下單", "點餐", "點甜點"):
        await line_reply(reply_token, [flex_main_menu()])
        return

    # default
    await line_reply(reply_token, [flex_main_menu()])

async def handle_postback(user_id: str, reply_token: str, data: str) -> None:
    data = (data or "").strip()
    sess = _sess_get(user_id)

    if data == "PB:MENU":
        await line_reply(reply_token, [flex_items_list(load_menu_items())])
        return

    if data.startswith("PB:SELECT_ITEM:"):
        item_id = data.split("PB:SELECT_ITEM:", 1)[1].strip()
        it = get_menu_item(item_id)
        if not it:
            await line_reply(reply_token, [_text_message("找不到這個品項，請重新選擇。"), flex_items_list(load_menu_items())])
            return
        # Rule B: flavor first if has flavors; otherwise go qty
        if it.flavor_list:
            await line_reply(reply_token, [flex_choose_flavor(it)])
        else:
            await line_reply(reply_token, [flex_choose_qty(it.item_id, "", it.name)])
        return

    if data.startswith("PB:FLAVOR:"):
        # PB:FLAVOR:item_id:flavor
        parts = data.split("PB:FLAVOR:", 1)[1].split(":", 1)
        if len(parts) != 2:
            await line_reply(reply_token, [_text_message("口味資料有誤，請重新選擇。"), flex_items_list(load_menu_items())])
            return
        item_id = parts[0].strip()
        flavor = parts[1].strip()
        it = get_menu_item(item_id)
        if not it:
            await line_reply(reply_token, [_text_message("找不到這個品項，請重新選擇。"), flex_items_list(load_menu_items())])
            return
        await line_reply(reply_token, [flex_choose_qty(it.item_id, flavor, it.name)])
        return

    if data.startswith("PB:QTY:"):
        # PB:QTY:item_id:flavor:qty
        rest = data.split("PB:QTY:", 1)[1]
        parts = rest.split(":")
        if len(parts) < 3:
            await line_reply(reply_token, [_text_message("數量資料有誤，請重新選擇。")])
            return
        item_id = parts[0].strip()
        flavor = parts[1].strip()
        qty_s = parts[2].strip()
        qty = int(qty_s) if qty_s.isdigit() else 1

        it = get_menu_item(item_id)
        if not it:
            await line_reply(reply_token, [_text_message("找不到這個品項，請重新選擇。")])
            return

        cart_add_or_set(sess, it, flavor, qty)
        await line_reply(reply_token, [_text_message("已加入購物車。"), flex_main_menu()])
        return

    if data == "PB:CART":
        cart = cart_list(sess)
        pm = sess.get("pickup_method", "")
        pickup_date = sess.get("pickup_date", "")
        pickup_time = sess.get("pickup_time", "")
        amount, shipping_fee = cart_totals(sess)
        await line_reply(reply_token, [flex_cart(cart, pm, pickup_date, pickup_time, shipping_fee)])
        return

    if data == "PB:HOW_PICKUP":
        await line_reply(reply_token, [flex_pickup_info()])
        return

    if data == "PB:PAY_INFO":
        await line_reply(reply_token, [flex_payment_info()])
        return

    if data == "PB:EDIT_CART":
        cart = cart_list(sess)
        if not cart:
            await line_reply(reply_token, [_text_message("購物車目前是空的。"), flex_items_list(load_menu_items())])
            return

        # quick edit: + / -
        lines = []
        cart_map = _get_cart(sess)
        for key, v in cart_map.items():
            name = str(v.get("name",""))
            flavor = str(v.get("flavor",""))
            qty = int(v.get("qty",1) or 1)
            label = name + (f"（{flavor}）" if flavor else "")
            lines.append({
                "type": "box", "layout": "horizontal", "spacing": "sm",
                "contents": [
                    {"type": "text", "text": f"{label} ×{qty}", "flex": 1, "wrap": True},
                    {"type": "button", "style": "secondary", "height": "sm",
                     "action": {"type": "postback", "label": "➖", "data": f"PB:DEC:{key}"}},
                    {"type": "button", "style": "secondary", "height": "sm",
                     "action": {"type": "postback", "label": "➕", "data": f"PB:INC:{key}"}},
                ],
            })

        flex = {
            "type": "flex",
            "altText": "修改品項",
            "contents": {"type":"bubble","size":"giga","body":{
                "type":"box","layout":"vertical","spacing":"md","contents":[
                    {"type":"text","text":"修改品項","weight":"bold","size":"xl","wrap":True},
                    {"type":"separator"},
                    *lines,
                    {"type":"separator"},
                    {"type":"button","style":"primary","action":{"type":"postback","label":"🧾 回到結帳","data":"PB:CART"}},
                ]
            }},
        }
        await line_reply(reply_token, [flex])
        return

    if data.startswith("PB:DEC:"):
        key = data.split("PB:DEC:", 1)[1].strip()
        cart = _get_cart(sess)
        if key in cart:
            new_qty = int(cart[key].get("qty", 1) or 1) - 1
            cart_set_qty(sess, key, new_qty)
        await handle_postback(user_id, reply_token, "PB:EDIT_CART")
        return

    if data.startswith("PB:INC:"):
        key = data.split("PB:INC:", 1)[1].strip()
        cart = _get_cart(sess)
        if key in cart:
            new_qty = int(cart[key].get("qty", 1) or 1) + 1
            cart_set_qty(sess, key, new_qty)
        await handle_postback(user_id, reply_token, "PB:EDIT_CART")
        return

    if data == "PB:CHECKOUT":
        ok, msg = validate_min_qty(sess)
        if not ok:
            await line_reply(reply_token, [_text_message(msg), flex_items_list(load_menu_items())])
            return

        cart = cart_list(sess)
        if not cart:
            await line_reply(reply_token, [_text_message("購物車是空的，請先加入品項。"), flex_items_list(load_menu_items())])
            return

        # Choose pickup method
        flex = {
            "type": "flex",
            "altText": "選擇取貨方式",
            "contents": {
                "type": "bubble",
                "size": "giga",
                "body": {"type":"box","layout":"vertical","spacing":"md","contents":[
                    {"type":"text","text":"選擇取貨方式","weight":"bold","size":"xl","wrap":True},
                    {"type":"separator"},
                    {"type":"button","style":"primary","action":{"type":"postback","label":"🏠 店取","data":"PB:PM:店取"}},
                    {"type":"button","style":"primary","action":{"type":"postback","label":"🚚 宅配","data":"PB:PM:宅配"}},
                ]},
            },
        }
        await line_reply(reply_token, [flex])
        return

    if data.startswith("PB:PM:"):
        pm = data.split("PB:PM:", 1)[1].strip()
        sess["pickup_method"] = pm

        dates = list_available_dates()
        if not dates:
            await line_reply(reply_token, [_text_message("近期無可選日期，請稍後再試。")])
            return

        btns = [{"type":"button","style":"secondary","action":{"type":"postback","label":d,"data":f"PB:DATE:{d}"}} for d in dates[:10]]
        flex = {
            "type":"flex","altText":"選擇日期",
            "contents":{"type":"bubble","size":"giga","body":{"type":"box","layout":"vertical","spacing":"md","contents":[
                {"type":"text","text":"選擇日期","weight":"bold","size":"xl","wrap":True},
                {"type":"text","text":f"（可選 {MIN_DAYS}–{MAX_DAYS} 天內）","size":"sm","color":"#666666","wrap":True},
                {"type":"separator"},
                *btns,
            ]}}
        }
        await line_reply(reply_token, [flex])
        return

    if data.startswith("PB:DATE:"):
        d = data.split("PB:DATE:", 1)[1].strip()
        sess["pickup_date"] = d
        if sess.get("pickup_method") == "店取":
            btns = [{"type":"button","style":"secondary","action":{"type":"postback","label":t,"data":f"PB:TIME:{t}"}} for t in PICKUP_TIMES]
            flex = {
                "type":"flex","altText":"選擇時段",
                "contents":{"type":"bubble","size":"giga","body":{"type":"box","layout":"vertical","spacing":"md","contents":[
                    {"type":"text","text":"選擇時段","weight":"bold","size":"xl","wrap":True},
                    {"type":"separator"},
                    *btns,
                ]}}
            }
            await line_reply(reply_token, [flex])
        else:
            sess["pickup_time"] = ""
            sess["awaiting"] = "receiver_name"
            await line_reply(reply_token, [_text_message("請輸入收件人姓名：")])
        return

    if data.startswith("PB:TIME:"):
        t = data.split("PB:TIME:", 1)[1].strip()
        sess["pickup_time"] = t
        sess["awaiting"] = "receiver_name"
        await line_reply(reply_token, [_text_message("請輸入取件人姓名：")])
        return

    # Admin actions
    if data.startswith("PB:ADMIN_"):
        if not is_admin(user_id):
            await line_reply(reply_token, [_text_message("此功能僅限商家使用。")])
            return

        if data.startswith("PB:ADMIN_PAID:"):
            oid = data.split("PB:ADMIN_PAID:", 1)[1].strip()
            update_order_status(oid, "PAID")
            await line_reply(reply_token, [_text_message("已標記為已收款。")])
            return

        if data.startswith("PB:ADMIN_READY:"):
            oid = data.split("PB:ADMIN_READY:", 1)[1].strip()
            update_order_status(oid, "READY")
            await line_reply(reply_token, [_text_message("已更新為 READY。")])
            return

        if data.startswith("PB:ADMIN_SHIPPED:"):
            oid = data.split("PB:ADMIN_SHIPPED:", 1)[1].strip()
            update_order_status(oid, "SHIPPED")
            await line_reply(reply_token, [_text_message("已更新為 SHIPPED。")])
            return

    # fallback
    await line_reply(reply_token, [flex_main_menu()])


# =============================================================================
# Webhook
# =============================================================================
@app.get("/")
def health():
    return {
        "ok": True,
        "app": "uoo-order-bot",
        "min_days": MIN_DAYS,
        "max_days": MAX_DAYS,
        "sheets": {"A": SHEET_A_NAME, "B": SHEET_B_NAME, "C": SHEET_C_NAME, "c_log": SHEET_CLOG_NAME, "items": SHEET_ITEMS_NAME},
        "calendar_enabled": bool(GCAL_CALENDAR_ID),
    }

@app.post("/callback")
async def callback(request: Request, x_line_signature: str = Header(default="")):
    body = await request.body()

    if not _verify_line_signature(body, x_line_signature):
        return PlainTextResponse("invalid signature", status_code=400)

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        return PlainTextResponse("bad request", status_code=400)

    events = payload.get("events", []) or []
    for ev in events:
        try:
            ev_id = ev.get("webhookEventId") or ""
            if not ev_id:
                ev_id = hashlib.sha1(json.dumps(ev, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
            if _dedup_seen(ev_id):
                continue

            etype = ev.get("type")
            src = ev.get("source", {}) or {}
            user_id = src.get("userId", "")
            reply_token = ev.get("replyToken", "")

            if etype == "message":
                msg = ev.get("message", {}) or {}
                if msg.get("type") == "text":
                    text = msg.get("text", "")
                    consumed = await handle_followup_text(user_id, reply_token, text)
                    if consumed:
                        continue
                    await handle_text(user_id, reply_token, text)
                else:
                    await line_reply(reply_token, [flex_main_menu()])

            elif etype == "postback":
                data = (ev.get("postback", {}) or {}).get("data", "")
                await handle_postback(user_id, reply_token, data)

            elif etype == "follow":
                await line_reply(reply_token, [flex_main_menu()])

        except Exception as e:
            print("[Webhook] event error:", repr(e))

    return JSONResponse({"ok": True})
