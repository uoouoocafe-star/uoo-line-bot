# main.py
# UooUoo Cafe LINE Order Bot (FastAPI) - Stable Full Version
# - Google Sheets: A (orders), B (items/details), C (status log), c_log (raw log)
# - Optional Google Calendar event creation
# - Removes all customer-facing debug messages
# - Prevents duplicate webhook deliveries and double-writes
# - Safe parsing for MIN_DAYS/MAX_DAYS even if env value looks like "(3)"

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
import traceback
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Header, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse

# Google APIs
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# =============================================================================
# App
# =============================================================================

app = FastAPI()

# =============================================================================
# Environment / Config
# =============================================================================

CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "").strip()
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "").strip()

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
ADMIN_USER_IDS = [x.strip() for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()]

# Sheets
GSHEET_ID = os.getenv("GSHEET_ID", "").strip()

# A/B: You may have legacy envs. Keep both.
GSHEET_SHEET_NAME = os.getenv("GSHEET_SHEET_NAME", "").strip()  # legacy
GSHEET_TAB = os.getenv("GSHEET_TAB", "").strip()                # legacy

# Preferred: explicit sheet/tab names
SHEET_SETTINGS_NAME = os.getenv("SHEET_SETTINGS_NAME", "settings").strip()
SHEET_ITEMS_NAME = os.getenv("SHEET_ITEMS_NAME", "items").strip()
SHEET_C_NAME = os.getenv("SHEET_C_NAME", "C").strip()
SHEET_CASHFLOW_NAME = os.getenv("SHEET_CASHFLOW_NAME", "cashflow").strip()

# A (orders summary) and B (items detail) — if you already use "orders" as sheet name
SHEET_A_NAME = os.getenv("SHEET_A_NAME", GSHEET_SHEET_NAME or "orders").strip()
SHEET_B_NAME = os.getenv("SHEET_B_NAME", GSHEET_TAB or "orders_items").strip()

# C log sheets
SHEET_CLOG_NAME = os.getenv("SHEET_CLOG_NAME", "c_log").strip()   # raw event/order log
SHEET_C_LOG_NAME = os.getenv("SHEET_C_LOG_NAME", SHEET_CLOG_NAME).strip()  # compatibility

# Business rules
def _safe_int_env(key: str, default: int) -> int:
    """
    Accepts values like:
      "3", " 3 ", "(3)", "3days", "MIN=3"
    Returns the first integer found, otherwise default.
    """
    raw = os.getenv(key, "")
    if raw is None:
        return default
    s = str(raw).strip()
    m = re.search(r"-?\d+", s)
    if not m:
        return default
    try:
        return int(m.group(0))
    except Exception:
        return default

MIN_DAYS = _safe_int_env("MIN_DAYS", 3)
MAX_DAYS = _safe_int_env("MAX_DAYS", 14)
ORDER_CUTOFF_HOURS = _safe_int_env("ORDER_CUTOFF_HOURS", 0)

CLOSED_DATES_RAW = os.getenv("CLOSED_DATES", "").strip()     # e.g. "2026-01-01,2026-01-02"
CLOSED_WEEKDAYS_RAW = os.getenv("CLOSED_WEEKDAYS", "").strip()  # e.g. "0,1" (Mon=0)

STORE_ADDRESS = os.getenv("STORE_ADDRESS", "").strip()

# Payment info
BANK_NAME = os.getenv("BANK_NAME", "").strip()
BANK_CORE = os.getenv("BANK_CORE", "").strip()
BANK_ACCOUNT = os.getenv("BANK_ACCOUNT", "").strip()

# Google Service Account (base64 json)
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "").strip()

# Optional Calendar
GCAL_CALENDAR_ID = os.getenv("GCAL_CALENDAR_ID", "").strip()
GCAL_TIMEZONE = os.getenv("GCAL_TIMEZONE", "Asia/Taipei").strip()

# Misc
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip()  # optional, for debugging
APP_ENV = os.getenv("APP_ENV", "prod").strip().lower()

# =============================================================================
# Constants / Helpers
# =============================================================================

LINE_REPLY_ENDPOINT = "https://api.line.me/v2/bot/message/reply"
LINE_PUSH_ENDPOINT = "https://api.line.me/v2/bot/message/push"

SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]
CAL_SCOPES = [
    "https://www.googleapis.com/auth/calendar",
]

# Webhook de-dup (LINE may resend)
_DEDUP_LOCK = threading.Lock()
_DEDUP_TTL_SEC = 60 * 10
_DEDUP_MAP: Dict[str, float] = {}  # key -> expires_at


def _now_ts() -> float:
    return time.time()


def _dedup_seen(key: str) -> bool:
    """Return True if already processed recently."""
    now = _now_ts()
    with _DEDUP_LOCK:
        # cleanup
        expired = [k for k, exp in _DEDUP_MAP.items() if exp <= now]
        for k in expired:
            _DEDUP_MAP.pop(k, None)

        if key in _DEDUP_MAP:
            return True
        _DEDUP_MAP[key] = now + _DEDUP_TTL_SEC
        return False


def _hmac_sha256(secret: str, body: bytes) -> str:
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")


def _verify_line_signature(body: bytes, signature: str) -> bool:
    if not CHANNEL_SECRET or not signature:
        return False
    expected = _hmac_sha256(CHANNEL_SECRET, body)
    return hmac.compare_digest(expected, signature)


def _line_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


async def line_reply(reply_token: str, messages: List[Dict[str, Any]]) -> None:
    if not reply_token:
        return
    # LINE requires altText for Flex; for text no need. We'll ensure for Flex.
    payload = {"replyToken": reply_token, "messages": messages}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(LINE_REPLY_ENDPOINT, headers=_line_headers(), json=payload)
        # don't raise to avoid webhook failure loops
        if r.status_code >= 400:
            # Keep logs minimal; avoid exposing to user
            print("[LINE] reply failed:", r.status_code, r.text[:500])


async def line_push(to: str, messages: List[Dict[str, Any]]) -> None:
    if not to:
        return
    payload = {"to": to, "messages": messages}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(LINE_PUSH_ENDPOINT, headers=_line_headers(), json=payload)
        if r.status_code >= 400:
            print("[LINE] push failed:", r.status_code, r.text[:500])


def is_admin(user_id: str, token: str = "") -> bool:
    if token and ADMIN_TOKEN and token == ADMIN_TOKEN:
        return True
    return user_id in ADMIN_USER_IDS


def _parse_csv_dates(s: str) -> set[str]:
    out: set[str] = set()
    for part in (s or "").split(","):
        p = part.strip()
        if p:
            out.add(p)
    return out


def _parse_csv_ints(s: str) -> set[int]:
    out: set[int] = set()
    for part in (s or "").split(","):
        p = part.strip()
        if not p:
            continue
        try:
            out.add(int(p))
        except Exception:
            pass
    return out


CLOSED_DATES = _parse_csv_dates(CLOSED_DATES_RAW)
CLOSED_WEEKDAYS = _parse_csv_ints(CLOSED_WEEKDAYS_RAW)  # Monday=0 .. Sunday=6


def _today_tz() -> dt.date:
    # We assume server timezone isn't guaranteed; use Asia/Taipei offset (+8) approximation.
    # If you need full timezone support, use pytz/zoneinfo, but keep dependencies minimal.
    utc = dt.datetime.utcnow()
    local = utc + dt.timedelta(hours=8)
    return local.date()


def _date_to_str(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def _fmt_currency(n: int) -> str:
    # "NT$1,380"
    try:
        return f"NT${int(n):,}"
    except Exception:
        return f"NT${n}"


def _escape_sheet_name(sheet: str) -> str:
    """
    Sheets API range supports quoting:
      'My Sheet'!A:Z
    If name has special chars/spaces, quote it.
    Also escape single quotes by doubling.
    """
    sheet = (sheet or "").strip()
    if sheet == "":
        return sheet
    needs_quote = bool(re.search(r"[ \[\]\(\)\-!@#$%^&*+=,./\\;:]", sheet))
    sheet_escaped = sheet.replace("'", "''")
    return f"'{sheet_escaped}'" if needs_quote else sheet_escaped


def _a1(sheet: str, rng: str) -> str:
    s = _escape_sheet_name(sheet)
    r = (rng or "").strip()
    if "!" in r:
        return r
    return f"{s}!{r}"


# =============================================================================
# Google Clients
# =============================================================================

_google_lock = threading.Lock()
_sheets_service = None
_cal_service = None


def _load_sa_credentials(scopes: List[str]) -> Credentials:
    if not GOOGLE_SERVICE_ACCOUNT_B64:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_B64")
    raw = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64).decode("utf-8")
    info = json.loads(raw)
    return Credentials.from_service_account_info(info, scopes=scopes)


def sheets_service():
    global _sheets_service
    with _google_lock:
        if _sheets_service is None:
            creds = _load_sa_credentials(SHEETS_SCOPES)
            _sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return _sheets_service


def cal_service():
    global _cal_service
    with _google_lock:
        if _cal_service is None:
            creds = _load_sa_credentials(CAL_SCOPES)
            _cal_service = build("calendar", "v3", credentials=creds, cache_discovery=False)
        return _cal_service


# =============================================================================
# Sheets helper (header mapping + append/update)
# =============================================================================

class SheetRepo:
    def __init__(self, spreadsheet_id: str):
        self.sid = spreadsheet_id

    def _get_values(self, range_a1: str) -> List[List[Any]]:
        svc = sheets_service()
        resp = svc.spreadsheets().values().get(
            spreadsheetId=self.sid,
            range=range_a1,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
        return resp.get("values", [])

    def _append_values(self, sheet: str, values: List[List[Any]], start_range: str = "A:Z") -> None:
        svc = sheets_service()
        rng = _a1(sheet, start_range)
        svc.spreadsheets().values().append(
            spreadsheetId=self.sid,
            range=rng,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()

    def _update_values(self, range_a1: str, values: List[List[Any]]) -> None:
        svc = sheets_service()
        svc.spreadsheets().values().update(
            spreadsheetId=self.sid,
            range=range_a1,
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

    def get_header_map(self, sheet: str, header_row: int = 1) -> Dict[str, int]:
        # Read first row
        rng = _a1(sheet, f"A{header_row}:ZZ{header_row}")
        rows = self._get_values(rng)
        if not rows:
            return {}
        header = rows[0]
        m: Dict[str, int] = {}
        for idx, name in enumerate(header):
            if name is None:
                continue
            k = str(name).strip()
            if k:
                m[k] = idx
        return m

    def append_by_header(self, sheet: str, row_dict: Dict[str, Any], header_row: int = 1) -> None:
        hm = self.get_header_map(sheet, header_row=header_row)
        if not hm:
            # If no header, just append raw dict values (stable order not guaranteed)
            self._append_values(sheet, [[str(v) if v is not None else "" for v in row_dict.values()]])
            return

        # Build a row the same length as header
        max_col = max(hm.values()) if hm else -1
        row = [""] * (max_col + 1)
        for k, v in row_dict.items():
            if k not in hm:
                continue
            i = hm[k]
            row[i] = "" if v is None else str(v)
        self._append_values(sheet, [row], start_range="A:ZZ")

    def find_row_index(self, sheet: str, key_col_name: str, key_value: str, header_row: int = 1) -> Optional[int]:
        """
        Find first row index (1-based) where key column equals key_value.
        Reads the column and searches.
        """
        hm = self.get_header_map(sheet, header_row=header_row)
        if key_col_name not in hm:
            return None
        col_idx = hm[key_col_name]  # 0-based
        col_letter = self._col_to_letter(col_idx + 1)
        rng = _a1(sheet, f"{col_letter}{header_row+1}:{col_letter}")
        vals = self._get_values(rng)
        for i, row in enumerate(vals):
            if row and str(row[0]).strip() == str(key_value).strip():
                # header_row+1 is first data row; i is 0-based offset
                return (header_row + 1) + i
        return None

    def update_cells_by_header(
        self,
        sheet: str,
        row_index_1based: int,
        updates: Dict[str, Any],
        header_row: int = 1,
    ) -> None:
        hm = self.get_header_map(sheet, header_row=header_row)
        if not hm:
            return

        # Build batch update ranges for each field
        data = []
        for k, v in updates.items():
            if k not in hm:
                continue
            col_idx = hm[k] + 1  # 1-based col
            col_letter = self._col_to_letter(col_idx)
            rng = _a1(sheet, f"{col_letter}{row_index_1based}:{col_letter}{row_index_1based}")
            data.append({"range": rng, "values": [[("" if v is None else str(v))]]})

        if not data:
            return

        svc = sheets_service()
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=self.sid,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()

    @staticmethod
    def _col_to_letter(n: int) -> str:
        # 1 -> A
        letters = ""
        while n:
            n, rem = divmod(n - 1, 26)
            letters = chr(65 + rem) + letters
        return letters


repo = SheetRepo(GSHEET_ID) if GSHEET_ID else None


# =============================================================================
# Data models
# =============================================================================

@dataclass
class CartItem:
    item_id: str
    name: str
    unit_price: int
    qty: int = 1
    flavor: str = ""
    spec: str = ""

    @property
    def subtotal(self) -> int:
        return int(self.unit_price) * int(self.qty)


@dataclass
class OrderDraft:
    user_id: str
    display_name: str
    order_id: str
    pickup_method: str  # "店取" or "宅配"
    pickup_date: str    # YYYY-MM-DD
    pickup_time: str    # e.g. "12:00-14:00" or "" for delivery
    phone: str
    receiver_name: str
    address: str
    items: List[CartItem]
    shipping_fee: int = 0
    note: str = ""

    @property
    def amount(self) -> int:
        return sum(i.subtotal for i in self.items)

    @property
    def grand_total(self) -> int:
        return int(self.amount) + int(self.shipping_fee)


# =============================================================================
# In-memory session store (simple & stable)
# Note: Render instances can restart; for enterprise-grade, persist sessions to sheet/db.
# =============================================================================

_SESS_LOCK = threading.Lock()
_SESS: Dict[str, Dict[str, Any]] = {}  # user_id -> session dict
_SESS_TTL = 60 * 60  # 1 hour


def _sess_get(user_id: str) -> Dict[str, Any]:
    now = _now_ts()
    with _SESS_LOCK:
        # cleanup
        dead = []
        for uid, s in _SESS.items():
            if s.get("_exp", 0) <= now:
                dead.append(uid)
        for uid in dead:
            _SESS.pop(uid, None)

        s = _SESS.get(user_id)
        if not s:
            s = {"_exp": now + _SESS_TTL}
            _SESS[user_id] = s
        else:
            s["_exp"] = now + _SESS_TTL
        return s


def _sess_clear(user_id: str) -> None:
    with _SESS_LOCK:
        _SESS.pop(user_id, None)


# =============================================================================
# Items / Settings (from Sheets)
# =============================================================================

def load_items() -> List[Dict[str, Any]]:
    """
    Reads SHEET_ITEMS_NAME with header.
    Required headers recommended:
      item_id, name, price, active
    Optional:
      shipping_fee, max_qty, spec, flavor
    """
    if not repo:
        return []
    try:
        values = repo._get_values(_a1(SHEET_ITEMS_NAME, "A1:ZZ"))
        if not values or len(values) < 2:
            return []
        header = [str(x).strip() for x in values[0]]
        out = []
        for row in values[1:]:
            d = {}
            for i, h in enumerate(header):
                if not h:
                    continue
                d[h] = row[i] if i < len(row) else ""
            # active filter
            active = str(d.get("active", "1")).strip()
            if active in ("0", "false", "FALSE", "N", "n"):
                continue
            # normalize
            if "price" in d:
                try:
                    d["price"] = int(float(d["price"]))
                except Exception:
                    d["price"] = 0
            out.append(d)
        return out
    except Exception as e:
        print("[Sheets] load_items failed:", repr(e))
        return []


def calc_shipping_fee(pickup_method: str, amount: int) -> int:
    # Customize as needed; keep stable default
    # Delivery: default 180; Pickup: 0
    if pickup_method == "宅配":
        return 180
    return 0


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
    """
    Returns available dates from MIN_DAYS to MAX_DAYS ahead, excluding closed dates.
    """
    today = _today_tz()
    out = []
    for delta in range(MIN_DAYS, MAX_DAYS + 1):
        d = today + dt.timedelta(days=delta)
        if _is_closed_date(d):
            continue
        out.append(_date_to_str(d))
    return out


PICKUP_TIMES = [
    "11:00-12:00",
    "12:00-14:00",
    "14:00-16:00",
]


# =============================================================================
# Flex Message Builders (No truncation)
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
                    {"type": "text", "text": "請選擇要做什麼：", "size": "md", "color": "#666666", "wrap": True},
                    {
                        "type": "button",
                        "style": "primary",
                        "action": {"type": "postback", "label": "🍰 甜點清單", "data": "PB:ITEMS"},
                    },
                    {
                        "type": "button",
                        "style": "secondary",
                        "action": {"type": "postback", "label": "🛒 查看購物車", "data": "PB:CART"},
                    },
                    {
                        "type": "button",
                        "style": "secondary",
                        "action": {"type": "postback", "label": "📦 取貨說明", "data": "PB:HOW_PICKUP"},
                    },
                    {
                        "type": "button",
                        "style": "secondary",
                        "action": {"type": "postback", "label": "💳 付款資訊", "data": "PB:PAY_INFO"},
                    },
                ],
            },
        },
    }


def flex_items_list(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    contents = []
    if not items:
        contents.append({"type": "text", "text": "目前沒有可下單的品項。", "wrap": True})
    else:
        for it in items[:12]:
            name = str(it.get("name", ""))
            price = int(it.get("price", 0) or 0)
            item_id = str(it.get("item_id", it.get("id", name)))
            line = {
                "type": "box",
                "layout": "horizontal",
                "spacing": "md",
                "contents": [
                    {
                        "type": "box",
                        "layout": "vertical",
                        "flex": 1,
                        "contents": [
                            {"type": "text", "text": name, "weight": "bold", "size": "md", "wrap": True},
                            {"type": "text", "text": _fmt_currency(price), "size": "sm", "color": "#666666", "wrap": True},
                        ],
                    },
                    {
                        "type": "button",
                        "style": "primary",
                        "height": "sm",
                        "action": {"type": "postback", "label": "加入", "data": f"PB:ADD_ITEM:{item_id}"},
                    },
                ],
            }
            contents.append(line)

    return {
        "type": "flex",
        "altText": "甜點清單",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "contents": [
                    {"type": "text", "text": "甜點清單", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "separator"},
                    *contents,
                    {"type": "separator"},
                    {
                        "type": "button",
                        "style": "secondary",
                        "action": {"type": "postback", "label": "🛒 查看購物車", "data": "PB:CART"},
                    },
                ],
            },
        },
    }


def flex_cart(cart: List[CartItem], pickup_method: str = "", pickup_date: str = "", pickup_time: str = "", shipping_fee: int = 0) -> Dict[str, Any]:
    item_lines = []
    total = 0
    for ci in cart:
        total += ci.subtotal
        item_lines.append(
            {
                "type": "box",
                "layout": "horizontal",
                "contents": [
                    {"type": "text", "text": f"{ci.name} ×{ci.qty}", "flex": 1, "wrap": True},
                    {"type": "text", "text": _fmt_currency(ci.subtotal), "align": "end", "wrap": True},
                ],
            }
        )

    if not item_lines:
        item_lines.append({"type": "text", "text": "（購物車是空的）", "size": "sm", "color": "#666666", "wrap": True})

    grand_total = total + int(shipping_fee)

    meta_lines = []
    if pickup_method:
        meta_lines.append({"type": "text", "text": f"取貨方式：{pickup_method}", "size": "sm", "color": "#666666", "wrap": True})
    if pickup_date:
        meta_lines.append({"type": "text", "text": f"日期：{pickup_date}", "size": "sm", "color": "#666666", "wrap": True})
    if pickup_time:
        meta_lines.append({"type": "text", "text": f"時段：{pickup_time}", "size": "sm", "color": "#666666", "wrap": True})

    return {
        "type": "flex",
        "altText": "結帳內容",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "contents": [
                    {"type": "text", "text": "結帳內容", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "separator"},
                    *item_lines,
                    {"type": "separator"},
                    *meta_lines,
                    {
                        "type": "box",
                        "layout": "vertical",
                        "spacing": "xs",
                        "contents": [
                            {
                                "type": "box",
                                "layout": "horizontal",
                                "contents": [
                                    {"type": "text", "text": "小計", "flex": 1, "wrap": True},
                                    {"type": "text", "text": _fmt_currency(total), "align": "end", "wrap": True},
                                ],
                            },
                            {
                                "type": "box",
                                "layout": "horizontal",
                                "contents": [
                                    {"type": "text", "text": "運費", "flex": 1, "wrap": True},
                                    {"type": "text", "text": _fmt_currency(shipping_fee), "align": "end", "wrap": True},
                                ],
                            },
                            {
                                "type": "box",
                                "layout": "horizontal",
                                "contents": [
                                    {"type": "text", "text": "合計", "flex": 1, "weight": "bold", "wrap": True},
                                    {"type": "text", "text": _fmt_currency(grand_total), "align": "end", "weight": "bold", "wrap": True},
                                ],
                            },
                        ],
                    },
                    {"type": "separator"},
                    {
                        "type": "button",
                        "style": "secondary",
                        "action": {"type": "postback", "label": "🔧 修改品項", "data": "PB:EDIT_CART"},
                    },
                    {
                        "type": "button",
                        "style": "primary",
                        "action": {"type": "postback", "label": "🧾 前往結帳", "data": "PB:CHECKOUT"},
                    },
                    {
                        "type": "button",
                        "style": "secondary",
                        "action": {"type": "postback", "label": "➕ 繼續加購", "data": "PB:ITEMS"},
                    },
                ],
            },
        },
    }


def flex_payment_info() -> Dict[str, Any]:
    text_lines = []
    if BANK_NAME or BANK_CORE or BANK_ACCOUNT:
        text_lines.append(f"{BANK_NAME} {BANK_CORE}".strip())
        text_lines.append(f"帳號：{BANK_ACCOUNT}".strip())
    else:
        text_lines.append("（尚未設定匯款資訊）")

    return {
        "type": "flex",
        "altText": "付款資訊",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "contents": [
                    {"type": "text", "text": "付款資訊", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "separator"},
                    {"type": "text", "text": "\n".join(text_lines), "wrap": True},
                    {"type": "text", "text": "匯款後請回覆：訂單編號＋末五碼", "size": "sm", "color": "#666666", "wrap": True},
                ],
            },
        },
    }


def flex_pickup_info() -> Dict[str, Any]:
    txt = []
    txt.append("店取：下單後可選 3–14 天內日期與時段。")
    txt.append("宅配：選擇希望到貨日期（我們會依製作進度安排出貨）。")
    if STORE_ADDRESS:
        txt.append(f"店址：{STORE_ADDRESS}")
    return {
        "type": "flex",
        "altText": "取貨說明",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "contents": [
                    {"type": "text", "text": "取貨說明", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "separator"},
                    {"type": "text", "text": "\n".join(txt), "wrap": True},
                ],
            },
        },
    }


def flex_admin_actions(order_id: str) -> Dict[str, Any]:
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
                    {"type": "text", "text": f"訂單編號：{order_id}", "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "separator"},
                    {
                        "type": "button",
                        "style": "primary",
                        "action": {"type": "postback", "label": "✅ 已收款 (PAID)", "data": f"PB:ADMIN_PAID:{order_id}"},
                    },
                    {
                        "type": "button",
                        "style": "primary",
                        "action": {"type": "postback", "label": "📣 已做好，通知客人取貨 (READY)", "data": f"PB:ADMIN_READY:{order_id}"},
                    },
                    {
                        "type": "button",
                        "style": "primary",
                        "action": {"type": "postback", "label": "🚚 已出貨，通知客人 (SHIPPED)", "data": f"PB:ADMIN_SHIPPED:{order_id}"},
                    },
                    {
                        "type": "button",
                        "style": "secondary",
                        "action": {"type": "postback", "label": "🔔 新訂單通知（測試）", "data": "PB:ADMIN_TEST_NEW_ORDER"},
                    },
                ],
            },
        },
    }


# =============================================================================
# Order ID
# =============================================================================

def new_order_id() -> str:
    # UOO-YYYYMMDD-XXXX
    today = _today_tz()
    ymd = today.strftime("%Y%m%d")
    tail = str(uuid.uuid4().int)[-4:]
    return f"UOO-{ymd}-{tail}"


# =============================================================================
# Write to Sheets (A/B/C/C_LOG)
# =============================================================================

def write_order_to_sheets(order: OrderDraft) -> None:
    """
    - A sheet: summary row with payment_status=UNPAID
    - B sheet: one row per item (detail)
    - C sheet: optional status log row "ORDER"
    - c_log sheet: raw log row
    """
    if not repo:
        return

    created_at = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # Prepare items_json for A (summary)
    items_json = json.dumps(
        [
            {
                "item_id": i.item_id,
                "name": i.name,
                "qty": i.qty,
                "unit_price": i.unit_price,
                "subtotal": i.subtotal,
                "flavor": i.flavor,
                "spec": i.spec,
            }
            for i in order.items
        ],
        ensure_ascii=False,
    )

    # A sheet row (header-driven)
    a_row = {
        "created_at": created_at,
        "user_id": order.user_id,
        "display_name": order.display_name,
        "order_id": order.order_id,
        "items_json": items_json,
        "pickup_method": order.pickup_method,
        "pickup_date": order.pickup_date,
        "pickup_time": order.pickup_time,
        "note": order.note,
        "amount": str(order.amount),
        "shipping_fee": str(order.shipping_fee),
        "grand_total": str(order.grand_total),
        # important:
        "payment_status": "UNPAID",
        "ship_status": "",  # optional
        "status": "ORDER",  # optional
    }
    repo.append_by_header(SHEET_A_NAME, a_row)

    # B sheet detail rows
    for i in order.items:
        b_row = {
            "created_at": created_at,
            "order_id": order.order_id,
            "item_id": i.item_id,
            "item_name": i.name,
            "spec": i.spec,
            "flavor": i.flavor,
            "qty": str(i.qty),
            "unit_price": str(i.unit_price),
            "subtotal": str(i.subtotal),
            "pickup_method": order.pickup_method,
            "pickup_date": order.pickup_date,
            "pickup_time": order.pickup_time,
            "phone": order.phone,
            "receiver_name": order.receiver_name,
            "address": order.address,
        }
        repo.append_by_header(SHEET_B_NAME, b_row)

    # C sheet status log (summary log)
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
    try:
        repo.append_by_header(SHEET_C_NAME, c_row)
    except Exception as e:
        print("[Sheets] append C failed:", repr(e))

    # c_log raw
    clog_row = {
        "created_at": created_at,
        "order_id": order.order_id,
        "event": "ORDER_CREATED",
        "payload": items_json,
    }
    try:
        repo.append_by_header(SHEET_C_LOG_NAME, clog_row)
    except Exception as e:
        print("[Sheets] append c_log failed:", repr(e))


def update_order_status(order_id: str, status: str) -> None:
    """
    Update A sheet + append C + c_log
    status: PAID / READY / SHIPPED
    """
    if not repo:
        return

    created_at = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # Update A sheet row by order_id (header-driven)
    row_idx = repo.find_row_index(SHEET_A_NAME, "order_id", order_id)
    if row_idx:
        updates = {}
        if status == "PAID":
            updates["payment_status"] = "PAID"
        elif status == "READY":
            updates["ship_status"] = "READY"
        elif status == "SHIPPED":
            updates["ship_status"] = "SHIPPED"
        # also keep a generic status column if exists
        updates["status"] = status
        updates["updated_at"] = created_at
        repo.update_cells_by_header(SHEET_A_NAME, row_idx, updates)

    # Append to C sheet
    c_row = {
        "created_at": created_at,
        "order_id": order_id,
        "flow_type": "STATUS",
        "status": status,
        "note": "已收款" if status == "PAID" else ("店取已做好通知" if status == "READY" else "宅配已出貨通知"),
    }
    try:
        repo.append_by_header(SHEET_C_NAME, c_row)
    except Exception as e:
        print("[Sheets] append C STATUS failed:", repr(e))

    # Append to c_log
    clog_row = {
        "created_at": created_at,
        "order_id": order_id,
        "event": f"STATUS_{status}",
        "payload": "",
    }
    try:
        repo.append_by_header(SHEET_C_LOG_NAME, clog_row)
    except Exception as e:
        print("[Sheets] append c_log STATUS failed:", repr(e))


# =============================================================================
# Optional: Google Calendar
# =============================================================================

def create_calendar_event_for_order(order: OrderDraft) -> None:
    if not GCAL_CALENDAR_ID or not GOOGLE_SERVICE_ACCOUNT_B64:
        return

    # Only create if pickup_date exists
    if not order.pickup_date:
        return

    try:
        # Determine start/end
        # Pickup: use pickup_time; Delivery: use 10:00-10:30 placeholder
        tz = GCAL_TIMEZONE or "Asia/Taipei"
        date = order.pickup_date

        if order.pickup_method == "店取" and order.pickup_time:
            # "12:00-14:00" => start 12:00, end 14:00
            m = re.match(r"(\d{2}:\d{2})-(\d{2}:\d{2})", order.pickup_time.strip())
            if m:
                st, et = m.group(1), m.group(2)
            else:
                st, et = "12:00", "12:30"
        else:
            st, et = "10:00", "10:30"

        start_dt = f"{date}T{st}:00"
        end_dt = f"{date}T{et}:00"

        title = f"UooUoo 訂單 {order.order_id} ({order.pickup_method})"
        desc_lines = [
            f"訂單：{order.order_id}",
            f"取貨方式：{order.pickup_method}",
            f"日期：{order.pickup_date}",
            f"時段：{order.pickup_time}" if order.pickup_time else "",
            f"客人：{order.receiver_name} {order.phone}",
            f"地址：{order.address}" if order.address else "",
            f"金額：{_fmt_currency(order.grand_total)}",
        ]
        description = "\n".join([x for x in desc_lines if x])

        location = STORE_ADDRESS if order.pickup_method == "店取" else (order.address or "")

        svc = cal_service()
        event = {
            "summary": title,
            "description": description,
            "location": location,
            "start": {"dateTime": start_dt, "timeZone": tz},
            "end": {"dateTime": end_dt, "timeZone": tz},
        }
        svc.events().insert(calendarId=GCAL_CALENDAR_ID, body=event).execute()

    except Exception as e:
        print("[GCAL] create event failed:", repr(e))


# =============================================================================
# Conversation flows (simple)
# =============================================================================

def _ensure_cart(sess: Dict[str, Any]) -> List[CartItem]:
    if "cart" not in sess:
        sess["cart"] = []
    return sess["cart"]


def _find_item_by_id(items: List[Dict[str, Any]], item_id: str) -> Optional[Dict[str, Any]]:
    for it in items:
        if str(it.get("item_id", it.get("id", ""))).strip() == item_id:
            return it
    return None


def _cart_add(user_id: str, item: Dict[str, Any]) -> None:
    sess = _sess_get(user_id)
    cart = _ensure_cart(sess)
    item_id = str(item.get("item_id", item.get("id", item.get("name", ""))))
    name = str(item.get("name", ""))
    price = int(item.get("price", 0) or 0)

    for ci in cart:
        if ci.item_id == item_id:
            ci.qty += 1
            return
    cart.append(CartItem(item_id=item_id, name=name, unit_price=price, qty=1))


def _cart_set_qty(user_id: str, item_id: str, delta: int) -> None:
    sess = _sess_get(user_id)
    cart = _ensure_cart(sess)
    for ci in list(cart):
        if ci.item_id == item_id:
            ci.qty = max(0, ci.qty + delta)
            if ci.qty <= 0:
                cart.remove(ci)
            return


def _cart_clear(user_id: str) -> None:
    sess = _sess_get(user_id)
    sess["cart"] = []


# =============================================================================
# LINE Event handlers
# =============================================================================

def _text_message(text: str) -> Dict[str, Any]:
    return {"type": "text", "text": text}


def _get_user_profile(user_id: str) -> Tuple[str, str]:
    # display_name, picture_url
    # Optional; keep stable even if fails
    if not user_id:
        return "", ""
    try:
        url = f"https://api.line.me/v2/bot/profile/{user_id}"
        headers = _line_headers()
        r = httpx.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            j = r.json()
            return j.get("displayName", ""), j.get("pictureUrl", "")
    except Exception:
        pass
    return "", ""


async def handle_text(user_id: str, reply_token: str, text: str) -> None:
    t = (text or "").strip()

    # Admin: quick access by typing an order id
    if t.startswith("UOO-") and is_admin(user_id):
        await line_reply(reply_token, [flex_admin_actions(t)])
        return

    if t in ("選單", "menu", "開始", "start"):
        await line_reply(reply_token, [flex_main_menu()])
        return

    # default
    await line_reply(reply_token, [flex_main_menu()])


async def handle_postback(user_id: str, reply_token: str, data: str) -> None:
    data = (data or "").strip()
    sess = _sess_get(user_id)

    if data == "PB:ITEMS":
        items = load_items()
        await line_reply(reply_token, [flex_items_list(items)])
        return

    if data.startswith("PB:ADD_ITEM:"):
        item_id = data.split("PB:ADD_ITEM:", 1)[1].strip()
        items = load_items()
        it = _find_item_by_id(items, item_id)
        if it:
            _cart_add(user_id, it)
            await line_reply(reply_token, [_text_message("已加入購物車。"), flex_main_menu()])
        else:
            await line_reply(reply_token, [_text_message("找不到這個品項，請重新選擇。"), flex_items_list(items)])
        return

    if data == "PB:CART":
        cart = _ensure_cart(sess)
        pickup_method = sess.get("pickup_method", "")
        pickup_date = sess.get("pickup_date", "")
        pickup_time = sess.get("pickup_time", "")
        shipping_fee = int(sess.get("shipping_fee", 0) or 0)
        await line_reply(reply_token, [flex_cart(cart, pickup_method, pickup_date, pickup_time, shipping_fee)])
        return

    if data == "PB:HOW_PICKUP":
        await line_reply(reply_token, [flex_pickup_info()])
        return

    if data == "PB:PAY_INFO":
        await line_reply(reply_token, [flex_payment_info()])
        return

    if data == "PB:EDIT_CART":
        # Provide simple + / - operations by showing items list; real editing can be extended.
        cart = _ensure_cart(sess)
        if not cart:
            await line_reply(reply_token, [_text_message("購物車目前是空的。"), flex_items_list(load_items())])
            return

        # Build a quick-edit flex
        lines = []
        for ci in cart:
            lines.append(
                {
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "sm",
                    "contents": [
                        {"type": "text", "text": f"{ci.name} ×{ci.qty}", "flex": 1, "wrap": True},
                        {"type": "button", "style": "secondary", "height": "sm",
                         "action": {"type": "postback", "label": "➖", "data": f"PB:QTY_DEC:{ci.item_id}"}},
                        {"type": "button", "style": "secondary", "height": "sm",
                         "action": {"type": "postback", "label": "➕", "data": f"PB:QTY_INC:{ci.item_id}"}},
                    ],
                }
            )

        flex = {
            "type": "flex",
            "altText": "修改品項",
            "contents": {
                "type": "bubble",
                "size": "giga",
                "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
                    {"type": "text", "text": "修改品項", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "separator"},
                    *lines,
                    {"type": "separator"},
                    {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🧾 回到結帳", "data": "PB:CART"}},
                ]},
            },
        }
        await line_reply(reply_token, [flex])
        return

    if data.startswith("PB:QTY_DEC:"):
        item_id = data.split("PB:QTY_DEC:", 1)[1].strip()
        _cart_set_qty(user_id, item_id, -1)
        await handle_postback(user_id, reply_token, "PB:EDIT_CART")
        return

    if data.startswith("PB:QTY_INC:"):
        item_id = data.split("PB:QTY_INC:", 1)[1].strip()
        _cart_set_qty(user_id, item_id, +1)
        await handle_postback(user_id, reply_token, "PB:EDIT_CART")
        return

    if data == "PB:CHECKOUT":
        cart = _ensure_cart(sess)
        if not cart:
            await line_reply(reply_token, [_text_message("購物車是空的，請先加入品項。"), flex_items_list(load_items())])
            return

        # Start checkout flow: choose pickup method
        flex = {
            "type": "flex",
            "altText": "選擇取貨方式",
            "contents": {
                "type": "bubble",
                "size": "giga",
                "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
                    {"type": "text", "text": "選擇取貨方式", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "separator"},
                    {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🏠 店取", "data": "PB:PM:店取"}},
                    {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🚚 宅配", "data": "PB:PM:宅配"}},
                ]},
            },
        }
        await line_reply(reply_token, [flex])
        return

    if data.startswith("PB:PM:"):
        pickup_method = data.split("PB:PM:", 1)[1].strip()
        sess["pickup_method"] = pickup_method

        # Pick date
        dates = list_available_dates()
        if not dates:
            await line_reply(reply_token, [_text_message("近期無可選日期，請稍後再試。")])
            return

        # Build date buttons (max 10 shown; can paginate if needed)
        btns = []
        for d in dates[:10]:
            btns.append({"type": "button", "style": "secondary", "action": {"type": "postback", "label": d, "data": f"PB:DATE:{d}"}})
        flex = {
            "type": "flex",
            "altText": "選擇日期",
            "contents": {
                "type": "bubble",
                "size": "giga",
                "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
                    {"type": "text", "text": "選擇日期", "weight": "bold", "size": "xl", "wrap": True},
                    {"type": "text", "text": f"（可選 {MIN_DAYS}–{MAX_DAYS} 天內）", "size": "sm", "color": "#666666", "wrap": True},
                    {"type": "separator"},
                    *btns,
                ]},
            },
        }
        await line_reply(reply_token, [flex])
        return

    if data.startswith("PB:DATE:"):
        d = data.split("PB:DATE:", 1)[1].strip()
        sess["pickup_date"] = d

        pickup_method = sess.get("pickup_method", "")
        if pickup_method == "店取":
            # pick time
            btns = []
            for t in PICKUP_TIMES:
                btns.append({"type": "button", "style": "secondary", "action": {"type": "postback", "label": t, "data": f"PB:TIME:{t}"}})
            flex = {
                "type": "flex",
                "altText": "選擇時段",
                "contents": {
                    "type": "bubble",
                    "size": "giga",
                    "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
                        {"type": "text", "text": "選擇時段", "weight": "bold", "size": "xl", "wrap": True},
                        {"type": "separator"},
                        *btns,
                    ]},
                },
            }
            await line_reply(reply_token, [flex])
        else:
            # delivery: skip time
            sess["pickup_time"] = ""
            await line_reply(reply_token, [_text_message("請輸入收件人姓名：")])
            sess["awaiting"] = "receiver_name"
        return

    if data.startswith("PB:TIME:"):
        t = data.split("PB:TIME:", 1)[1].strip()
        sess["pickup_time"] = t
        await line_reply(reply_token, [_text_message("請輸入取件人姓名：")])
        sess["awaiting"] = "receiver_name"
        return

    # Admin actions
    if data.startswith("PB:ADMIN_"):
        if not is_admin(user_id):
            await line_reply(reply_token, [_text_message("此功能僅限商家使用。")])
            return

        if data == "PB:ADMIN_TEST_NEW_ORDER":
            await line_reply(reply_token, [_text_message("新訂單通知（測試）已觸發。")])
            return

        if data.startswith("PB:ADMIN_PAID:"):
            oid = data.split("PB:ADMIN_PAID:", 1)[1].strip()
            update_order_status(oid, "PAID")
            # 不回 debug，僅回商家確認
            await line_reply(reply_token, [_text_message("已標記為已收款。")])
            return

        if data.startswith("PB:ADMIN_READY:"):
            oid = data.split("PB:ADMIN_READY:", 1)[1].strip()
            update_order_status(oid, "READY")
            await line_reply(reply_token, [_text_message("已通知客人取貨（若你有做推播可再加）。")])
            return

        if data.startswith("PB:ADMIN_SHIPPED:"):
            oid = data.split("PB:ADMIN_SHIPPED:", 1)[1].strip()
            update_order_status(oid, "SHIPPED")
            await line_reply(reply_token, [_text_message("已通知客人出貨（若你有做推播可再加）。")])
            return

    # fallback
    await line_reply(reply_token, [flex_main_menu()])


async def handle_followup_text(user_id: str, reply_token: str, text: str) -> bool:
    """
    Handles checkout follow-up fields:
      receiver_name -> phone -> address(if delivery) / done(if pickup)
    Returns True if consumed.
    """
    sess = _sess_get(user_id)
    awaiting = sess.get("awaiting", "")

    if awaiting == "receiver_name":
        sess["receiver_name"] = (text or "").strip()
        sess["awaiting"] = "phone"
        await line_reply(reply_token, [_text_message("請輸入電話：")])
        return True

    if awaiting == "phone":
        sess["phone"] = (text or "").strip()
        pm = sess.get("pickup_method", "")
        if pm == "宅配":
            sess["awaiting"] = "address"
            await line_reply(reply_token, [_text_message("請輸入宅配地址（完整地址）：")])
        else:
            sess["awaiting"] = ""
            # finalize
            await finalize_order(user_id, reply_token)
        return True

    if awaiting == "address":
        sess["address"] = (text or "").strip()
        sess["awaiting"] = ""
        await finalize_order(user_id, reply_token)
        return True

    return False


async def finalize_order(user_id: str, reply_token: str) -> None:
    sess = _sess_get(user_id)
    cart: List[CartItem] = _ensure_cart(sess)
    if not cart:
        await line_reply(reply_token, [_text_message("購物車是空的，無法建立訂單。")])
        return

    display_name, _ = _get_user_profile(user_id)
    pm = sess.get("pickup_method", "")
    pickup_date = sess.get("pickup_date", "")
    pickup_time = sess.get("pickup_time", "")
    receiver_name = sess.get("receiver_name", "")
    phone = sess.get("phone", "")
    address = sess.get("address", "") if pm == "宅配" else ""
    shipping_fee = calc_shipping_fee(pm, sum(i.subtotal for i in cart))
    sess["shipping_fee"] = shipping_fee

    oid = new_order_id()

    # note format (matches your style)
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
        phone=phone,
        receiver_name=receiver_name,
        address=address,
        items=cart,
        shipping_fee=shipping_fee,
        note=note,
    )

    # Write to sheets (stable)
    try:
        write_order_to_sheets(order)
    except Exception as e:
        print("[Order] write failed:", repr(e))
        await line_reply(reply_token, [_text_message("系統忙碌中，訂單建立失敗，請稍後再試。")])
        return

    # Calendar (optional)
    try:
        create_calendar_event_for_order(order)
    except Exception as e:
        print("[GCAL] finalize create failed:", repr(e))

    # Customer receipt (no debug text)
    receipt_msgs = [
        flex_cart(cart, pm, pickup_date, pickup_time, shipping_fee),
        _text_message(f"✅ 訂單已建立（待轉帳）\n訂單編號：{oid}\n匯款後請回覆：訂單編號＋末五碼"),
    ]
    await line_reply(reply_token, receipt_msgs)

    # clear cart/session fields but keep minimal
    _cart_clear(user_id)
    # keep pickup defaults
    for k in ("awaiting", "receiver_name", "phone", "address"):
        sess.pop(k, None)


# =============================================================================
# Webhook route
# =============================================================================

@app.get("/")
def health():
    return {"ok": True, "app": "uoo-order-bot", "min_days": MIN_DAYS, "max_days": MAX_DAYS}


@app.post("/callback")
async def callback(request: Request, x_line_signature: str = Header(default="")):
    body = await request.body()

    # Signature verification
    if not _verify_line_signature(body, x_line_signature):
        return PlainTextResponse("invalid signature", status_code=400)

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        return PlainTextResponse("bad request", status_code=400)

    events = payload.get("events", []) or []
    for ev in events:
        try:
            # Dedup by webhook event id when available
            ev_id = ev.get("webhookEventId") or ""
            # Fallback: hash the event
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
                mtype = msg.get("type")

                if mtype == "text":
                    text = msg.get("text", "")

                    # If in checkout followup mode, consume
                    consumed = await handle_followup_text(user_id, reply_token, text)
                    if consumed:
                        continue

                    await handle_text(user_id, reply_token, text)

                else:
                    await line_reply(reply_token, [flex_main_menu()])

            elif etype == "postback":
                pb = ev.get("postback", {}) or {}
                data = pb.get("data", "")
                await handle_postback(user_id, reply_token, data)

            elif etype == "follow":
                await line_reply(ev.get("replyToken", ""), [flex_main_menu()])

            else:
                # ignore other events
                pass

        except Exception as e:
            print("[Webhook] event handling error:", repr(e))
            print(traceback.format_exc())

    return JSONResponse({"ok": True})


# =============================================================================
# Notes / Maintainability
# =============================================================================
# 1) 更改品項：請直接改 SHEET_ITEMS_NAME 的表（items），新增/修改 name、price、active。
# 2) 優惠券/折扣：建議新增一張 sheet 叫 coupons，再在 finalize_order() 套用折扣。
# 3) 若你要「按 PAID/READY/SHIPPED 自動推播給客人」：
#    - 需要在 A 表保存 user_id
#    - update_order_status() 內查到該訂單 user_id 後 line_push()
# 4) 若你要「C 表同一筆更新而不是一直 append」：
#    - 可以改成 C 只保留最新狀態欄位，不用 log；log 仍留在 c_log
# =============================================================================
