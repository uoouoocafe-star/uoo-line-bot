# main.py (Ultra-stable / anti-failed version)
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
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# =============================================================================
# Basic App
# =============================================================================
app = FastAPI()

# =============================================================================
# ENV
# =============================================================================
CHANNEL_ACCESS_TOKEN = (os.getenv("CHANNEL_ACCESS_TOKEN") or "").strip()
CHANNEL_SECRET = (os.getenv("CHANNEL_SECRET") or "").strip()

GSHEET_ID = (os.getenv("GSHEET_ID") or "").strip()

# Service account b64 (json)
GOOGLE_SERVICE_ACCOUNT_B64 = (os.getenv("GOOGLE_SERVICE_ACCOUNT_B64") or "").strip()

# Sheets names (best effort)
SHEET_A_NAME = (os.getenv("SHEET_A_NAME") or os.getenv("GSHEET_SHEET_NAME") or "orders").strip()
SHEET_B_NAME = (os.getenv("SHEET_B_NAME") or os.getenv("GSHEET_TAB") or "orders_items").strip()
SHEET_C_NAME = (os.getenv("SHEET_C_NAME") or "C").strip()
SHEET_CLOG_NAME = (os.getenv("SHEET_CLOG_NAME") or os.getenv("SHEET_C_LOG_NAME") or "c_log").strip()
SHEET_ITEMS_NAME = (os.getenv("SHEET_ITEMS_NAME") or "items").strip()

BANK_NAME = (os.getenv("BANK_NAME") or "").strip()
BANK_CORE = (os.getenv("BANK_CORE") or "").strip()
BANK_ACCOUNT = (os.getenv("BANK_ACCOUNT") or "").strip()
STORE_ADDRESS = (os.getenv("STORE_ADDRESS") or "").strip()

def _safe_int_env(key: str, default: int) -> int:
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

CLOSED_DATES_RAW = (os.getenv("CLOSED_DATES") or "").strip()
CLOSED_WEEKDAYS_RAW = (os.getenv("CLOSED_WEEKDAYS") or "").strip()


# =============================================================================
# Utilities
# =============================================================================
def _fmt_currency(n: int) -> str:
    try:
        return f"NT${int(n):,}"
    except Exception:
        return f"NT${n}"

def _parse_csv_dates(s: str) -> set[str]:
    out = set()
    for part in (s or "").split(","):
        p = part.strip()
        if p:
            out.add(p)
    return out

def _parse_csv_ints(s: str) -> set[int]:
    out = set()
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
CLOSED_WEEKDAYS = _parse_csv_ints(CLOSED_WEEKDAYS_RAW)  # Mon=0..Sun=6

def _today_tz() -> dt.date:
    # fixed +8 (Taiwan)
    return (dt.datetime.utcnow() + dt.timedelta(hours=8)).date()

def _date_to_str(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def list_available_dates() -> List[str]:
    today = _today_tz()
    out = []
    for delta in range(MIN_DAYS, MAX_DAYS + 1):
        d = today + dt.timedelta(days=delta)
        if _date_to_str(d) in CLOSED_DATES:
            continue
        if d.weekday() in CLOSED_WEEKDAYS:
            continue
        out.append(_date_to_str(d))
    return out

def new_order_id() -> str:
    ymd = _today_tz().strftime("%Y%m%d")
    tail = str(uuid.uuid4().int)[-4:]
    return f"UOO-{ymd}-{tail}"

# =============================================================================
# LINE Signature
# =============================================================================
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

LINE_REPLY_ENDPOINT = "https://api.line.me/v2/bot/message/reply"

async def line_reply(reply_token: str, messages: List[Dict[str, Any]]) -> None:
    if not reply_token:
        return
    payload = {"replyToken": reply_token, "messages": messages}
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(LINE_REPLY_ENDPOINT, headers=_line_headers(), json=payload)
        if r.status_code >= 400:
            # do NOT crash webhook
            print("[LINE] reply failed:", r.status_code, r.text[:300])

def _text_message(text: str) -> Dict[str, Any]:
    return {"type": "text", "text": text}

# =============================================================================
# Google Clients (ultra-stable)
# =============================================================================
_google_lock = threading.Lock()
_sheets_service = None

SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _load_sa_credentials() -> Credentials:
    if not GOOGLE_SERVICE_ACCOUNT_B64:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_B64")
    raw = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64).decode("utf-8")
    info = json.loads(raw)
    return Credentials.from_service_account_info(info, scopes=SHEETS_SCOPES)

def sheets_service():
    global _sheets_service
    with _google_lock:
        if _sheets_service is None:
            creds = _load_sa_credentials()
            _sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return _sheets_service

def _escape_sheet_name(sheet: str) -> str:
    sheet = (sheet or "").strip()
    if not sheet:
        return sheet
    # Always quote for safety
    return "'" + sheet.replace("'", "''") + "'"

def _a1(sheet: str, rng: str) -> str:
    return f"{_escape_sheet_name(sheet)}!{rng}"

class SheetRepo:
    def __init__(self, spreadsheet_id: str):
        self.sid = spreadsheet_id

    def get_values(self, sheet: str, rng: str) -> List[List[Any]]:
        svc = sheets_service()
        resp = svc.spreadsheets().values().get(
            spreadsheetId=self.sid,
            range=_a1(sheet, rng),
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
        return resp.get("values", [])

    def append_values(self, sheet: str, values: List[List[Any]]) -> None:
        svc = sheets_service()
        # IMPORTANT: use sheet!A:ZZ so it never breaks on A1 parse
        svc.spreadsheets().values().append(
            spreadsheetId=self.sid,
            range=_a1(sheet, "A:ZZ"),
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()

    def header_map(self, sheet: str) -> Dict[str, int]:
        rows = self.get_values(sheet, "A1:ZZ1")
        if not rows:
            return {}
        header = rows[0]
        m = {}
        for i, h in enumerate(header):
            if h is None:
                continue
            k = str(h).strip()
            if k:
                m[k] = i
        return m

    def append_by_header(self, sheet: str, row_dict: Dict[str, Any]) -> None:
        hm = self.header_map(sheet)
        if not hm:
            # fallback: append values only
            self.append_values(sheet, [[str(v) if v is not None else "" for v in row_dict.values()]])
            return
        max_col = max(hm.values())
        row = [""] * (max_col + 1)
        for k, v in row_dict.items():
            if k not in hm:
                continue
            row[hm[k]] = "" if v is None else str(v)
        self.append_values(sheet, [row])

    def find_row_index(self, sheet: str, key_col: str, key_value: str) -> Optional[int]:
        hm = self.header_map(sheet)
        if key_col not in hm:
            return None
        col_idx = hm[key_col] + 1  # 1-based
        col_letter = self._col_to_letter(col_idx)
        vals = self.get_values(sheet, f"{col_letter}2:{col_letter}")
        for i, row in enumerate(vals):
            if row and str(row[0]).strip() == str(key_value).strip():
                return 2 + i
        return None

    def update_cells(self, sheet: str, row_idx: int, updates: Dict[str, Any]) -> None:
        hm = self.header_map(sheet)
        if not hm:
            return
        data = []
        for k, v in updates.items():
            if k not in hm:
                continue
            col_idx = hm[k] + 1
            col_letter = self._col_to_letter(col_idx)
            data.append({"range": _a1(sheet, f"{col_letter}{row_idx}:{col_letter}{row_idx}"),
                         "values": [[("" if v is None else str(v))]]})
        if not data:
            return
        svc = sheets_service()
        svc.spreadsheets().values().batchUpdate(
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
# Dedup (prevents duplicate writes)
# =============================================================================
_DEDUP_LOCK = threading.Lock()
_DEDUP_TTL = 600
_DEDUP: Dict[str, float] = {}

def _dedup_seen(key: str) -> bool:
    now = time.time()
    with _DEDUP_LOCK:
        dead = [k for k, exp in _DEDUP.items() if exp <= now]
        for k in dead:
            _DEDUP.pop(k, None)
        if key in _DEDUP:
            return True
        _DEDUP[key] = now + _DEDUP_TTL
        return False


# =============================================================================
# Items (safe)
# =============================================================================
def load_items() -> List[Dict[str, Any]]:
    if not repo:
        return []
    try:
        rows = repo.get_values(SHEET_ITEMS_NAME, "A1:ZZ")
        if len(rows) < 2:
            return []
        header = [str(x).strip() for x in rows[0]]
        out = []
        for r in rows[1:]:
            d = {}
            for i, h in enumerate(header):
                if not h:
                    continue
                d[h] = r[i] if i < len(r) else ""
            active = str(d.get("active", "1")).strip()
            if active in ("0", "false", "FALSE", "N", "n"):
                continue
            try:
                d["price"] = int(float(d.get("price", 0) or 0))
            except Exception:
                d["price"] = 0
            d["item_id"] = str(d.get("item_id") or d.get("id") or d.get("name") or "").strip()
            out.append(d)
        return out
    except Exception as e:
        print("[Sheets] load_items failed:", repr(e))
        return []


# =============================================================================
# Flex (stable, no debug, NT$ not truncated)
# =============================================================================
def flex_main_menu() -> Dict[str, Any]:
    return {
        "type": "flex",
        "altText": "UooUoo 甜點下單",
        "contents": {
            "type": "bubble",
            "size": "giga",
            "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
                {"type": "text", "text": "UooUoo 甜點下單", "weight": "bold", "size": "xl", "wrap": True},
                {"type": "text", "text": "請選擇：", "size": "md", "color": "#666666", "wrap": True},
                {"type": "button", "style": "primary",
                 "action": {"type": "postback", "label": "🍰 甜點清單", "data": "PB:ITEMS"}},
                {"type": "button", "style": "secondary",
                 "action": {"type": "postback", "label": "🛒 查看購物車", "data": "PB:CART"}},
                {"type": "button", "style": "secondary",
                 "action": {"type": "postback", "label": "💳 付款資訊", "data": "PB:PAY_INFO"}},
            ]}
        }
    }

def flex_items(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    blocks = []
    if not items:
        blocks.append({"type": "text", "text": "目前沒有可下單品項。", "wrap": True})
    else:
        for it in items[:12]:
            name = str(it.get("name", ""))
            price = int(it.get("price", 0) or 0)
            item_id = str(it.get("item_id", ""))
            blocks.append({
                "type": "box", "layout": "horizontal", "spacing": "md",
                "contents": [
                    {"type": "box", "layout": "vertical", "flex": 1, "contents": [
                        {"type": "text", "text": name, "weight": "bold", "wrap": True},
                        {"type": "text", "text": _fmt_currency(price), "size": "sm", "color": "#666666", "wrap": True},
                    ]},
                    {"type": "button", "style": "primary", "height": "sm",
                     "action": {"type": "postback", "label": "加入", "data": f"PB:ADD:{item_id}"}},
                ]
            })
    return {
        "type": "flex",
        "altText": "甜點清單",
        "contents": {"type": "bubble", "size": "giga", "body": {
            "type": "box", "layout": "vertical", "spacing": "md", "contents": [
                {"type": "text", "text": "甜點清單", "weight": "bold", "size": "xl", "wrap": True},
                {"type": "separator"},
                *blocks,
                {"type": "separator"},
                {"type": "button", "style": "secondary",
                 "action": {"type": "postback", "label": "🛒 查看購物車", "data": "PB:CART"}},
            ]
        }}
    }

def flex_pay_info() -> Dict[str, Any]:
    lines = []
    if BANK_NAME or BANK_CORE or BANK_ACCOUNT:
        if BANK_NAME or BANK_CORE:
            lines.append(f"{BANK_NAME} {BANK_CORE}".strip())
        if BANK_ACCOUNT:
            lines.append(f"帳號：{BANK_ACCOUNT}")
    else:
        lines.append("（尚未設定匯款資訊）")
    return {
        "type": "flex",
        "altText": "付款資訊",
        "contents": {"type": "bubble", "size": "giga", "body": {
            "type": "box", "layout": "vertical", "spacing": "md", "contents": [
                {"type": "text", "text": "付款資訊", "weight": "bold", "size": "xl", "wrap": True},
                {"type": "separator"},
                {"type": "text", "text": "\n".join(lines), "wrap": True},
                {"type": "text", "text": "匯款後請回覆：訂單編號＋末五碼", "size": "sm", "color": "#666666", "wrap": True},
            ]
        }}
    }


# =============================================================================
# Session (minimal, stable)
# =============================================================================
_SESS_LOCK = threading.Lock()
_SESS: Dict[str, Dict[str, Any]] = {}
_SESS_TTL = 3600

def _sess(user_id: str) -> Dict[str, Any]:
    now = time.time()
    with _SESS_LOCK:
        dead = [u for u, s in _SESS.items() if s.get("_exp", 0) <= now]
        for u in dead:
            _SESS.pop(u, None)
        s = _SESS.get(user_id)
        if not s:
            s = {"_exp": now + _SESS_TTL, "cart": []}
            _SESS[user_id] = s
        s["_exp"] = now + _SESS_TTL
        return s

def _cart_total(cart: List[Dict[str, Any]]) -> int:
    return sum(int(x.get("unit_price", 0)) * int(x.get("qty", 1)) for x in cart)

def _shipping_fee(method: str) -> int:
    return 180 if method == "宅配" else 0

def flex_cart(sess: Dict[str, Any]) -> Dict[str, Any]:
    cart = sess.get("cart", [])
    lines = []
    subtotal = 0
    for it in cart:
        name = str(it.get("name", ""))
        qty = int(it.get("qty", 1))
        unit = int(it.get("unit_price", 0))
        sub = qty * unit
        subtotal += sub
        lines.append({
            "type": "box", "layout": "horizontal",
            "contents": [
                {"type": "text", "text": f"{name} ×{qty}", "flex": 1, "wrap": True},
                {"type": "text", "text": _fmt_currency(sub), "align": "end", "wrap": True},
            ]
        })
    if not lines:
        lines.append({"type": "text", "text": "（購物車是空的）", "size": "sm", "color": "#666666", "wrap": True})

    pm = sess.get("pickup_method", "")
    pd = sess.get("pickup_date", "")
    pt = sess.get("pickup_time", "")
    ship = int(sess.get("shipping_fee", 0) or 0)
    grand = subtotal + ship

    meta = []
    if pm: meta.append({"type":"text","text":f"取貨方式：{pm}","size":"sm","color":"#666666","wrap":True})
    if pd: meta.append({"type":"text","text":f"日期：{pd}","size":"sm","color":"#666666","wrap":True})
    if pt: meta.append({"type":"text","text":f"時段：{pt}","size":"sm","color":"#666666","wrap":True})

    return {
        "type": "flex",
        "altText": "結帳內容",
        "contents": {"type":"bubble","size":"giga","body":{
            "type":"box","layout":"vertical","spacing":"md","contents":[
                {"type":"text","text":"結帳內容","weight":"bold","size":"xl","wrap":True},
                {"type":"separator"},
                *lines,
                {"type":"separator"},
                *meta,
                {"type":"box","layout":"vertical","spacing":"xs","contents":[
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"小計","flex":1,"wrap":True},
                        {"type":"text","text":_fmt_currency(subtotal),"align":"end","wrap":True},
                    ]},
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"運費","flex":1,"wrap":True},
                        {"type":"text","text":_fmt_currency(ship),"align":"end","wrap":True},
                    ]},
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"合計","flex":1,"weight":"bold","wrap":True},
                        {"type":"text","text":_fmt_currency(grand),"align":"end","weight":"bold","wrap":True},
                    ]},
                ]},
                {"type":"separator"},
                {"type":"button","style":"primary",
                 "action":{"type":"postback","label":"🧾 前往結帳","data":"PB:CHECKOUT"}},
                {"type":"button","style":"secondary",
                 "action":{"type":"postback","label":"➕ 繼續加購","data":"PB:ITEMS"}},
            ]
        }}
    }


# =============================================================================
# Sheets writes (never crash webhook)
# =============================================================================
def safe_append(sheet: str, row: Dict[str, Any]) -> None:
    if not repo:
        return
    try:
        repo.append_by_header(sheet, row)
    except Exception as e:
        # NEVER raise; avoid killing service
        print(f"[Sheets] append failed ({sheet}):", repr(e))

def safe_update_status(order_id: str, status: str) -> None:
    if not repo:
        return
    try:
        idx = repo.find_row_index(SHEET_A_NAME, "order_id", order_id)
        if idx:
            updates = {"updated_at": dt.datetime.utcnow().isoformat() + "Z", "status": status}
            if status == "PAID":
                updates["payment_status"] = "PAID"
            if status in ("READY", "SHIPPED"):
                updates["ship_status"] = status
            repo.update_cells(SHEET_A_NAME, idx, updates)
    except Exception as e:
        print("[Sheets] update status failed:", repr(e))

    # log C & c_log
    ts = dt.datetime.utcnow().isoformat() + "Z"
    safe_append(SHEET_C_NAME, {"created_at": ts, "order_id": order_id, "flow_type": "STATUS", "status": status})
    safe_append(SHEET_CLOG_NAME, {"created_at": ts, "order_id": order_id, "event": f"STATUS_{status}", "payload": ""})


# =============================================================================
# Checkout flow
# =============================================================================
PICKUP_TIMES = ["11:00-12:00", "12:00-14:00", "14:00-16:00"]

async def start_checkout(user_id: str, reply_token: str) -> None:
    s = _sess(user_id)
    if not s.get("cart"):
        await line_reply(reply_token, [_text_message("購物車是空的，請先加入品項。")])
        return
    flex = {
        "type": "flex",
        "altText": "選擇取貨方式",
        "contents": {"type":"bubble","size":"giga","body":{
            "type":"box","layout":"vertical","spacing":"md","contents":[
                {"type":"text","text":"選擇取貨方式","weight":"bold","size":"xl","wrap":True},
                {"type":"separator"},
                {"type":"button","style":"primary","action":{"type":"postback","label":"🏠 店取","data":"PB:PM:店取"}},
                {"type":"button","style":"primary","action":{"type":"postback","label":"🚚 宅配","data":"PB:PM:宅配"}},
            ]
        }}
    }
    await line_reply(reply_token, [flex])

async def pick_date(user_id: str, reply_token: str) -> None:
    dates = list_available_dates()
    if not dates:
        await line_reply(reply_token, [_text_message("近期無可選日期，請稍後再試。")])
        return
    btns = []
    for d in dates[:10]:
        btns.append({"type":"button","style":"secondary","action":{"type":"postback","label":d,"data":f"PB:DATE:{d}"}})
    flex = {
        "type":"flex","altText":"選擇日期",
        "contents":{"type":"bubble","size":"giga","body":{
            "type":"box","layout":"vertical","spacing":"md","contents":[
                {"type":"text","text":"選擇日期","weight":"bold","size":"xl","wrap":True},
                {"type":"text","text":f"（可選 {MIN_DAYS}–{MAX_DAYS} 天內）","size":"sm","color":"#666666","wrap":True},
                {"type":"separator"},
                *btns
            ]
        }}
    }
    await line_reply(reply_token, [flex])

async def pick_time(user_id: str, reply_token: str) -> None:
    btns=[]
    for t in PICKUP_TIMES:
        btns.append({"type":"button","style":"secondary","action":{"type":"postback","label":t,"data":f"PB:TIME:{t}"}})
    flex = {
        "type":"flex","altText":"選擇時段",
        "contents":{"type":"bubble","size":"giga","body":{
            "type":"box","layout":"vertical","spacing":"md","contents":[
                {"type":"text","text":"選擇時段","weight":"bold","size":"xl","wrap":True},
                {"type":"separator"},
                *btns
            ]
        }}
    }
    await line_reply(reply_token, [flex])

async def finalize_order(user_id: str, reply_token: str) -> None:
    s = _sess(user_id)
    cart = s.get("cart", [])
    if not cart:
        await line_reply(reply_token, [_text_message("購物車是空的，無法建立訂單。")])
        return

    pm = s.get("pickup_method","")
    pd = s.get("pickup_date","")
    pt = s.get("pickup_time","")
    receiver = s.get("receiver_name","")
    phone = s.get("phone","")
    address = s.get("address","") if pm == "宅配" else ""

    ship = _shipping_fee(pm)
    s["shipping_fee"] = ship
    subtotal = _cart_total(cart)
    grand = subtotal + ship

    oid = new_order_id()
    ts = dt.datetime.utcnow().isoformat() + "Z"

    items_json = json.dumps(cart, ensure_ascii=False)

    note = ""
    if pm == "宅配":
        note = f"期望到貨:{pd} | 收件人:{receiver} | 電話:{phone} | 地址:{address}"
    else:
        note = f"店取 {pd} {pt} | {receiver} | {phone}"

    # A (summary)
    safe_append(SHEET_A_NAME, {
        "created_at": ts,
        "order_id": oid,
        "user_id": user_id,
        "items_json": items_json,
        "pickup_method": pm,
        "pickup_date": pd,
        "pickup_time": pt,
        "note": note,
        "amount": str(subtotal),
        "shipping_fee": str(ship),
        "grand_total": str(grand),
        "payment_status": "UNPAID",
        "status": "ORDER",
    })

    # B (detail) - one row per item (if your B has header)
    for it in cart:
        safe_append(SHEET_B_NAME, {
            "created_at": ts,
            "order_id": oid,
            "item_id": it.get("item_id",""),
            "item_name": it.get("name",""),
            "qty": str(it.get("qty",1)),
            "unit_price": str(it.get("unit_price",0)),
            "subtotal": str(int(it.get("qty",1))*int(it.get("unit_price",0))),
            "pickup_method": pm,
            "pickup_date": pd,
            "pickup_time": pt,
            "phone": phone,
            "receiver_name": receiver,
            "address": address,
        })

    # C & c_log
    safe_append(SHEET_C_NAME, {
        "created_at": ts, "order_id": oid, "flow_type": "ORDER",
        "method": pm, "amount": str(subtotal), "shipping_fee": str(ship),
        "grand_total": str(grand), "status": "ORDER", "note": note
    })
    safe_append(SHEET_CLOG_NAME, {"created_at": ts, "order_id": oid, "event": "ORDER_CREATED", "payload": items_json})

    # Customer message (NO debug)
    await line_reply(reply_token, [
        _text_message(f"✅ 訂單已建立（待轉帳）\n訂單編號：{oid}\n合計：{_fmt_currency(grand)}\n匯款後請回覆：訂單編號＋末五碼"),
    ])

    # clear cart + checkout fields
    s["cart"] = []
    for k in ("pickup_method","pickup_date","pickup_time","receiver_name","phone","address","awaiting"):
        s.pop(k, None)


# =============================================================================
# Handlers
# =============================================================================
async def handle_text(user_id: str, reply_token: str, text: str) -> None:
    t = (text or "").strip()
    s = _sess(user_id)

    # follow-up fields
    awaiting = s.get("awaiting","")
    if awaiting == "receiver_name":
        s["receiver_name"] = t
        s["awaiting"] = "phone"
        await line_reply(reply_token, [_text_message("請輸入電話：")])
        return
    if awaiting == "phone":
        s["phone"] = t
        if s.get("pickup_method") == "宅配":
            s["awaiting"] = "address"
            await line_reply(reply_token, [_text_message("請輸入宅配地址（完整地址）：")])
        else:
            s["awaiting"] = ""
            await finalize_order(user_id, reply_token)
        return
    if awaiting == "address":
        s["address"] = t
        s["awaiting"] = ""
        await finalize_order(user_id, reply_token)
        return

    # normal commands
    if t in ("開始", "選單", "menu", "start"):
        await line_reply(reply_token, [flex_main_menu()])
        return

    await line_reply(reply_token, [flex_main_menu()])


async def handle_postback(user_id: str, reply_token: str, data: str) -> None:
    data = (data or "").strip()
    s = _sess(user_id)

    if data == "PB:ITEMS":
        items = load_items()
        await line_reply(reply_token, [flex_items(items)])
        return

    if data.startswith("PB:ADD:"):
        item_id = data.split("PB:ADD:", 1)[1].strip()
        items = load_items()
        it = next((x for x in items if str(x.get("item_id","")).strip() == item_id), None)
        if not it:
            await line_reply(reply_token, [_text_message("找不到這個品項，請重新選擇。"), flex_items(items)])
            return
        cart = s.get("cart", [])
        found = None
        for c in cart:
            if c.get("item_id") == item_id:
                found = c
                break
        if found:
            found["qty"] = int(found.get("qty",1)) + 1
        else:
            cart.append({"item_id": item_id, "name": it.get("name",""), "unit_price": int(it.get("price",0) or 0), "qty": 1})
        s["cart"] = cart
        await line_reply(reply_token, [_text_message("已加入購物車。")])
        return

    if data == "PB:CART":
        await line_reply(reply_token, [flex_cart(s)])
        return

    if data == "PB:PAY_INFO":
        await line_reply(reply_token, [flex_pay_info()])
        return

    if data == "PB:CHECKOUT":
        await start_checkout(user_id, reply_token)
        return

    if data.startswith("PB:PM:"):
        s["pickup_method"] = data.split("PB:PM:",1)[1].strip()
        await pick_date(user_id, reply_token)
        return

    if data.startswith("PB:DATE:"):
        s["pickup_date"] = data.split("PB:DATE:",1)[1].strip()
        if s.get("pickup_method") == "店取":
            await pick_time(user_id, reply_token)
        else:
            s["pickup_time"] = ""
            s["awaiting"] = "receiver_name"
            await line_reply(reply_token, [_text_message("請輸入收件人姓名：")])
        return

    if data.startswith("PB:TIME:"):
        s["pickup_time"] = data.split("PB:TIME:",1)[1].strip()
        s["awaiting"] = "receiver_name"
        await line_reply(reply_token, [_text_message("請輸入取件人姓名：")])
        return

    await line_reply(reply_token, [flex_main_menu()])


# =============================================================================
# Webhook
# =============================================================================
@app.get("/")
def health():
    return {"ok": True, "min_days": MIN_DAYS, "max_days": MAX_DAYS, "sheets": bool(GSHEET_ID)}

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
            # de-dup (LINE can resend)
            ev_id = ev.get("webhookEventId") or hashlib.sha1(
                json.dumps(ev, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest()
            if _dedup_seen(ev_id):
                continue

            etype = ev.get("type")
            user_id = (ev.get("source") or {}).get("userId", "")
            reply_token = ev.get("replyToken", "")

            if etype == "message":
                msg = ev.get("message") or {}
                if msg.get("type") == "text":
                    await handle_text(user_id, reply_token, msg.get("text",""))
                else:
                    await line_reply(reply_token, [flex_main_menu()])

            elif etype == "postback":
                pb = ev.get("postback") or {}
                await handle_postback(user_id, reply_token, pb.get("data",""))

            elif etype == "follow":
                await line_reply(reply_token, [flex_main_menu()])

        except Exception as e:
            # NEVER crash webhook
            print("[Webhook] event error:", repr(e))
            print(traceback.format_exc())

    return JSONResponse({"ok": True})
