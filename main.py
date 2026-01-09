import os
import json
import base64
import hmac
import hashlib
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Tuple, Set

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse

from google.oauth2 import service_account
from googleapiclient.discovery import build


# =========================
# Config / Env
# =========================
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "").strip()
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "").strip()

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

# 管理員 userId（逗號分隔）
ADMIN_USER_IDS = [x.strip() for x in (os.getenv("ADMIN_USER_IDS", "") or "").split(",") if x.strip()]

# A表（orders）
SHEET_ORDERS = (
    os.getenv("GSHEET_TAB", "").strip()
    or os.getenv("GSHEET_SHEET_NAME", "").strip()
    or os.getenv("GSHEET_SHEET", "").strip()
    or os.getenv("SHEET_NAME", "orders").strip()
    or "orders"
)

# B表（白話品項）
SHEET_ITEMS = (
    os.getenv("SHEET_ITEMS_NAME", "").strip()
    or os.getenv("SHEET_ITEMS", "").strip()
    or "order_items_readable"
)

# C表（cashflow）
SHEET_CASHFLOW = (
    os.getenv("SHEET_CASHFLOW_NAME", "").strip()
    or os.getenv("SHEET_CASHFLOW", "").strip()
    or "cashflow"
)

# settings 表
SHEET_SETTINGS = (
    os.getenv("SHEET_SETTINGS_NAME", "").strip()
    or os.getenv("SHEET_SETTINGS", "").strip()
    or "settings"
)

STORE_ADDRESS = os.getenv("STORE_ADDRESS", "新竹縣竹北市隘口六街65號").strip()

# 公休/不出貨（env 優先）
ENV_CLOSED_DATES = os.getenv("CLOSED_DATES", "").strip()          # 逗號分隔 yyyy-mm-dd
ENV_CLOSED_WEEKDAYS = os.getenv("CLOSED_WEEKDAYS", "").strip()    # "2" 或 "2,4"（可 0~6 或 1~7）
MIN_DAYS = int((os.getenv("MIN_DAYS", "") or "3").strip() or "3")
MAX_DAYS = int((os.getenv("MAX_DAYS", "") or "14").strip() or "14")

TZ = timezone(timedelta(hours=8))  # Asia/Taipei
LINE_API_BASE = "https://api.line.me/v2/bot/message"


# =========================
# App
# =========================
app = FastAPI()


# =========================
# In-memory session store
# =========================
SESSIONS: Dict[str, Dict[str, Any]] = {}


def get_session(user_id: str) -> Dict[str, Any]:
    if user_id not in SESSIONS:
        SESSIONS[user_id] = {
            "ordering": False,
            "state": "IDLE",

            "cart": [],
            "pending_item": None,
            "pending_flavor": None,

            "pickup_method": None,   # 店取 / 宅配
            "pickup_date": None,
            "pickup_time": None,
            "pickup_name": None,
            "pickup_phone": None,    # ✅ 店取也要電話

            "delivery_date": None,
            "delivery_name": None,
            "delivery_phone": None,
            "delivery_address": None,

            "edit_mode": None,       # INC/DEC/DEL
        }
    return SESSIONS[user_id]


# =========================
# Menu / Data
# =========================
DACQUOISE_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]
TOAST_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

ITEMS = {
    "dacquoise": {"label": "達克瓦茲", "unit_price": 95, "has_flavor": True,  "flavors": DACQUOISE_FLAVORS, "min_qty": 2, "step": 1},
    "scone":     {"label": "原味司康", "unit_price": 65, "has_flavor": False, "flavors": [],               "min_qty": 1, "step": 1},
    "canele6":   {"label": "可麗露 6顆/盒", "unit_price": 490, "has_flavor": False, "flavors": [],        "min_qty": 1, "step": 1},
    "toast":     {"label": "伊思尼奶酥厚片", "unit_price": 85, "has_flavor": True, "flavors": TOAST_FLAVORS,"min_qty": 1, "step": 1},
}

BANK_TRANSFER_TEXT = (
    "付款方式：轉帳（對帳後依訂單號安排出貨/取貨）\n"
    "台灣銀行 004\n"
    "帳號：248-001-03430-6\n\n"
    "轉帳後請回傳：\n"
    "「已轉帳 訂單編號 末五碼12345」"
)

DELIVERY_NOTICE = (
    "宅配：一律冷凍宅配（大榮）\n"
    "運費180元／滿2500免運\n\n"
    "注意事項：\n"
    "• 保持電話暢通（避免退件）\n"
    "• 收到後立刻開箱確認並儘快冷凍/冷藏\n"
    "• 若嚴重損壞請拍照（含原箱）並當日聯繫\n"
    "• 未處理完前請保留原狀勿丟棄/食用\n\n"
    "風險認知：\n"
    "• 易碎品運送中輕微位移/裝飾掉落通常不在理賠範圍\n"
    "• 天災可能導致延遲或停送，無法保證準時"
)

PICKUP_NOTICE = (
    "店取地址：\n"
    f"{STORE_ADDRESS}\n\n"
    "提醒：所有甜點需提前 3 天預訂。"
)


# =========================
# LINE API helpers
# =========================
def line_headers() -> dict:
    return {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def line_reply(reply_token: str, messages: List[dict]):
    if not CHANNEL_ACCESS_TOKEN:
        return
    payload = {"replyToken": reply_token, "messages": messages}
    r = requests.post(
        f"{LINE_API_BASE}/reply",
        headers=line_headers(),
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        timeout=15,
    )
    if r.status_code >= 300:
        print("[ERROR] reply failed:", r.status_code, r.text)


def line_push(to_user_id: str, messages: List[dict]):
    if not CHANNEL_ACCESS_TOKEN or not to_user_id:
        return
    payload = {"to": to_user_id, "messages": messages}
    r = requests.post(
        f"{LINE_API_BASE}/push",
        headers=line_headers(),
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        timeout=15,
    )
    if r.status_code >= 300:
        print("[ERROR] push failed:", r.status_code, r.text)


def msg_text(text: str, quick_items: Optional[List[dict]] = None) -> dict:
    m = {"type": "text", "text": text}
    if quick_items:
        m["quickReply"] = {"items": quick_items}
    return m


def quick_postback(label: str, data: str, display_text: Optional[str] = None) -> dict:
    action = {"type": "postback", "label": label, "data": data}
    if display_text:
        action["displayText"] = display_text
    return {"type": "action", "action": action}


def msg_flex(alt_text: str, contents: dict) -> dict:
    return {"type": "flex", "altText": alt_text, "contents": contents}


def is_admin(user_id: str) -> bool:
    return bool(ADMIN_USER_IDS) and (user_id in ADMIN_USER_IDS)


# =========================
# Google Sheets
# =========================
def load_service_account_info() -> Optional[dict]:
    if GOOGLE_SERVICE_ACCOUNT_B64:
        try:
            raw = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64.encode("utf-8")).decode("utf-8")
            return json.loads(raw)
        except Exception as e:
            print("[ERROR] decode GOOGLE_SERVICE_ACCOUNT_B64 failed:", e)
            return None
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        try:
            return json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        except Exception as e:
            print("[ERROR] parse GOOGLE_SERVICE_ACCOUNT_JSON failed:", e)
            return None
    return None


def get_sheets_service():
    info = load_service_account_info()
    if not info:
        return None
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def safe_a1(tab: str) -> str:
    # ✅ 一律用 'TAB'!A1 避免 parse range 失敗
    return f"'{tab}'!A1"


def append_row(tab: str, row: List[Any]) -> bool:
    if not GSHEET_ID:
        print("[WARN] GSHEET_ID missing, skip append.")
        return False
    service = get_sheets_service()
    if not service:
        print("[WARN] Google Sheet creds missing, skip append.")
        return False
    try:
        service.spreadsheets().values().append(
            spreadsheetId=GSHEET_ID,
            range=safe_a1(tab),
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
        return True
    except Exception as e:
        print(f"[ERROR] append_row failed tab={tab}:", e)
        return False


def read_range(tab: str, a1_range: str) -> List[List[str]]:
    service = get_sheets_service()
    if not service or not GSHEET_ID:
        return []
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"'{tab}'!{a1_range}",
        ).execute()
        return resp.get("values", []) or []
    except Exception as e:
        print("[WARN] read_range failed:", e)
        return []


def update_cell(tab: str, a1: str, value: Any) -> bool:
    service = get_sheets_service()
    if not service or not GSHEET_ID:
        return False
    try:
        service.spreadsheets().values().update(
            spreadsheetId=GSHEET_ID,
            range=f"'{tab}'!{a1}",
            valueInputOption="RAW",
            body={"values": [[value]]},
        ).execute()
        return True
    except Exception as e:
        print("[WARN] update_cell failed:", e)
        return False


def read_settings_sheet() -> Dict[str, str]:
    """
    settings tab 格式：
    A:key  B:value
    closed_weekday / closed_dates / min_days / max_days
    """
    values = read_range(SHEET_SETTINGS, "A1:B200")
    out = {}
    for row in values:
        if len(row) >= 2:
            k = str(row[0]).strip()
            v = str(row[1]).strip()
            if k:
                out[k] = v
    return out


# =========================
# Helpers (rules / cart)
# =========================
def now_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


def gen_order_id() -> str:
    d = datetime.now(TZ).strftime("%Y%m%d")
    suffix = "".join(random.choices(string.digits, k=4))
    return f"UOO-{d}-{suffix}"


def cart_total(cart: List[dict]) -> int:
    return sum(int(x.get("subtotal", 0)) for x in cart)


def shipping_fee(total: int) -> int:
    return 0 if total >= 2500 else 180


def parse_closed_dates(v: str) -> Set[str]:
    out = set()
    if not v:
        return out
    parts = [x.strip() for x in v.split(",") if x.strip()]
    for p in parts:
        if len(p) >= 8:
            out.add(p)
    return out


def parse_weekdays_list(v: str) -> Set[int]:
    """
    支援：
    - "2" 或 "2,4"
    - 0~6 (Mon=0..Sun=6) 或 1~7 (Mon=1..Sun=7)
    回傳 python weekday(0~6)
    """
    out = set()
    if not v:
        return out
    parts = [x.strip() for x in v.split(",") if x.strip()]
    for p in parts:
        try:
            n = int(p)
        except:
            continue
        if 0 <= n <= 6:
            out.add(n)
        elif 1 <= n <= 7:
            out.add(n - 1)
    return out


def get_rules() -> Tuple[int, int, Set[int], Set[str]]:
    min_days = MIN_DAYS
    max_days = MAX_DAYS

    closed_weekdays = parse_weekdays_list(ENV_CLOSED_WEEKDAYS)
    closed_dates = parse_closed_dates(ENV_CLOSED_DATES)

    if (not closed_weekdays) and (not closed_dates):
        s = read_settings_sheet()
        cw = parse_weekdays_list(s.get("closed_weekday", ""))
        cd = parse_closed_dates(s.get("closed_dates", ""))
        if cw:
            closed_weekdays = cw
        if cd:
            closed_dates = cd
        try:
            min_days = int(s.get("min_days", str(min_days)) or str(min_days))
            max_days = int(s.get("max_days", str(max_days)) or str(max_days))
        except:
            pass

    return min_days, max_days, closed_weekdays, closed_dates


def fmt_md_date(dt: datetime) -> str:
    wk = "一二三四五六日"[dt.weekday()]
    return f"{dt.month}/{dt.day}（{wk}）"


def build_date_buttons() -> List[Tuple[str, str]]:
    min_days, max_days, closed_weekdays, closed_dates = get_rules()
    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    out = []
    for i in range(min_days, max_days + 1):
        d = today + timedelta(days=i)
        ymd = d.strftime("%Y-%m-%d")
        if closed_weekdays and d.weekday() in closed_weekdays:
            continue
        if ymd in closed_dates:
            continue
        out.append((fmt_md_date(d), ymd))
    return out


def recalc_cart(sess: dict):
    for x in sess["cart"]:
        x["subtotal"] = int(x["unit_price"]) * int(x["qty"])


def can_dec_item(item_key: str, new_qty: int) -> bool:
    return new_qty >= ITEMS[item_key]["min_qty"]


def add_to_cart(user_id: str, item_key: str, flavor: Optional[str], qty: int):
    sess = get_session(user_id)
    meta = ITEMS[item_key]
    if meta["has_flavor"] and not flavor:
        raise ValueError("缺少口味")
    if qty < meta["min_qty"]:
        raise ValueError(f"數量至少 {meta['min_qty']}")
    unit = meta["unit_price"]
    sess["cart"].append({
        "item_key": item_key,
        "label": meta["label"],
        "flavor": flavor or "",
        "qty": qty,
        "unit_price": unit,
        "subtotal": unit * qty,
    })


def find_cart_line_label(x: dict) -> str:
    name = x["label"]
    if x.get("flavor"):
        name += f"（{x['flavor']}）"
    return f"{name} ×{x['qty']}（{x['unit_price']}/單位）＝{x['subtotal']}"


def human_item_summary(cart: List[dict]) -> str:
    parts = []
    for x in cart:
        label = x["label"]
        qty = int(x["qty"])
        if x["item_key"] == "canele6":
            qty_text = f"{qty}盒"
        else:
            qty_text = f"{qty}顆"
        if x.get("flavor"):
            parts.append(f"{label}｜{qty_text}｜{x['flavor']}")
        else:
            parts.append(f"{label}｜{qty_text}")
    return "；".join(parts)


# =========================
# Flex builders
# =========================
def flex_home_hint() -> dict:
    return {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "UooUoo 甜點訂購", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "請先點「我要下單」才會進入下單流程。\n想先看品項可點「甜點」。", "wrap": True, "size": "sm", "color": "#666666"},
            ],
        },
    }


def flex_product_menu(ordering: bool) -> dict:
    def btn(label: str, data: str, enabled: bool = True) -> dict:
        return {
            "type": "button",
            "style": "primary" if enabled else "secondary",
            "action": {"type": "postback", "label": label, "data": data, "displayText": label},
            "height": "sm",
        }

    disable = not ordering
    return {
        "type": "bubble",
        "size": "mega",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "請選擇商品", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "（全部甜點需提前 3 天預訂）", "size": "sm", "color": "#666666"},
                btn("達克瓦茲｜NT$95", "PB:ITEM:dacquoise", enabled=not disable),
                btn("原味司康｜NT$65", "PB:ITEM:scone", enabled=not disable),
                btn("可麗露 6顆/盒｜NT$490", "PB:ITEM:canele6", enabled=not disable),
                btn("伊思尼奶酥厚片｜NT$85", "PB:ITEM:toast", enabled=not disable),
                {"type": "separator", "margin": "lg"},
                {"type": "button", "style": "secondary",
                 "action": {"type": "postback", "label": "🧾 前往結帳", "data": "PB:CHECKOUT", "displayText": "前往結帳"}},
                {"type": "button", "style": "secondary",
                 "action": {"type": "postback", "label": "🗑 清空重來", "data": "PB:RESET", "displayText": "清空重來"}},
            ],
        },
    }


def flex_pickup_method() -> dict:
    return {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "請選擇店取或宅配", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "（日期可直接按按鈕，不用手打）", "size": "sm", "color": "#666666"},
                {"type": "button", "style": "primary",
                 "action": {"type": "postback", "label": "🏪 店取", "data": "PB:PICKUP:店取", "displayText": "店取"}},
                {"type": "button", "style": "primary",
                 "action": {"type": "postback", "label": "🚚 冷凍宅配", "data": "PB:PICKUP:宅配", "displayText": "冷凍宅配"}},
            ],
        },
    }


def flex_checkout_summary(sess: dict) -> dict:
    cart = sess["cart"]
    lines = [find_cart_line_label(x) for x in cart]
    total = cart_total(cart)
    fee = shipping_fee(total) if sess.get("pickup_method") == "宅配" else 0
    grand = total + fee

    method = sess.get("pickup_method") or "（未選）"
    date = sess.get("pickup_date") if method == "店取" else sess.get("delivery_date")
    date = date or "（未選）"
    time = sess.get("pickup_time") or ("—" if method != "店取" else "（未選）")

    shown = lines[:10]
    if len(lines) > 10:
        shown.append(f"…等 {len(lines)} 項（請先刪減購物車）")
    list_text = "\n".join([f"• {s}" for s in shown]) if shown else "（購物車是空的）"

    bottom_text = f"小計：NT${total}"
    if method == "宅配":
        bottom_text += f"\n運費：NT${fee}\n應付：NT${grand}"

    return {
        "type": "bubble",
        "size": "mega",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "🧾 結帳內容", "weight": "bold", "size": "xl"},
                {"type": "text", "text": list_text, "wrap": True, "size": "sm"},
                {"type": "separator", "margin": "md"},
                {"type": "text", "text": f"取貨方式：{method}", "size": "sm", "color": "#666666"},
                {"type": "text", "text": f"日期：{date}", "size": "sm", "color": "#666666"},
                {"type": "text", "text": f"時段：{time}", "size": "sm", "color": "#666666"},
                {"type": "separator", "margin": "md"},
                {"type": "text", "text": bottom_text, "weight": "bold", "size": "lg"},
            ],
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "button", "style": "primary",
                 "action": {"type": "postback", "label": "🛠 修改品項", "data": "PB:EDIT:MENU", "displayText": "修改品項"}},
                {"type": "button", "style": "secondary",
                 "action": {"type": "postback", "label": "➕ 繼續加購", "data": "PB:CONTINUE", "displayText": "繼續加購"}},
                {"type": "button", "style": "secondary",
                 "action": {"type": "postback", "label": "✅ 下一步", "data": "PB:NEXT", "displayText": "下一步"}},
            ],
        },
    }


def flex_admin_notify_card(order_id: str, method: str, date: str, time: str, note: str, item_summary: str, amount: int, fee: int, grand: int) -> dict:
    if method == "店取":
        title = "🔔 新店取訂單"
        action_label = "✅ 已做好 → 通知客人可取貨"
        action_data = f"PB:ADMIN:READY:{order_id}"
        price_line = f"小計 NT${amount}"
    else:
        title = "🔔 新宅配訂單"
        action_label = "🚚 已出貨 → 通知客人"
        action_data = f"PB:ADMIN:SHIPPED:{order_id}"
        price_line = f"小計 NT${amount}｜運費 NT${fee}｜應付 NT${grand}"

    date_line = f"日期：{date}" + (f"｜時段：{time}" if method == "店取" else "")
    return {
        "type": "bubble",
        "size": "mega",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "text", "text": title, "weight": "bold", "size": "xl"},
                {"type": "text", "text": f"訂單：{order_id}", "size": "sm", "color": "#555555"},
                {"type": "text", "text": f"方式：{method}", "size": "sm", "color": "#555555"},
                {"type": "text", "text": date_line, "size": "sm", "color": "#555555"},
                {"type": "text", "text": f"內容：{item_summary}", "wrap": True, "size": "sm"},
                {"type": "text", "text": price_line, "size": "sm", "weight": "bold"},
                {"type": "text", "text": f"備註：{note}", "wrap": True, "size": "xs", "color": "#777777"},
                {"type": "separator", "margin": "md"},
                {"type": "button", "style": "primary",
                 "action": {"type": "postback", "label": action_label, "data": action_data, "displayText": action_label}},
            ],
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "button", "style": "secondary",
                 "action": {"type": "postback", "label": "📌 複製訂單號", "data": f"PB:ADMIN:COPY:{order_id}", "displayText": order_id}},
            ],
        }
    }


# =========================
# Order read/update (for admin buttons)
# =========================
def find_order_in_orders_sheet(order_id: str) -> Optional[dict]:
    """
    orders 欄位（本程式寫入）：
    A created_at
    B user_id
    C display_name
    D order_id
    E items_json
    F pickup_method
    G pickup_date
    H pickup_time
    I note
    J amount
    K pay_status
    L transaction_note (白話總結)
    """
    rows = read_range(SHEET_ORDERS, "A1:L2000")
    if not rows or len(rows) < 2:
        return None

    header = rows[0]
    for i in range(1, len(rows)):
        r = rows[i]
        if len(r) >= 4 and str(r[3]).strip() == order_id:
            # 位置 i+1 = sheet row index
            return {
                "sheet_row": i + 1,
                "created_at": r[0] if len(r) > 0 else "",
                "user_id": r[1] if len(r) > 1 else "",
                "order_id": r[3] if len(r) > 3 else order_id,
                "items_json": r[4] if len(r) > 4 else "",
                "pickup_method": r[5] if len(r) > 5 else "",
                "pickup_date": r[6] if len(r) > 6 else "",
                "pickup_time": r[7] if len(r) > 7 else "",
                "note": r[8] if len(r) > 8 else "",
                "amount": int(r[9]) if len(r) > 9 and str(r[9]).isdigit() else 0,
                "pay_status": r[10] if len(r) > 10 else "",
                "transaction_note": r[11] if len(r) > 11 else "",
            }
    return None


def update_cashflow_status(order_id: str, new_status: str) -> bool:
    """
    cashflow 欄位：
    A created_at
    B order_id
    C flow_type
    D method
    E amount
    F shipping_fee
    G grand_total
    H status  <-- 更新這格
    I note
    """
    rows = read_range(SHEET_CASHFLOW, "A1:I5000")
    if not rows or len(rows) < 2:
        return False

    for i in range(1, len(rows)):
        r = rows[i]
        if len(r) >= 2 and str(r[1]).strip() == order_id:
            sheet_row = i + 1
            # H 欄 = status
            return update_cell(SHEET_CASHFLOW, f"H{sheet_row}", new_status)

    return False


# =========================
# Order write (A/B/C)
# =========================
def write_order_to_sheets(user_id: str, order_id: str, cart: List[dict], pickup_method: str,
                          pickup_date: str, pickup_time: str, note: str,
                          amount: int, fee: int, grand: int) -> None:
    # A 表
    a_row = [
        now_str(),
        user_id,
        "",  # display_name 目前不抓 profile
        order_id,
        json.dumps({"cart": cart}, ensure_ascii=False),
        pickup_method,
        pickup_date,
        pickup_time,
        note,
        amount,
        "UNPAID",
        human_item_summary(cart),  # transaction_note（白話總結）
    ]
    append_row(SHEET_ORDERS, a_row)

    # B 表（逐品項）
    for x in cart:
        item_name = x["label"]
        qty = int(x["qty"])
        unit = int(x["unit_price"])
        sub = int(x["subtotal"])
        if x.get("flavor"):
            item_name = f"{item_name}｜{x['flavor']}"
        b_row = [
            now_str(),
            order_id,
            item_name,
            qty,
            unit,
            sub,
            pickup_method,
            pickup_date,
            pickup_time,
            "UNPAID",
        ]
        append_row(SHEET_ITEMS, b_row)

    # C 表（cashflow）
    c_row = [
        now_str(),
        order_id,
        "ORDER",                 # flow_type
        pickup_method,           # method
        amount,                  # amount
        fee,                     # shipping_fee
        grand,                   # grand_total
        "UNPAID",                # status
        note,                    # note
    ]
    append_row(SHEET_CASHFLOW, c_row)


def push_admin_new_order_card(order_id: str):
    """
    建單後推播給管理員：一張可按鈕通知客人的卡片
    """
    if not ADMIN_USER_IDS:
        print("[WARN] ADMIN_USER_IDS not set, skip admin push.")
        return

    info = find_order_in_orders_sheet(order_id)
    if not info:
        print("[WARN] cannot find order in sheet for admin card:", order_id)
        return

    method = info.get("pickup_method", "")
    date = info.get("pickup_date", "")
    time = info.get("pickup_time", "")
    note = info.get("note", "")
    item_summary = info.get("transaction_note", "")
    amount = int(info.get("amount", 0) or 0)

    fee = 0
    grand = amount
    if method == "宅配":
        fee = shipping_fee(amount)
        grand = amount + fee

    card = flex_admin_notify_card(order_id, method, date, time, note, item_summary, amount, fee, grand)
    for admin_id in ADMIN_USER_IDS:
        line_push(admin_id, [msg_flex("新訂單通知", card)])


def create_order(user_id: str, sess: dict) -> str:
    cart = sess["cart"]
    if not cart:
        return ""

    order_id = gen_order_id()
    amount = cart_total(cart)
    method = sess.get("pickup_method") or ""

    if method == "宅配":
        delivery_date = sess.get("delivery_date") or ""
        dn = sess.get("delivery_name") or ""
        dp = sess.get("delivery_phone") or ""
        da = sess.get("delivery_address") or ""
        note = f"希望到貨:{delivery_date} | 收件人:{dn} | 電話:{dp} | 地址:{da}"
        fee = shipping_fee(amount)
        grand = amount + fee
        pickup_date = delivery_date
        pickup_time = ""
    else:
        pickup_date = sess.get("pickup_date") or ""
        pickup_time = sess.get("pickup_time") or ""
        pn = sess.get("pickup_name") or ""
        pp = sess.get("pickup_phone") or ""
        note = f"取件人:{pn} | 電話:{pp}"
        fee = 0
        grand = amount

    write_order_to_sheets(
        user_id=user_id,
        order_id=order_id,
        cart=cart,
        pickup_method=method,
        pickup_date=pickup_date,
        pickup_time=pickup_time,
        note=note,
        amount=amount,
        fee=fee,
        grand=grand,
    )

    # ✅ 建單後推播管理員卡片（可按按鈕通知客人）
    push_admin_new_order_card(order_id)

    return order_id


# =========================
# Signature verify
# =========================
def verify_line_signature(body: bytes, signature: str) -> bool:
    if not CHANNEL_SECRET:
        return False
    mac = hmac.new(CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature)


# =========================
# Routes
# =========================
@app.get("/")
def root():
    return {
        "ok": True,
        "service": "uoo-line-bot",
        "tabs": {"orders": SHEET_ORDERS, "items": SHEET_ITEMS, "cashflow": SHEET_CASHFLOW, "settings": SHEET_SETTINGS},
        "admin_user_ids_set": bool(ADMIN_USER_IDS),
    }


@app.post("/callback")
async def callback(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")
    if not verify_line_signature(body, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(body.decode("utf-8"))
    events = payload.get("events", [])
    for ev in events:
        try:
            handle_event(ev)
        except Exception as e:
            print("[ERROR] handle_event:", e)

    return PlainTextResponse("OK")


# =========================
# Event handler
# =========================
def handle_event(ev: dict):
    etype = ev.get("type")
    user_id = (ev.get("source") or {}).get("userId", "")
    reply_token = ev.get("replyToken", "")

    if not user_id:
        return

    sess = get_session(user_id)

    # ---- message text ----
    if etype == "message" and (ev.get("message") or {}).get("type") == "text":
        text = (ev["message"].get("text") or "").strip()

        # 取管理員ID
        if text == "我的管理員ID":
            line_reply(reply_token, [msg_text(f"你的 userId：\n{user_id}\n\n把這串填到 Render 環境變數 ADMIN_USER_IDS 即可。")])
            return

        if text in ["清空重來", "清空", "reset"]:
            reset_session(sess)
            line_reply(reply_token, [msg_text("已清空，重新開始。\n請點「我要下單」開始，或點「甜點」先看菜單。")])
            return

        if text == "甜點":
            line_reply(reply_token, [msg_flex("甜點菜單", flex_product_menu(ordering=sess["ordering"]))])
            return

        if text == "我要下單":
            sess["ordering"] = True
            sess["state"] = "IDLE"
            line_reply(reply_token, [
                msg_text("好的，開始下單。\n請從甜點菜單選擇商品。"),
                msg_flex("甜點菜單", flex_product_menu(ordering=True)),
            ])
            return

        if text == "取貨說明":
            line_reply(reply_token, [msg_text(PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE)])
            return

        if text == "付款資訊" or text == "付款說明":
            line_reply(reply_token, [msg_text(BANK_TRANSFER_TEXT)])
            return

        if text.startswith("已轉帳"):
            line_reply(reply_token, [msg_text("收到，我們會核對帳款後依訂單號安排出貨/取貨。\n若需補充資訊也可以直接留言。")])
            return

        handle_state_text(user_id, reply_token, text)
        return

    # ---- postback ----
    if etype == "postback":
        data = (ev.get("postback") or {}).get("data", "")
        handle_postback(user_id, reply_token, data)
        return


def reset_session(sess: dict):
    sess["ordering"] = False
    sess["state"] = "IDLE"
    sess["cart"] = []
    sess["pending_item"] = None
    sess["pending_flavor"] = None
    sess["pickup_method"] = None
    sess["pickup_date"] = None
    sess["pickup_time"] = None
    sess["pickup_name"] = None
    sess["pickup_phone"] = None
    sess["delivery_date"] = None
    sess["delivery_name"] = None
    sess["delivery_phone"] = None
    sess["delivery_address"] = None
    sess["edit_mode"] = None


# =========================
# Postback flows
# =========================
def build_qty_quick(min_qty: int, max_qty: int, prefix: str) -> List[dict]:
    return [quick_postback(str(i), f"{prefix}{i}", display_text=str(i)) for i in range(min_qty, max_qty + 1)]


def build_cart_item_choices(sess: dict, mode: str) -> List[dict]:
    items = []
    for idx, x in enumerate(sess["cart"]):
        label = x["label"]
        if x.get("flavor"):
            label += f"（{x['flavor']}）"
        label += f" ×{x['qty']}"
        items.append(quick_postback(label, f"PB:EDIT:{mode}:{idx}", display_text=label))
    return items


def handle_postback(user_id: str, reply_token: str, data: str):
    sess = get_session(user_id)

    # =========================
    # ✅ 管理員通知按鈕
    # =========================
    if data.startswith("PB:ADMIN:"):
        if not is_admin(user_id):
            line_reply(reply_token, [msg_text("此功能僅限管理員使用。")])
            return

        # PB:ADMIN:READY:ORDERID / PB:ADMIN:SHIPPED:ORDERID
        parts = data.split(":")
        if len(parts) < 4:
            line_reply(reply_token, [msg_text("管理員指令格式錯誤。")])
            return

        action = parts[2].strip()
        order_id = parts[3].strip()

        info = find_order_in_orders_sheet(order_id)
        if not info:
            line_reply(reply_token, [msg_text(f"找不到訂單：{order_id}\n請確認 orders 表內有此訂單號。")])
            return

        customer_id = info.get("user_id", "").strip()
        method = info.get("pickup_method", "")
        date = info.get("pickup_date", "")
        time = info.get("pickup_time", "")
        note = info.get("note", "")
        item_summary = info.get("transaction_note", "")

        if not customer_id:
            line_reply(reply_token, [msg_text("此訂單缺少客人 user_id，無法推播通知。")])
            return

        if action == "READY":
            # 店取：做好通知
            text = (
                "✅ 你的甜點已準備完成，可前來取貨\n"
                f"訂單編號：{order_id}\n"
                f"取貨方式：店取\n"
                f"日期：{date}\n"
                f"時段：{time}\n"
                f"內容：{item_summary}\n\n"
                f"地址：{STORE_ADDRESS}\n"
                "到店後報「訂單編號」即可。"
            )
            line_push(customer_id, [msg_text(text)])
            update_cashflow_status(order_id, "READY")
            line_reply(reply_token, [msg_text(f"已通知客人可取貨（{order_id}）。\n並已將 cashflow 狀態更新為 READY。")])
            return

        if action == "SHIPPED":
            # 宅配：出貨通知
            text = (
                "🚚 你的訂單已安排出貨\n"
                f"訂單編號：{order_id}\n"
                f"取貨方式：冷凍宅配\n"
                f"希望到貨日：{date}（僅作希望日，實際依物流配送）\n"
                f"內容：{item_summary}\n\n"
                "若有配送疑問，可直接回覆此訊息，我們會協助確認。"
            )
            line_push(customer_id, [msg_text(text)])
            update_cashflow_status(order_id, "SHIPPED")
            line_reply(reply_token, [msg_text(f"已通知客人已出貨（{order_id}）。\n並已將 cashflow 狀態更新為 SHIPPED。")])
            return

        if action == "COPY":
            # 只是為了 displayText 複製，不做事
            line_reply(reply_token, [msg_text(f"訂單號：{order_id}")])
            return

        line_reply(reply_token, [msg_text("未知的管理員操作。")])
        return

    # =========================
    # 一般流程
    # =========================
    if data == "PB:RESET":
        reset_session(sess)
        line_reply(reply_token, [msg_text("已清空。\n請點「我要下單」開始，或點「甜點」先看菜單。")])
        return

    if data == "PB:CONTINUE":
        line_reply(reply_token, [msg_flex("甜點菜單", flex_product_menu(ordering=sess["ordering"]))])
        return

    if data == "PB:CHECKOUT":
        if not sess["ordering"]:
            line_reply(reply_token, [msg_text("請先點「我要下單」開始下單流程。")])
            return
        if not sess["cart"]:
            line_reply(reply_token, [msg_text("購物車是空的，請先選商品。"), msg_flex("甜點菜單", flex_product_menu(ordering=True))])
            return
        sess["state"] = "WAIT_PICKUP_METHOD"
        line_reply(reply_token, [msg_flex("取貨方式", flex_pickup_method())])
        return

    # ITEM
    if data.startswith("PB:ITEM:"):
        if not sess["ordering"]:
            line_reply(reply_token, [msg_text("想下單請先點「我要下單」。\n你也可以點「甜點」先看菜單。")])
            return
        item_key = data.split("PB:ITEM:", 1)[1].strip()
        if item_key not in ITEMS:
            line_reply(reply_token, [msg_text("品項不存在，請重新選擇。")])
            return

        sess["pending_item"] = item_key
        sess["pending_flavor"] = None

        meta = ITEMS[item_key]
        if meta["has_flavor"]:
            sess["state"] = "WAIT_FLAVOR"
            q = [quick_postback(f, f"PB:FLAVOR:{f}", display_text=f) for f in meta["flavors"]]
            line_reply(reply_token, [msg_text(f"你選了：{meta['label']}\n請選口味：", quick_items=q)])
            return
        else:
            sess["state"] = "WAIT_QTY"
            q = build_qty_quick(meta["min_qty"], 12, prefix="PB:QTY:")
            line_reply(reply_token, [msg_text(f"你選了：{meta['label']}\n請選數量：", quick_items=q)])
            return

    # FLAVOR
    if data.startswith("PB:FLAVOR:"):
        flavor = data.split("PB:FLAVOR:", 1)[1].strip()
        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            line_reply(reply_token, [msg_text("流程有點亂掉了，請點「我要下單」重新開始。")])
            return
        if flavor not in ITEMS[item_key]["flavors"]:
            line_reply(reply_token, [msg_text("口味不正確，請重新選。")])
            return

        sess["pending_flavor"] = flavor
        sess["state"] = "WAIT_QTY"
        q = build_qty_quick(ITEMS[item_key]["min_qty"], 12, prefix="PB:QTY:")
        line_reply(reply_token, [msg_text(f"口味：{flavor}\n請選數量：", quick_items=q)])
        return

    # QTY
    if data.startswith("PB:QTY:"):
        qty = int(data.split("PB:QTY:", 1)[1].strip())
        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            line_reply(reply_token, [msg_text("流程有點亂掉了，請點「我要下單」重新開始。")])
            return

        meta = ITEMS[item_key]
        flavor = sess.get("pending_flavor")

        try:
            add_to_cart(user_id, item_key, flavor, qty)
        except Exception as e:
            line_reply(reply_token, [msg_text(f"加入失敗：{e}")])
            return

        sess["pending_item"] = None
        sess["pending_flavor"] = None
        sess["state"] = "IDLE"
        recalc_cart(sess)

        line_reply(reply_token, [
            msg_text("✅ 已加入購物車"),
            msg_flex("結帳內容", flex_checkout_summary(sess)),
        ])
        return

    # PICKUP METHOD
    if data.startswith("PB:PICKUP:"):
        method = data.split("PB:PICKUP:", 1)[1].strip()
        sess["pickup_method"] = method

        date_buttons = build_date_buttons()
        if not date_buttons:
            line_reply(reply_token, [msg_text("目前可選日期都被公休/不出貨日排除。\n請調整 CLOSED_DATES / CLOSED_WEEKDAYS 或 settings。")])
            return

        quick_items = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]

        if method == "店取":
            sess["state"] = "WAIT_PICKUP_DATE"
            line_reply(reply_token, [msg_text("請選「店取日期」（依規則顯示可選日）：", quick_items=quick_items)])
            return

        if method == "宅配":
            sess["state"] = "WAIT_DELIVERY_DATE"
            line_reply(reply_token, [msg_text("請選「希望到貨日期」（依規則顯示可選日；不保證準時到貨，僅作希望日）：", quick_items=quick_items)])
            return

    # DATE
    if data.startswith("PB:DATE:"):
        ymd = data.split("PB:DATE:", 1)[1].strip()

        if sess["state"] == "WAIT_PICKUP_DATE":
            sess["pickup_date"] = ymd
            sess["state"] = "WAIT_PICKUP_TIME"
            q = [
                quick_postback("10:00-12:00", "PB:TIME:10:00-12:00", display_text="10:00-12:00"),
                quick_postback("12:00-14:00", "PB:TIME:12:00-14:00", display_text="12:00-14:00"),
                quick_postback("14:00-16:00", "PB:TIME:14:00-16:00", display_text="14:00-16:00"),
            ]
            line_reply(reply_token, [msg_text(f"✅ 已選店取日期：{ymd}\n請選店取時段：", quick_items=q)])
            return

        if sess["state"] == "WAIT_DELIVERY_DATE":
            sess["delivery_date"] = ymd
            sess["state"] = "WAIT_DELIVERY_NAME"
            line_reply(reply_token, [msg_text(f"✅ 已選希望到貨日期：{ymd}\n請輸入宅配收件人姓名：")])
            return

        line_reply(reply_token, [msg_text("日期已收到，但目前流程不在選日期階段。請點「前往結帳」重新操作。")])
        return

    # TIME
    if data.startswith("PB:TIME:") and sess["state"] == "WAIT_PICKUP_TIME":
        t = data.split("PB:TIME:", 1)[1].strip()
        sess["pickup_time"] = t
        sess["state"] = "WAIT_PICKUP_NAME"
        line_reply(reply_token, [
            msg_text(f"✅ 店取資訊已選好：\n日期：{sess.get('pickup_date')}\n時段：{t}\n地址：{STORE_ADDRESS}\n\n請輸入取件人姓名：")
        ])
        return

    # EDIT MENU
    if data == "PB:EDIT:MENU":
        if not sess["cart"]:
            line_reply(reply_token, [msg_text("購物車是空的，無法修改。")])
            return
        sess["state"] = "EDIT_MENU"
        q = [
            quick_postback("➕ 增加數量", "PB:EDITMODE:INC", display_text="增加數量"),
            quick_postback("➖ 減少數量", "PB:EDITMODE:DEC", display_text="減少數量"),
            quick_postback("🗑 移除品項", "PB:EDITMODE:DEL", display_text="移除品項"),
        ]
        line_reply(reply_token, [msg_text("請選要修改的方式：", quick_items=q)])
        return

    if data.startswith("PB:EDITMODE:"):
        mode = data.split("PB:EDITMODE:", 1)[1].strip()
        sess["edit_mode"] = mode
        sess["state"] = "EDIT_PICK_ITEM"
        q = build_cart_item_choices(sess, mode)
        line_reply(reply_token, [msg_text("請選要修改的品項：", quick_items=q)])
        return

    if data.startswith("PB:EDIT:"):
        parts = data.split(":")
        if len(parts) != 4:
            line_reply(reply_token, [msg_text("修改指令格式錯誤，請重新操作。")])
            return
        mode = parts[2].strip()
        idx = int(parts[3].strip())

        if idx < 0 or idx >= len(sess["cart"]):
            line_reply(reply_token, [msg_text("找不到該品項，請重新操作。")])
            return

        x = sess["cart"][idx]
        item_key = x["item_key"]

        if mode == "INC":
            x["qty"] += ITEMS[item_key]["step"]
        elif mode == "DEC":
            new_qty = x["qty"] - ITEMS[item_key]["step"]
            if not can_dec_item(item_key, new_qty):
                line_reply(reply_token, [msg_text(f"此品項最低數量為 {ITEMS[item_key]['min_qty']}，不能再減了。")])
                return
            x["qty"] = new_qty
        elif mode == "DEL":
            sess["cart"].pop(idx)
        else:
            line_reply(reply_token, [msg_text("未知的修改模式。")])
            return

        recalc_cart(sess)
        sess["state"] = "IDLE"
        sess["edit_mode"] = None

        if not sess["cart"]:
            line_reply(reply_token, [msg_text("✅ 已更新。購物車目前是空的。"), msg_flex("甜點菜單", flex_product_menu(ordering=True))])
            return

        line_reply(reply_token, [
            msg_text("✅ 已更新結帳內容"),
            msg_flex("結帳內容", flex_checkout_summary(sess)),
        ])
        return

    # NEXT
    if data == "PB:NEXT":
        if not sess["cart"]:
            line_reply(reply_token, [msg_text("購物車是空的，請先選商品。")])
            return

        if not sess.get("pickup_method"):
            sess["state"] = "WAIT_PICKUP_METHOD"
            line_reply(reply_token, [msg_flex("取貨方式", flex_pickup_method())])
            return

        if sess["pickup_method"] == "店取":
            if not sess.get("pickup_date"):
                sess["state"] = "WAIT_PICKUP_DATE"
                date_buttons = build_date_buttons()
                quick_items = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]
                line_reply(reply_token, [msg_text("請選店取日期：", quick_items=quick_items)])
                return
            if not sess.get("pickup_time"):
                sess["state"] = "WAIT_PICKUP_TIME"
                q = [
                    quick_postback("10:00-12:00", "PB:TIME:10:00-12:00", display_text="10:00-12:00"),
                    quick_postback("12:00-14:00", "PB:TIME:12:00-14:00", display_text="12:00-14:00"),
                    quick_postback("14:00-16:00", "PB:TIME:14:00-16:00", display_text="14:00-16:00"),
                ]
                line_reply(reply_token, [msg_text("請選店取時段：", quick_items=q)])
                return
            if not sess.get("pickup_name"):
                sess["state"] = "WAIT_PICKUP_NAME"
                line_reply(reply_token, [msg_text("請輸入取件人姓名：")])
                return
            if not sess.get("pickup_phone"):
                sess["state"] = "WAIT_PICKUP_PHONE"
                line_reply(reply_token, [msg_text("請輸入店取電話：")])
                return

        if sess["pickup_method"] == "宅配":
            if not sess.get("delivery_date"):
                sess["state"] = "WAIT_DELIVERY_DATE"
                date_buttons = build_date_buttons()
                quick_items = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]
                line_reply(reply_token, [msg_text("請選希望到貨日期：", quick_items=quick_items)])
                return
            if not sess.get("delivery_name"):
                sess["state"] = "WAIT_DELIVERY_NAME"
                line_reply(reply_token, [msg_text("請輸入宅配收件人姓名：")])
                return
            if not sess.get("delivery_phone"):
                sess["state"] = "WAIT_DELIVERY_PHONE"
                line_reply(reply_token, [msg_text("請輸入宅配電話：")])
                return
            if not sess.get("delivery_address"):
                sess["state"] = "WAIT_DELIVERY_ADDRESS"
                line_reply(reply_token, [msg_text("請輸入宅配地址（完整地址）：")])
                return

        # 建單
        order_id = create_order(user_id, sess)

        total = cart_total(sess["cart"])
        fee = shipping_fee(total) if sess["pickup_method"] == "宅配" else 0
        grand = total + fee
        summary_lines = "\n".join([f"• {find_cart_line_label(x)}" for x in sess["cart"]])

        if sess["pickup_method"] == "店取":
            msg = (
                "✅ 訂單已建立\n"
                f"訂單編號：{order_id}\n\n"
                f"{summary_lines}\n\n"
                f"取貨方式：店取\n"
                f"日期：{sess['pickup_date']}\n"
                f"時段：{sess['pickup_time']}\n"
                f"取件人：{sess['pickup_name']}\n"
                f"電話：{sess['pickup_phone']}\n"
                f"地址：{STORE_ADDRESS}\n\n"
                f"小計：NT${total}\n\n"
                + BANK_TRANSFER_TEXT
            )
        else:
            msg = (
                "✅ 訂單已建立\n"
                f"訂單編號：{order_id}\n\n"
                f"{summary_lines}\n\n"
                f"取貨方式：冷凍宅配\n"
                f"希望到貨日期：{sess['delivery_date']}（不保證準時）\n"
                f"收件人：{sess['delivery_name']}\n"
                f"電話：{sess['delivery_phone']}\n"
                f"地址：{sess['delivery_address']}\n\n"
                f"小計：NT${total}\n"
                f"運費：NT${fee}\n"
                f"應付：NT${grand}\n\n"
                + DELIVERY_NOTICE
                + "\n\n"
                + BANK_TRANSFER_TEXT
            )

        # 清空
        reset_session(sess)

        line_reply(reply_token, [msg_text(msg)])
        return

    line_reply(reply_token, [msg_text("已收到操作，但流程未對上。請點「我要下單」重新開始。")])


# =========================
# State text handlers
# =========================
def handle_state_text(user_id: str, reply_token: str, text: str):
    sess = get_session(user_id)

    if not sess["ordering"]:
        line_reply(reply_token, [msg_flex("提示", flex_home_hint())])
        return

    if sess["state"] == "WAIT_PICKUP_NAME":
        sess["pickup_name"] = text.strip()
        sess["state"] = "WAIT_PICKUP_PHONE"
        line_reply(reply_token, [msg_text("請輸入店取電話：")])
        return

    if sess["state"] == "WAIT_PICKUP_PHONE":
        sess["pickup_phone"] = text.strip()
        sess["state"] = "IDLE"
        line_reply(reply_token, [msg_text("✅ 已收到店取資訊"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

    if sess["state"] == "WAIT_DELIVERY_NAME":
        sess["delivery_name"] = text.strip()
        sess["state"] = "WAIT_DELIVERY_PHONE"
        line_reply(reply_token, [msg_text("請輸入宅配電話：")])
        return

    if sess["state"] == "WAIT_DELIVERY_PHONE":
        sess["delivery_phone"] = text.strip()
        sess["state"] = "WAIT_DELIVERY_ADDRESS"
        line_reply(reply_token, [msg_text("請輸入宅配地址（完整地址）：")])
        return

    if sess["state"] == "WAIT_DELIVERY_ADDRESS":
        sess["delivery_address"] = text.strip()
        sess["state"] = "IDLE"
        line_reply(reply_token, [msg_text("✅ 已收到宅配資訊"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

    line_reply(reply_token, [msg_text("我有收到你的訊息，但目前建議用按鈕操作。\n若要開始下單請點「我要下單」，要看菜單請點「甜點」。")])
