import os
import json
import base64
import hmac
import hashlib
import random
import string
import re
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Any, Optional, List, Tuple

import requests
from fastapi import FastAPI, Request, HTTPException, Response, BackgroundTasks
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

# A/B/C 表（你說 A=orders）
SHEET_A_NAME = os.getenv("SHEET_NAME", "orders").strip()  # A表（orders）
SHEET_B_NAME = os.getenv("SHEET_B_NAME", "order_items_readable").strip()  # B表（items明細）

# C表：你要保留 c_log
SHEET_C_NAME = os.getenv("SHEET_C_NAME", "c_log").strip()  # C表（log）
# ✅ 新增：cashflow 表（跟 c_log 同格式）
SHEET_CASHFLOW_NAME = os.getenv("SHEET_CASHFLOW_NAME", "cashflow").strip()

SHEET_SETTINGS_NAME = os.getenv("SHEET_SETTINGS_NAME", "settings").strip()  # settings（可無）

# 管理員 ID（逗號分隔）
ADMIN_USER_IDS = [x.strip() for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()]

TZ = timezone(timedelta(hours=8))  # Asia/Taipei

LINE_API_BASE = "https://api.line.me/v2/bot/message"

PICKUP_ADDRESS = os.getenv("PICKUP_ADDRESS", "新竹縣竹北市隘口六街65號").strip()

BANK_TRANSFER_TEXT = os.getenv(
    "BANK_TRANSFER_TEXT",
    "付款方式：轉帳（對帳後依訂單號安排出貨/取貨）\n"
    "台灣銀行 004\n"
    "帳號：248-001-03430-6\n\n"
    "轉帳後請回傳：\n"
    "「已轉帳 訂單編號 末五碼12345」"
).strip()

DELIVERY_NOTICE = os.getenv(
    "DELIVERY_NOTICE",
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
).strip()

PICKUP_NOTICE = os.getenv(
    "PICKUP_NOTICE",
    f"店取地址：\n{PICKUP_ADDRESS}\n\n提醒：所有甜點需提前3天預訂。"
).strip()


def safe_int_env(key: str, default: int) -> int:
    """
    Render / ENV 有時候會出現 '(3)' 這種字串，int() 會炸。
    這裡做最保險的解析：抓出第一段數字。
    """
    raw = (os.getenv(key, "") or "").strip()
    if not raw:
        return default
    m = re.search(r"-?\d+", raw)
    if not m:
        return default
    try:
        return int(m.group(0))
    except:
        return default


# 日期規則
MIN_DAYS = safe_int_env("MIN_DAYS", 3)
MAX_DAYS = safe_int_env("MAX_DAYS", 14)

# 公休日（ENV 可先用，settings sheet 可覆蓋）
ENV_CLOSED_WEEKDAYS = os.getenv("CLOSED_WEEKDAYS", "2").strip()
ENV_CLOSED_DATES = os.getenv("CLOSED_DATES", "").strip()

# 店取時段
PICKUP_SLOTS = ["10:00-12:00", "12:00-14:00", "14:00-16:00"]


# =========================
# App
# =========================
app = FastAPI()

# ✅ 給 UptimeRobot 免費版 HEAD 用：永遠回 200
@app.head("/")
def head_root():
    return Response(status_code=200)

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

            "pickup_method": None,
            "pickup_date": None,
            "pickup_time": None,
            "pickup_name": None,
            "pickup_phone": None,
            "pickup_phone_ok": False,

            "delivery_date": None,
            "delivery_name": None,
            "delivery_phone": None,
            "delivery_phone_ok": False,
            "delivery_address": None,

            "edit_mode": None,

            # 防止「容易沒反應」：同一秒連點同一 postback 直接忽略
            "last_postback_data": None,
            "last_postback_ts": 0.0,
        }
    return SESSIONS[user_id]


# =========================
# Menu / Items
# =========================
DACQUOISE_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]
TOAST_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

ITEMS = {
    "dacquoise": {"label": "達克瓦茲", "unit_price": 95, "has_flavor": True,  "flavors": DACQUOISE_FLAVORS, "min_qty": 1, "step": 1},
    "scone":     {"label": "原味司康", "unit_price": 65, "has_flavor": False, "flavors": [],               "min_qty": 1, "step": 1},
    "canele6":   {"label": "可麗露 6顆/盒", "unit_price": 490, "has_flavor": False, "flavors": [],        "min_qty": 1, "step": 1},
    "toast":     {"label": "伊思尼奶酥厚片", "unit_price": 85, "has_flavor": True, "flavors": TOAST_FLAVORS,"min_qty": 1, "step": 1},
}


# =========================
# LINE API (no SDK)
# =========================
def line_headers() -> dict:
    return {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def line_reply(reply_token: str, messages: List[dict]):
    if not CHANNEL_ACCESS_TOKEN:
        return
    # 保險：過濾空訊息（避免 LINE 400）
    safe_msgs = []
    for m in (messages or []):
        if not m:
            continue
        if m.get("type") == "text" and not (m.get("text") or "").strip():
            continue
        if m.get("type") == "flex" and (not m.get("altText") or not m.get("contents")):
            continue
        safe_msgs.append(m)
    if not safe_msgs:
        safe_msgs = [{"type": "text", "text": "收到～"}]

    payload = {"replyToken": reply_token, "messages": safe_msgs}
    r = requests.post(
        f"{LINE_API_BASE}/reply",
        headers=line_headers(),
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        timeout=12,
    )
    if r.status_code >= 300:
        print("[ERROR] reply failed:", r.status_code, r.text)


def line_push(user_id: str, messages: List[dict]):
    if not CHANNEL_ACCESS_TOKEN:
        return
    safe_msgs = []
    for m in (messages or []):
        if not m:
            continue
        if m.get("type") == "text" and not (m.get("text") or "").strip():
            continue
        if m.get("type") == "flex" and (not m.get("altText") or not m.get("contents")):
            continue
        safe_msgs.append(m)
    if not safe_msgs:
        return

    payload = {"to": user_id, "messages": safe_msgs}
    r = requests.post(
        f"{LINE_API_BASE}/push",
        headers=line_headers(),
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        timeout=12,
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
    if not alt_text:
        alt_text = "訊息"
    if not contents:
        contents = {"type": "bubble", "body": {"type": "box", "layout": "vertical", "contents": [{"type": "text", "text": "…" }]}}
    return {"type": "flex", "altText": alt_text, "contents": contents}


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


def sheet_append(sheet_name: str, row: List[Any]) -> bool:
    if not GSHEET_ID:
        print("[WARN] GSHEET_ID missing, skip append.")
        return False
    service = get_sheets_service()
    if not service:
        print("[WARN] Google Sheet env missing, skip append.")
        return False
    try:
        range_ = f"'{sheet_name}'!A1"
        body = {"values": [row]}
        service.spreadsheets().values().append(
            spreadsheetId=GSHEET_ID,
            range=range_,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()
        return True
    except Exception as e:
        print(f"[ERROR] append to {sheet_name} failed:", e)
        return False


def sheet_read_range(sheet_name: str, a1: str) -> List[List[str]]:
    service = get_sheets_service()
    if not service or not GSHEET_ID:
        return []
    try:
        r = service.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"'{sheet_name}'!{a1}"
        ).execute()
        return r.get("values", []) or []
    except Exception as e:
        print(f"[WARN] read range failed {sheet_name} {a1}:", e)
        return []


def sheet_update_a1(sheet_name: str, a1: str, values_2d: List[List[Any]]) -> bool:
    service = get_sheets_service()
    if not service or not GSHEET_ID:
        return False
    try:
        service.spreadsheets().values().update(
            spreadsheetId=GSHEET_ID,
            range=f"'{sheet_name}'!{a1}",
            valueInputOption="RAW",
            body={"values": values_2d},
        ).execute()
        return True
    except Exception as e:
        print(f"[ERROR] update range failed {sheet_name} {a1}:", e)
        return False


# =========================
# Settings: 公休
# =========================
def parse_int_list(s: str) -> List[int]:
    out = []
    for x in (s or "").split(","):
        x = x.strip()
        if not x:
            continue
        try:
            out.append(int(x))
        except:
            pass
    return out


def parse_date_set(s: str) -> set:
    out = set()
    for x in (s or "").split(","):
        x = x.strip()
        if not x:
            continue
        out.add(x)
    return out


def load_settings() -> Dict[str, Any]:
    settings = {
        "closed_weekdays": parse_int_list(ENV_CLOSED_WEEKDAYS),
        "closed_dates": parse_date_set(ENV_CLOSED_DATES),
        "min_days": MIN_DAYS,
        "max_days": MAX_DAYS,
    }

    try:
        rows = sheet_read_range(SHEET_SETTINGS_NAME, "A1:B200")
        if rows and len(rows) >= 2:
            for r in rows[1:]:
                if len(r) < 2:
                    continue
                k = (r[0] or "").strip()
                v = (r[1] or "").strip()
                if not k:
                    continue
                if k == "closed_weekdays":
                    settings["closed_weekdays"] = parse_int_list(v)
                elif k == "closed_dates":
                    settings["closed_dates"] = parse_date_set(v)
                elif k == "min_days":
                    try:
                        settings["min_days"] = int(v)
                    except:
                        pass
                elif k == "max_days":
                    try:
                        settings["max_days"] = int(v)
                    except:
                        pass
    except Exception as e:
        print("[INFO] settings sheet not loaded, use ENV:", e)

    return settings


def weekday_user_to_py(wd: int) -> int:
    if 1 <= wd <= 7:
        return wd - 1
    return wd


def is_closed(d: date, settings: Dict[str, Any]) -> bool:
    ymd = d.strftime("%Y-%m-%d")
    if ymd in settings["closed_dates"]:
        return True
    for wd in settings["closed_weekdays"]:
        if d.weekday() == weekday_user_to_py(wd):
            return True
    return False


def fmt_md_date(dt: datetime) -> str:
    wk = "一二三四五六日"[dt.weekday()]
    return f"{dt.month}/{dt.day}（{wk}）"


def build_available_date_buttons(settings: Dict[str, Any]) -> List[Tuple[str, str]]:
    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    out = []
    for i in range(settings["min_days"], settings["max_days"] + 1):
        d = today + timedelta(days=i)
        if not is_closed(d.date(), settings):
            out.append((fmt_md_date(d), d.strftime("%Y-%m-%d")))
    return out


# =========================
# Helpers
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


def recalc_cart(sess: dict):
    for x in sess["cart"]:
        x["subtotal"] = int(x["unit_price"]) * int(x["qty"])


def find_cart_line_label(x: dict) -> str:
    name = x["label"]
    if x.get("flavor"):
        name += f"（{x['flavor']}）"
    qty = x["qty"]
    unit = x["unit_price"]
    sub = x["subtotal"]
    return f"{name} ×{qty}（{unit}/單位）＝{sub}"


def cart_readable_text(cart: List[dict]) -> str:
    parts = []
    for x in cart:
        label = x["label"]
        qty = x["qty"]
        flavor = (x.get("flavor") or "").strip()
        if flavor:
            parts.append(f"{label}｜{qty}｜{flavor}")
        else:
            parts.append(f"{label}｜{qty}")
    return "；".join(parts)


def is_phone_digits(s: str) -> bool:
    s = (s or "").strip()
    return s.isdigit() and 8 <= len(s) <= 10


# =========================
# Flex builders（純色系、統一）
# =========================
def flex_home_hint() -> dict:
    return {
        "type": "bubble",
        "body": {"type":"box","layout":"vertical","spacing":"md","contents":[
            {"type":"text","text":"UooUoo 甜點訂購","weight":"bold","size":"xl"},
            {"type":"text","text":"• 點「甜點」只看菜單\n• 點「我要下單」才會開始下訂流程",
             "wrap":True,"size":"sm","color":"#666666"},
        ]}
    }


def flex_menu_view_only() -> dict:
    rows = []
    for _, meta in ITEMS.items():
        rows.append({
            "type":"box","layout":"horizontal","contents":[
                {"type":"text","text":meta["label"],"flex":7,"wrap":True},
                {"type":"text","text":f"NT${meta['unit_price']}", "flex":3,"align":"end","color":"#666666"},
            ]
        })
    return {
        "type":"bubble",
        "size":"mega",
        "body":{"type":"box","layout":"vertical","spacing":"md","contents":[
            {"type":"text","text":"甜點菜單","weight":"bold","size":"xl"},
            {"type":"text","text":"（點「我要下單」才會開始下訂流程）","size":"sm","color":"#666666","wrap":True},
            {"type":"separator"},
            *rows,
        ]}
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
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
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
        ]},
    }


def flex_pickup_method() -> dict:
    return {
        "type": "bubble",
        "body": {"type":"box","layout":"vertical","spacing":"md","contents":[
            {"type":"text","text":"請選擇店取或宅配","weight":"bold","size":"xl"},
            {"type":"text","text":"（日期會自動排除公休/不出貨日）","size":"sm","color":"#666666"},
            {"type":"button","style":"primary","action":{"type":"postback","label":"🏪 店取","data":"PB:PICKUP:店取","displayText":"店取"}},
            {"type":"button","style":"primary","action":{"type":"postback","label":"🚚 冷凍宅配","data":"PB:PICKUP:宅配","displayText":"冷凍宅配"}},
        ]}
    }


def flex_phone_confirm(phone: str, kind: str) -> dict:
    ok_data = f"PB:PHONE_OK:{kind}"
    retry_data = f"PB:PHONE_RETRY:{kind}"
    return {
        "type":"bubble",
        "body":{"type":"box","layout":"vertical","spacing":"md","contents":[
            {"type":"text","text":"電話二次確認","weight":"bold","size":"xl"},
            {"type":"text","text":f"你填的電話：{phone}","size":"md","wrap":True},
            {"type":"text","text":"請確認正確，避免通知不到你。","size":"sm","color":"#666666","wrap":True},
        ]},
        "footer":{"type":"box","layout":"vertical","spacing":"sm","contents":[
            {"type":"button","style":"primary","action":{"type":"postback","label":"✅ 正確","data":ok_data,"displayText":"電話正確"}},
            {"type":"button","style":"secondary","action":{"type":"postback","label":"✏️ 重新輸入","data":retry_data,"displayText":"重新輸入電話"}},
        ]}
    }


def flex_checkout_summary(sess: dict) -> dict:
    cart = sess["cart"]
    lines = [find_cart_line_label(x) for x in cart]
    total = cart_total(cart)

    method = sess.get("pickup_method") or "（未選）"

    if method == "宅配":
        fee = shipping_fee(total)
        grand = total + fee
        date_show = sess.get("delivery_date") or "（未選）"
        time_show = "—"
        bottom_text = f"小計：NT${total}\n運費：NT${fee}\n應付：NT${grand}"
    elif method == "店取":
        date_show = sess.get("pickup_date") or "（未選）"
        time_show = sess.get("pickup_time") or "（未選）"
        bottom_text = f"小計：NT${total}"
    else:
        date_show = "（未選）"
        time_show = "（未選）"
        bottom_text = f"小計：NT${total}"

    shown = lines[:10]
    if len(lines) > 10:
        shown.append(f"…等 {len(lines)} 項（請先刪減購物車）")

    list_text = "\n".join([f"• {s}" for s in shown]) if shown else "（購物車是空的）"

    return {
        "type": "bubble",
        "size": "mega",
        "body": {"type":"box","layout":"vertical","spacing":"md","contents":[
            {"type":"text","text":"🧾 結帳內容","weight":"bold","size":"xl"},
            {"type":"text","text":list_text,"wrap":True,"size":"sm"},
            {"type":"separator","margin":"md"},
            {"type":"text","text":f"取貨方式：{method}","size":"sm","color":"#666666"},
            {"type":"text","text":f"日期：{date_show}","size":"sm","color":"#666666"},
            {"type":"text","text":f"時段：{time_show}","size":"sm","color":"#666666"},
            {"type":"separator","margin":"md"},
            {"type":"text","text":bottom_text,"weight":"bold","size":"lg"},
        ]},
        "footer": {"type":"box","layout":"vertical","spacing":"sm","contents":[
            {"type":"button","style":"primary","action":{"type":"postback","label":"🛠 修改品項","data":"PB:EDIT:MENU","displayText":"修改品項"}},
            {"type":"button","style":"secondary","action":{"type":"postback","label":"➕ 繼續加購","data":"PB:CONTINUE","displayText":"繼續加購"}},
            {"type":"button","style":"secondary","action":{"type":"postback","label":"✅ 下一步","data":"PB:NEXT","displayText":"下一步"}},
        ]}
    }


def flex_admin_order_actions(order_id: str, method: str, current_status: str = "UNPAID") -> dict:
    """
    商家後台卡片（不噴 debug）
    1) 已收款
    2) 店取：已做好 / 宅配：已出貨
    3) 今日待辦總覽
    """
    buttons = []

    if current_status != "PAID":
        buttons.append({
            "type":"button",
            "style":"primary",
            "action":{"type":"postback","label":"💰 已收款","data":f"ADMIN:PAID:{order_id}","displayText":"已收款"},
        })

    if method == "店取":
        buttons.append({
            "type":"button",
            "style":"secondary",
            "action":{"type":"postback","label":"📣 已做好，通知取貨","data":f"ADMIN:READY:{order_id}","displayText":"已做好"},
        })
    else:
        buttons.append({
            "type":"button",
            "style":"secondary",
            "action":{"type":"postback","label":"🚚 已出貨，通知客人","data":f"ADMIN:SHIPPED:{order_id}","displayText":"已出貨"},
        })

    buttons.append({
        "type":"button",
        "style":"secondary",
        "action":{"type":"postback","label":"📋 今日待辦總覽","data":"ADMIN:SUMMARY:TODAY","displayText":"今日待辦"},
    })

    return {
        "type":"bubble",
        "body":{"type":"box","layout":"vertical","spacing":"md","contents":[
            {"type":"text","text":"🧁 新訂單提醒","weight":"bold","size":"xl"},
            {"type":"text","text":f"訂單編號：{order_id}","wrap":True,"size":"sm","color":"#666666"},
            {"type":"text","text":f"取貨方式：{method}","wrap":True,"size":"sm","color":"#666666"},
        ]},
        "footer":{"type":"box","layout":"vertical","spacing":"sm","contents":buttons}
    }


# =========================
# Cart operations
# =========================
def add_to_cart(user_id: str, item_key: str, flavor: Optional[str], qty: int):
    sess = get_session(user_id)
    meta = ITEMS[item_key]

    if meta["has_flavor"] and not flavor:
        raise ValueError("缺少口味")
    if qty < meta["min_qty"]:
        raise ValueError(f"數量至少 {meta['min_qty']}")

    unit = meta["unit_price"]
    subtotal = unit * qty

    sess["cart"].append({
        "item_key": item_key,
        "label": meta["label"],
        "flavor": flavor or "",
        "qty": qty,
        "unit_price": unit,
        "subtotal": subtotal,
    })


def can_dec_item(item_key: str, new_qty: int) -> bool:
    min_qty = ITEMS[item_key]["min_qty"]
    return new_qty >= min_qty


def build_cart_item_choices(sess: dict, mode: str) -> List[dict]:
    items = []
    for idx, x in enumerate(sess["cart"]):
        label = x["label"]
        if x.get("flavor"):
            label += f"（{x['flavor']}）"
        label += f" ×{x['qty']}"
        items.append(quick_postback(label, f"PB:EDIT:{mode}:{idx}", display_text=label))
    return items


def build_qty_quick(min_qty: int, max_qty: int, prefix: str) -> List[dict]:
    return [quick_postback(str(i), f"{prefix}{i}", display_text=str(i)) for i in range(min_qty, max_qty + 1)]


# =========================
# Order write: A/B/C
# =========================
def write_order_A(user_id: str, order_id: str, sess: dict) -> bool:
    cart = sess["cart"]
    total = cart_total(cart)

    pickup_method = sess.get("pickup_method") or ""
    pickup_date = sess.get("pickup_date") or ""
    pickup_time = sess.get("pickup_time") or ""

    note = ""
    if pickup_method == "宅配":
        delivery_date = sess.get("delivery_date") or ""
        dn = sess.get("delivery_name") or ""
        dp = sess.get("delivery_phone") or ""
        da = sess.get("delivery_address") or ""
        note = f"期望到貨:{delivery_date} | 收件人:{dn} | 電話:{dp} | 地址:{da}"
        pickup_date = delivery_date
        pickup_time = ""

    if pickup_method == "店取":
        pn = sess.get("pickup_name") or ""
        pp = sess.get("pickup_phone") or ""
        note = f"取件人:{pn} | 電話:{pp}"

    rowA = [
        now_str(),                               # A created_at
        user_id,                                 # B user_id
        "",                                      # C display_name（先留空）
        order_id,                                # D order_id
        json.dumps({"cart": cart}, ensure_ascii=False),  # E raw_json
        pickup_method,                           # F method
        pickup_date,                             # G pickup_date
        pickup_time,                             # H pickup_time
        note,                                    # I note
        total,                                   # J total
        "UNPAID",                                # K status（最新狀態）
        cart_readable_text(cart),                # L transaction_note（白話）
    ]
    return sheet_append(SHEET_A_NAME, rowA)


def write_order_B(order_id: str, sess: dict) -> bool:
    ok_all = True
    created_at = now_str()
    pickup_method = sess.get("pickup_method") or ""
    pickup_date = sess.get("pickup_date") or ""
    pickup_time = sess.get("pickup_time") or ""

    if pickup_method == "宅配":
        pickup_date = sess.get("delivery_date") or ""
        pickup_time = ""

    phone = sess.get("pickup_phone") if pickup_method == "店取" else sess.get("delivery_phone")

    for it in sess["cart"]:
        item_name = it["label"]
        flavor = (it.get("flavor") or "").strip()
        spec = ""

        rowB = [
            created_at,
            order_id,
            item_name,
            spec,
            flavor,
            it["qty"],
            it["unit_price"],
            it["subtotal"],
            pickup_method,
            pickup_date,
            pickup_time,
            phone or "",
        ]
        ok = sheet_append(SHEET_B_NAME, rowB)
        ok_all = ok_all and ok

    return ok_all


def _append_log_to_both(row: List[Any]) -> Tuple[bool, bool]:
    ok1 = sheet_append(SHEET_C_NAME, row)
    ok2 = sheet_append(SHEET_CASHFLOW_NAME, row)
    return ok1, ok2


def write_order_C_order(order_id: str, sess: dict) -> bool:
    created_at = now_str()
    method = sess.get("pickup_method") or ""
    amount = cart_total(sess["cart"])
    fee = shipping_fee(amount) if method == "宅配" else 0
    grand = amount + fee

    if method == "店取":
        note = f"店取 {sess.get('pickup_date','')} {sess.get('pickup_time','')} | {sess.get('pickup_name','')} | {sess.get('pickup_phone','')}"
    else:
        note = f"宅配 期望到貨:{sess.get('delivery_date','')} | {sess.get('delivery_name','')} | {sess.get('delivery_phone','')} | {sess.get('delivery_address','')}"

    # ✅ c_log + cashflow 同格式雙寫
    row = [created_at, order_id, "ORDER", method, amount, fee, grand, "ORDER", note]
    ok1, ok2 = _append_log_to_both(row)
    return bool(ok1 and ok2)


def append_C_status(order_id: str, status: str, note: str) -> bool:
    row = [now_str(), order_id, "STATUS", "", "", "", "", status, note]
    ok1, ok2 = _append_log_to_both(row)
    return bool(ok1 and ok2)


def find_user_id_by_order_id(order_id: str) -> Optional[str]:
    rows = sheet_read_range(SHEET_A_NAME, "A1:L5000")
    if not rows or len(rows) < 2:
        return None
    for r in rows[1:]:
        if len(r) >= 4 and (r[3] or "").strip() == order_id:
            return (r[1] or "").strip()
    return None


def get_A_row_index_by_order_id(order_id: str) -> Optional[int]:
    rows = sheet_read_range(SHEET_A_NAME, "A1:D5000")
    if not rows or len(rows) < 2:
        return None
    for i, r in enumerate(rows[1:], start=2):
        if len(r) >= 4 and (r[3] or "").strip() == order_id:
            return i
    return None


def get_A_status_by_order_id(order_id: str) -> Optional[str]:
    row_idx = get_A_row_index_by_order_id(order_id)
    if not row_idx:
        return None
    rows = sheet_read_range(SHEET_A_NAME, f"K{row_idx}:K{row_idx}")
    if rows and rows[0]:
        return (rows[0][0] or "").strip()
    return ""


def update_A_table_status(order_id: str, new_status: str) -> bool:
    row_idx = get_A_row_index_by_order_id(order_id)
    if not row_idx:
        return False
    return sheet_update_a1(SHEET_A_NAME, f"K{row_idx}", [[new_status]])


# =========================
# 統一狀態入口（不噴 debug）
# =========================
def update_order_status(
    reply_token: str,
    admin_user_id: str,
    order_id: str,
    new_status: str,
    admin_message: str,
    customer_message: Optional[str] = None,
):
    current = get_A_status_by_order_id(order_id) or ""
    if current.strip().upper() == new_status.strip().upper():
        line_reply(reply_token, [msg_text("這筆訂單已經更新過囉～不用重複按 ✅")])
        return

    okA = update_A_table_status(order_id, new_status)
    okC = append_C_status(order_id, new_status, admin_message)

    if okA and okC:
        line_reply(reply_token, [msg_text(admin_message)])
    else:
        line_reply(reply_token, [msg_text("我有幫你按，但表單寫入好像沒成功，麻煩你看一下 Google Sheet 欄位/權限。")])

    if customer_message:
        target_user = find_user_id_by_order_id(order_id)
        if target_user:
            line_push(target_user, [msg_text(customer_message)])


# =========================
# 今日待辦總覽（商家用）
# =========================
def build_today_summary_text() -> str:
    rows = sheet_read_range(SHEET_A_NAME, "A1:K5000")
    if not rows or len(rows) < 2:
        return "今天還沒有訂單～"

    today = datetime.now(TZ).strftime("%Y-%m-%d")
    unp, paid, ready, shipped = 0, 0, 0, 0

    for r in rows[1:]:
        if len(r) < 11:
            continue
        created_at = (r[0] or "").strip()
        status = (r[10] or "").strip().upper()
        if not created_at.startswith(today):
            continue
        if status == "UNPAID":
            unp += 1
        elif status == "PAID":
            paid += 1
        elif status == "READY":
            ready += 1
        elif status == "SHIPPED":
            shipped += 1

    return (
        f"📋 今日待辦（{today}）\n"
        f"• 未收款 UNPAID：{unp}\n"
        f"• 已收款 PAID：{paid}\n"
        f"• 店取待通知 READY：{ready}\n"
        f"• 宅配待通知 SHIPPED：{shipped}\n\n"
        "小提醒：以 A表 status 為主（最不會漏）。"
    )


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
    return {"ok": True, "service": "uoo-line-bot"}


def _process_events(events: List[dict]):
    for ev in events:
        try:
            handle_event(ev)
        except Exception as e:
            print("[ERROR] handle_event:", e)


@app.post("/callback")
async def callback(request: Request, background_tasks: BackgroundTasks):
    """
    ✅ 秒回 OK：先回 200 給 LINE，避免冷啟/慢寫 sheet 導致 webhook 失敗
    事件改用背景處理。
    """
    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")

    if not verify_line_signature(body, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(body.decode("utf-8"))
    events = payload.get("events", [])

    # ✅ 背景處理事件，HTTP 立刻回 OK
    background_tasks.add_task(_process_events, events)
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

        if text == "甜點":
            line_reply(reply_token, [msg_flex("甜點菜單", flex_menu_view_only())])
            return

        if text == "我要下單":
            sess["ordering"] = True
            sess["state"] = "IDLE"
            line_reply(reply_token, [
                msg_text("好的～開始下單。\n請從菜單選擇商品加入購物車。"),
                msg_flex("甜點菜單", flex_product_menu(ordering=True)),
            ])
            return

        if text in ["清空重來", "清空", "reset"]:
            reset_session(sess)
            line_reply(reply_token, [msg_text("已清空～\n請點「我要下單」開始，或點「甜點」先看菜單。")])
            return

        if text == "取貨說明":
            line_reply(reply_token, [msg_text(PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE)])
            return

        if text == "付款說明":
            line_reply(reply_token, [msg_text(BANK_TRANSFER_TEXT)])
            return

        if text.startswith("已轉帳"):
            line_reply(reply_token, [msg_text("收到～我們會核對帳款後安排出貨/取貨。\n若需補充資訊也可以直接留言。")])
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
    sess["pickup_phone_ok"] = False

    sess["delivery_date"] = None
    sess["delivery_name"] = None
    sess["delivery_phone"] = None
    sess["delivery_phone_ok"] = False
    sess["delivery_address"] = None

    sess["edit_mode"] = None
    sess["last_postback_data"] = None
    sess["last_postback_ts"] = 0.0


def too_fast_duplicate(sess: dict, data: str) -> bool:
    now_ts = datetime.now(TZ).timestamp()
    if sess.get("last_postback_data") == data and (now_ts - float(sess.get("last_postback_ts", 0.0))) < 1.0:
        return True
    sess["last_postback_data"] = data
    sess["last_postback_ts"] = now_ts
    return False


# =========================
# Postback flows
# =========================
def handle_postback(user_id: str, reply_token: str, data: str):
    sess = get_session(user_id)

    if too_fast_duplicate(sess, data):
        return

    # ---- 管理員功能 ----
    if data.startswith("ADMIN:"):
        if ADMIN_USER_IDS and user_id not in ADMIN_USER_IDS:
            line_reply(reply_token, [msg_text("此功能僅限商家管理員使用～")])
            return

        parts = data.split(":")
        if len(parts) < 2:
            line_reply(reply_token, [msg_text("指令格式錯誤～")])
            return

        act = parts[1].strip()

        if act == "SUMMARY":
            line_reply(reply_token, [msg_text(build_today_summary_text())])
            return

        if len(parts) != 3:
            line_reply(reply_token, [msg_text("指令格式錯誤～")])
            return

        order_id = parts[2].strip()

        if act == "PAID":
            update_order_status(
                reply_token=reply_token,
                admin_user_id=user_id,
                order_id=order_id,
                new_status="PAID",
                admin_message="💰 收款完成，開始製作囉",
                customer_message=f"💰 已收到款項，我們會開始製作。\n訂單編號：{order_id}",
            )
            return

        if act == "READY":
            update_order_status(
                reply_token=reply_token,
                admin_user_id=user_id,
                order_id=order_id,
                new_status="READY",
                admin_message="📣 已做好，已通知客人取貨",
                customer_message=f"📣 你的甜點已完成，可以來取貨囉！\n訂單編號：{order_id}\n如需更改取貨時間請回覆訊息。",
            )
            return

        if act == "SHIPPED":
            update_order_status(
                reply_token=reply_token,
                admin_user_id=user_id,
                order_id=order_id,
                new_status="SHIPPED",
                admin_message="🚚 已出貨，已通知客人",
                customer_message=f"🚚 你的訂單已出貨。\n訂單編號：{order_id}\n提醒：運送可能因天候/物流量延遲。",
            )
            return

        line_reply(reply_token, [msg_text("我看不懂這個按鈕耶～")])
        return

    # RESET
    if data == "PB:RESET":
        reset_session(sess)
        line_reply(reply_token, [msg_text("已清空～\n請點「我要下單」開始，或點「甜點」先看菜單。")])
        return

    # CONTINUE
    if data == "PB:CONTINUE":
        if not sess["ordering"]:
            line_reply(reply_token, [msg_text("請先點「我要下單」開始下單流程～")])
            return
        line_reply(reply_token, [msg_flex("甜點菜單", flex_product_menu(ordering=True))])
        return

    # CHECKOUT entry
    if data == "PB:CHECKOUT":
        if not sess["ordering"]:
            line_reply(reply_token, [msg_text("請先點「我要下單」開始下單流程～")])
            return
        if not sess["cart"]:
            line_reply(reply_token, [msg_text("購物車是空的～先選商品喔"), msg_flex("甜點菜單", flex_product_menu(ordering=True))])
            return

        sess["state"] = "WAIT_PICKUP_METHOD"
        line_reply(reply_token, [msg_flex("取貨方式", flex_pickup_method())])
        return

    # ITEM
    if data.startswith("PB:ITEM:"):
        if not sess["ordering"]:
            line_reply(reply_token, [msg_text("想下單請先點「我要下單」～\n你也可以點「甜點」先看菜單。")])
            return

        item_key = data.split("PB:ITEM:", 1)[1].strip()
        if item_key not in ITEMS:
            line_reply(reply_token, [msg_text("品項不存在～請重新選擇。")])
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
            line_reply(reply_token, [msg_text("流程好像亂掉了～請點「我要下單」重新開始。")])
            return
        if flavor not in ITEMS[item_key]["flavors"]:
            line_reply(reply_token, [msg_text("口味不正確～請重新選。")])
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
            line_reply(reply_token, [msg_text("流程好像亂掉了～請點「我要下單」重新開始。")])
            return

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

        settings = load_settings()
        date_buttons = build_available_date_buttons(settings)
        if not date_buttons:
            line_reply(reply_token, [msg_text("近期可選日期不足（可能都遇到公休/不出貨日）。")])
            return
        quick_items = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]

        if method == "店取":
            sess["state"] = "WAIT_PICKUP_DATE"
            line_reply(reply_token, [msg_text("請選「店取日期」（3～14天內，已排除公休）：", quick_items=quick_items)])
            return

        if method == "宅配":
            sess["state"] = "WAIT_DELIVERY_DATE"
            line_reply(reply_token, [msg_text("請選「期望到貨日」（3～14天內；僅期望日；已排除公休）：", quick_items=quick_items)])
            return

    # DATE
    if data.startswith("PB:DATE:"):
        ymd = data.split("PB:DATE:", 1)[1].strip()
        settings = load_settings()
        try:
            d_obj = datetime.strptime(ymd, "%Y-%m-%d").date()
            if is_closed(d_obj, settings):
                line_reply(reply_token, [msg_text("這天是公休/不出貨日～請重新選擇。")])
                line_reply(reply_token, [msg_flex("取貨方式", flex_pickup_method())])
                return
        except:
            pass

        if sess["state"] == "WAIT_PICKUP_DATE":
            sess["pickup_date"] = ymd
            sess["state"] = "WAIT_PICKUP_TIME"
            q = [quick_postback(s, f"PB:TIME:{s}", display_text=s) for s in PICKUP_SLOTS]
            line_reply(reply_token, [msg_text(f"✅ 已選店取日期：{ymd}\n請選店取時段：", quick_items=q)])
            return

        if sess["state"] == "WAIT_DELIVERY_DATE":
            sess["delivery_date"] = ymd
            sess["state"] = "WAIT_DELIVERY_NAME"
            line_reply(reply_token, [msg_text(f"✅ 已選期望到貨日：{ymd}\n請輸入宅配收件人姓名：")])
            return

        line_reply(reply_token, [msg_text("我有收到日期，但目前不是選日期的步驟喔～\n請點「前往結帳」再操作一次。")])
        return

    # TIME
    if data.startswith("PB:TIME:") and sess["state"] == "WAIT_PICKUP_TIME":
        t = data.split("PB:TIME:", 1)[1].strip()
        sess["pickup_time"] = t
        sess["state"] = "WAIT_PICKUP_NAME"
        line_reply(reply_token, [msg_text(
            f"✅ 店取資訊已選好：\n日期：{sess.get('pickup_date')}\n時段：{t}\n地址：{PICKUP_ADDRESS}\n\n請輸入取件人姓名："
        )])
        return

    # PHONE CONFIRM
    if data.startswith("PB:PHONE_OK:"):
        kind = data.split("PB:PHONE_OK:", 1)[1].strip()
        if kind == "PICKUP":
            sess["pickup_phone_ok"] = True
            sess["state"] = "IDLE"
            line_reply(reply_token, [msg_text("✅ 電話已確認"), msg_flex("結帳內容", flex_checkout_summary(sess))])
            return
        if kind == "DELIVERY":
            sess["delivery_phone_ok"] = True
            sess["state"] = "IDLE"
            line_reply(reply_token, [msg_text("✅ 電話已確認"), msg_flex("結帳內容", flex_checkout_summary(sess))])
            return

    if data.startswith("PB:PHONE_RETRY:"):
        kind = data.split("PB:PHONE_RETRY:", 1)[1].strip()
        if kind == "PICKUP":
            sess["pickup_phone"] = None
            sess["pickup_phone_ok"] = False
            sess["state"] = "WAIT_PICKUP_PHONE"
            line_reply(reply_token, [msg_text("請重新輸入店取電話（純數字）：")])
            return
        if kind == "DELIVERY":
            sess["delivery_phone"] = None
            sess["delivery_phone_ok"] = False
            sess["state"] = "WAIT_DELIVERY_PHONE"
            line_reply(reply_token, [msg_text("請重新輸入宅配電話（純數字）：")])
            return

    # EDIT MENU
    if data == "PB:EDIT:MENU":
        if not sess["cart"]:
            line_reply(reply_token, [msg_text("購物車是空的～沒有東西可以改。")])
            return
        sess["state"] = "EDIT_MENU"
        q = [
            quick_postback("➕ 增加數量", "PB:EDITMODE:INC", display_text="增加數量"),
            quick_postback("➖ 減少數量", "PB:EDITMODE:DEC", display_text="減少數量"),
            quick_postback("🗑 移除品項", "PB:EDITMODE:DEL", display_text="移除品項"),
            quick_postback("🍵 修改口味", "PB:EDITMODE:FLAVOR", display_text="修改口味"),
        ]
        line_reply(reply_token, [msg_text("想怎麼修改呢？", quick_items=q)])
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
            line_reply(reply_token, [msg_text("修改指令好像怪怪的～請再試一次。")])
            return
        mode = parts[2].strip()
        idx = int(parts[3].strip())

        if idx < 0 or idx >= len(sess["cart"]):
            line_reply(reply_token, [msg_text("找不到該品項～請重新選。")])
            return

        x = sess["cart"][idx]
        item_key = x["item_key"]

        if mode == "INC":
            x["qty"] += ITEMS[item_key]["step"]
        elif mode == "DEC":
            new_qty = x["qty"] - ITEMS[item_key]["step"]
            if not can_dec_item(item_key, new_qty):
                line_reply(reply_token, [msg_text(f"此品項最低數量為 {ITEMS[item_key]['min_qty']}，不能再減囉～")])
                return
            x["qty"] = new_qty
        elif mode == "DEL":
            sess["cart"].pop(idx)
        elif mode == "FLAVOR":
            if not ITEMS[item_key]["has_flavor"]:
                line_reply(reply_token, [msg_text("這個品項沒有口味可以改～")])
                return
            sess["state"] = "WAIT_EDIT_FLAVOR"
            sess["pending_item"] = item_key
            sess["pending_flavor"] = idx
            q = [quick_postback(f, f"PB:SETFLAVOR:{f}", display_text=f) for f in ITEMS[item_key]["flavors"]]
            line_reply(reply_token, [msg_text("請選新口味：", quick_items=q)])
            return
        else:
            line_reply(reply_token, [msg_text("我不太懂你想怎麼改～再試一次？")])
            return

        recalc_cart(sess)
        sess["state"] = "IDLE"
        sess["edit_mode"] = None

        if not sess["cart"]:
            line_reply(reply_token, [msg_text("✅ 已更新～購物車目前是空的。"), msg_flex("甜點菜單", flex_product_menu(ordering=True))])
            return

        line_reply(reply_token, [msg_text("✅ 已更新結帳內容"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

    if data.startswith("PB:SETFLAVOR:") and sess.get("state") == "WAIT_EDIT_FLAVOR":
        new_flavor = data.split("PB:SETFLAVOR:", 1)[1].strip()
        idx = sess.get("pending_flavor")
        if idx is None or not isinstance(idx, int) or idx < 0 or idx >= len(sess["cart"]):
            line_reply(reply_token, [msg_text("口味更新失敗～請重新操作。")])
            return
        sess["cart"][idx]["flavor"] = new_flavor
        sess["state"] = "IDLE"
        sess["pending_item"] = None
        sess["pending_flavor"] = None
        recalc_cart(sess)
        line_reply(reply_token, [msg_text("✅ 口味已更新"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

    # NEXT
    if data == "PB:NEXT":
        if not sess["cart"]:
            line_reply(reply_token, [msg_text("購物車是空的～先選商品喔")])
            return

        if not sess.get("pickup_method"):
            sess["state"] = "WAIT_PICKUP_METHOD"
            line_reply(reply_token, [msg_flex("取貨方式", flex_pickup_method())])
            return

        if sess["pickup_method"] == "店取":
            if not sess.get("pickup_date"):
                sess["state"] = "WAIT_PICKUP_DATE"
                settings = load_settings()
                date_buttons = build_available_date_buttons(settings)
                q = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]
                line_reply(reply_token, [msg_text("請選店取日期：", quick_items=q)])
                return
            if not sess.get("pickup_time"):
                sess["state"] = "WAIT_PICKUP_TIME"
                q = [quick_postback(s, f"PB:TIME:{s}", display_text=s) for s in PICKUP_SLOTS]
                line_reply(reply_token, [msg_text("請選店取時段：", quick_items=q)])
                return
            if not sess.get("pickup_name"):
                sess["state"] = "WAIT_PICKUP_NAME"
                line_reply(reply_token, [msg_text("請輸入取件人姓名：")])
                return
            if not sess.get("pickup_phone"):
                sess["state"] = "WAIT_PICKUP_PHONE"
                line_reply(reply_token, [msg_text("請輸入店取電話（純數字）：")])
                return
            if not sess.get("pickup_phone_ok"):
                line_reply(reply_token, [msg_flex("電話確認", flex_phone_confirm(sess["pickup_phone"], "PICKUP"))])
                return

        if sess["pickup_method"] == "宅配":
            if not sess.get("delivery_date"):
                sess["state"] = "WAIT_DELIVERY_DATE"
                settings = load_settings()
                date_buttons = build_available_date_buttons(settings)
                q = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]
                line_reply(reply_token, [msg_text("請選期望到貨日：", quick_items=q)])
                return
            if not sess.get("delivery_name"):
                sess["state"] = "WAIT_DELIVERY_NAME"
                line_reply(reply_token, [msg_text("請輸入宅配收件人姓名：")])
                return
            if not sess.get("delivery_phone"):
                sess["state"] = "WAIT_DELIVERY_PHONE"
                line_reply(reply_token, [msg_text("請輸入宅配電話（純數字）：")])
                return
            if not sess.get("delivery_phone_ok"):
                line_reply(reply_token, [msg_flex("電話確認", flex_phone_confirm(sess["delivery_phone"], "DELIVERY"))])
                return
            if not sess.get("delivery_address"):
                sess["state"] = "WAIT_DELIVERY_ADDRESS"
                line_reply(reply_token, [msg_text("請輸入宅配地址（完整地址）：")])
                return

        # 建單
        order_id = gen_order_id()

        total = cart_total(sess["cart"])
        fee = shipping_fee(total) if sess["pickup_method"] == "宅配" else 0
        grand = total + fee
        summary_lines = "\n".join([f"• {find_cart_line_label(x)}" for x in sess["cart"]])

        # ✅ 先回覆客人（避免 replyToken 過期），寫表放後面
        if sess["pickup_method"] == "店取":
            customer_msg = (
                "✅ 訂單已建立（待轉帳）\n"
                f"訂單編號：{order_id}\n\n"
                f"{summary_lines}\n\n"
                "【店取資訊】\n"
                f"日期：{sess['pickup_date']}\n"
                f"時段：{sess['pickup_time']}\n"
                f"取件人：{sess['pickup_name']}\n"
                f"電話：{sess['pickup_phone']}\n"
                f"地址：{PICKUP_ADDRESS}\n\n"
                f"小計：NT${total}\n\n"
                + BANK_TRANSFER_TEXT
            )
        else:
            customer_msg = (
                "✅ 訂單已建立（待轉帳）\n"
                f"訂單編號：{order_id}\n\n"
                f"{summary_lines}\n\n"
                "【宅配資訊】\n"
                f"期望到貨日：{sess['delivery_date']}（不保證準時）\n"
                f"收件人：{sess['delivery_name']}\n"
                f"電話：{sess['delivery_phone']}\n"
                f"地址：{sess['delivery_address']}\n\n"
                f"小計：NT${total}\n運費：NT${fee}\n應付：NT${grand}\n\n"
                + DELIVERY_NOTICE
                + "\n\n"
                + BANK_TRANSFER_TEXT
            )

        line_reply(reply_token, [msg_text(customer_msg)])

        # 新訂單通知（只給管理員）
        if ADMIN_USER_IDS:
            method = sess["pickup_method"]
            admin_card = msg_flex("新訂單提醒", flex_admin_order_actions(order_id, method, current_status="UNPAID"))
            for admin_uid in ADMIN_USER_IDS:
                line_push(admin_uid, [admin_card])

        # ✅ 寫入 A/B/C + cashflow（寫入失敗只通知管理員，不噴給客人）
        okA = write_order_A(user_id, order_id, sess)
        okB = write_order_B(order_id, sess)
        okC = write_order_C_order(order_id, sess)  # 已雙寫 c_log + cashflow

        if not (okA and okB and okC) and ADMIN_USER_IDS:
            warn = f"⚠️ 提醒：訂單 {order_id} 表單寫入可能失敗（請檢查 Sheet 名稱/權限/欄位）。"
            for admin_uid in ADMIN_USER_IDS:
                line_push(admin_uid, [msg_text(warn)])

        reset_session(sess)
        return

    # fallback
    line_reply(reply_token, [msg_text("我有收到你的操作～但流程沒對上。\n要下單請點「我要下單」。")])


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
        line_reply(reply_token, [msg_text("請輸入店取電話（純數字）：")])
        return

    if sess["state"] == "WAIT_PICKUP_PHONE":
        if not is_phone_digits(text):
            line_reply(reply_token, [msg_text("電話格式看起來不對～請輸入純數字（例如 09xxxxxxxx）。")])
            return
        sess["pickup_phone"] = text.strip()
        sess["pickup_phone_ok"] = False
        sess["state"] = "IDLE"
        line_reply(reply_token, [
            msg_text("已收到店取電話～請二次確認："),
            msg_flex("電話確認", flex_phone_confirm(sess["pickup_phone"], "PICKUP"))
        ])
        return

    if sess["state"] == "WAIT_DELIVERY_NAME":
        sess["delivery_name"] = text.strip()
        sess["state"] = "WAIT_DELIVERY_PHONE"
        line_reply(reply_token, [msg_text("請輸入宅配電話（純數字）：")])
        return

    if sess["state"] == "WAIT_DELIVERY_PHONE":
        if not is_phone_digits(text):
            line_reply(reply_token, [msg_text("電話格式看起來不對～請輸入純數字（例如 09xxxxxxxx）。")])
            return
        sess["delivery_phone"] = text.strip()
        sess["delivery_phone_ok"] = False
        sess["state"] = "IDLE"
        line_reply(reply_token, [
            msg_text("已收到宅配電話～請二次確認："),
            msg_flex("電話確認", flex_phone_confirm(sess["delivery_phone"], "DELIVERY"))
        ])
        return

    if sess["state"] == "WAIT_DELIVERY_ADDRESS":
        sess["delivery_address"] = text.strip()
        sess["state"] = "IDLE"
        line_reply(reply_token, [msg_text("✅ 已收到宅配地址"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

    line_reply(reply_token, [msg_text("我有收到你的訊息～但目前建議用按鈕操作比較不會出錯。\n要看菜單請點「甜點」，要下單請點「我要下單」。")])
