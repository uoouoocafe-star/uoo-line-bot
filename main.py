import os
import json
import base64
import time
import hmac
import hashlib
import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from linebot.v3.webhook import WebhookParser
from linebot.v3.webhooks import MessageEvent, PostbackEvent, FollowEvent
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    PushMessageRequest,
    TextMessage,
    FlexMessage,
    FlexContainer,
)

import gspread
from google.oauth2.service_account import Credentials

# --------------------------
# Logging
# --------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("uoouoo_line_order")

app = FastAPI()

# --------------------------
# ENV
# --------------------------
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "").strip()
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "").strip()

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GSHEET_SHEET_NAME = os.getenv("GSHEET_SHEET_NAME", "orders").strip()  # A表
SHEET_ITEMS_NAME = os.getenv("SHEET_ITEMS_NAME", "order_items_readable").strip()  # B表
SHEET_CASHFLOW_NAME = os.getenv("SHEET_CASHFLOW_NAME", "cashflow").strip()  # C表
SHEET_SETTINGS_NAME = os.getenv("SHEET_SETTINGS_NAME", "settings").strip()

GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "").strip()

BANK_NAME = os.getenv("BANK_NAME", "").strip()
BANK_CORE = os.getenv("BANK_CORE", "").strip()
BANK_ACCOUNT = os.getenv("BANK_ACCOUNT", "").strip()
STORE_ADDRESS = os.getenv("STORE_ADDRESS", "").strip()

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()

# 關店與規則（支援 Render ENV 或 settings 工作表）
ENV_CLOSED_WEEKDAYS = os.getenv("CLOSED_WEEKDAYS", "").strip()  # 例如 "2" or "2,3"
ENV_CLOSED_DATES = os.getenv("CLOSED_DATES", "").strip()  # 例如 "2026-01-13,2026-01-14"
ENV_MIN_DAYS = os.getenv("MIN_DAYS", "3").strip()
ENV_MAX_DAYS = os.getenv("MAX_DAYS", "14").strip()
ENV_ORDER_CUTOFF_HOURS = os.getenv("ORDER_CUTOFF_HOURS", "").strip()  # optional

# --------------------------
# LINE SDK
# --------------------------
if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    logger.warning("Missing LINE channel token/secret in ENV.")

configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(CHANNEL_SECRET)

# --------------------------
# In-memory state (可改成 Redis/DB)
# --------------------------
USER_STATE: Dict[str, Dict[str, Any]] = {}
EVENT_DEDUP: Dict[str, float] = {}  # event_id -> timestamp (basic idempotency)

# --------------------------
# Menu / Items (你可再擴充)
# --------------------------
ITEMS: Dict[str, Dict[str, Any]] = {
    "dacquoise": {"label": "達克瓦茲", "unit_price": 95, "flavor_required": True},
    "scone": {"label": "原味司康", "unit_price": 65, "flavor_required": False},
    "canele6": {"label": "可麗露 6顆/盒", "unit_price": 490, "fixed_qty": 1, "flavor_required": False},
    "toast": {"label": "伊思尼奶酥厚片", "unit_price": 85, "flavor_required": False},
}

# 店取時段（可自行調整/未來可做滿額管控）
PICKUP_SLOTS = ["10:00-12:00", "12:00-14:00", "14:00-16:00"]

# --------------------------
# Google Sheets client
# --------------------------
def get_gspread_client() -> gspread.Client:
    if not GOOGLE_SERVICE_ACCOUNT_B64:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_B64")
    try:
        sa_json = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64).decode("utf-8")
        info = json.loads(sa_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        raise RuntimeError(f"Failed to init gspread: {e}")

def open_sheet():
    gc = get_gspread_client()
    sh = gc.open_by_key(GSHEET_ID)
    return sh

def ws_get(sh, title: str):
    return sh.worksheet(title)

def append_row_safe(ws, row: List[Any]):
    # 盡量避免 429/偶發錯誤造成漏寫
    last_err = None
    for _ in range(4):
        try:
            ws.append_row(row, value_input_option="RAW")
            return
        except Exception as e:
            last_err = e
            time.sleep(0.6)
    raise last_err

# --------------------------
# Settings (ENV + settings sheet override)
# --------------------------
def parse_int_list(s: str) -> List[int]:
    if not s:
        return []
    out = []
    for x in s.split(","):
        x = x.strip()
        if not x:
            continue
        out.append(int(x))
    return out

def parse_date_list(s: str) -> List[str]:
    if not s:
        return []
    out = []
    for x in s.split(","):
        x = x.strip()
        if not x:
            continue
        out.append(x)
    return out

def load_settings() -> Dict[str, Any]:
    settings = {
        "closed_weekdays": parse_int_list(ENV_CLOSED_WEEKDAYS),
        "closed_dates": set(parse_date_list(ENV_CLOSED_DATES)),
        "min_days": int(ENV_MIN_DAYS or "3"),
        "max_days": int(ENV_MAX_DAYS or "14"),
        "cutoff_hours": int(ENV_ORDER_CUTOFF_HOURS) if ENV_ORDER_CUTOFF_HOURS else None,
    }

    # 若 settings sheet 存在，優先用 sheet 的
    try:
        sh = open_sheet()
        ws = ws_get(sh, SHEET_SETTINGS_NAME)
        rows = ws.get_all_values()
        # Expect headers: key, value
        for r in rows[1:]:
            if len(r) < 2:
                continue
            k = (r[0] or "").strip()
            v = (r[1] or "").strip()
            if not k:
                continue
            if k == "closed_weekday":
                settings["closed_weekdays"] = [int(v)] if v else []
            elif k == "closed_weekdays":
                settings["closed_weekdays"] = parse_int_list(v)
            elif k == "closed_dates":
                settings["closed_dates"] = set(parse_date_list(v))
            elif k == "min_days":
                settings["min_days"] = int(v or "3")
            elif k == "max_days":
                settings["max_days"] = int(v or "14")
            elif k == "order_cutoff_hours":
                settings["cutoff_hours"] = int(v) if v else None
    except Exception as e:
        logger.info(f"settings sheet not loaded (use ENV). reason={e}")

    return settings

def is_closed(d: date, settings: Dict[str, Any]) -> bool:
    if d.strftime("%Y-%m-%d") in settings["closed_dates"]:
        return True
    # Python weekday: Mon=0 ... Sun=6
    # 你設定想用「週二=2」這個習慣：通常是 Mon=1...Sun=7
    # 所以這裡同時兼容兩種：若用 2 表週二，轉為 python=1
    for wd in settings["closed_weekdays"]:
        if wd in [1,2,3,4,5,6,7]:
            py = wd - 1
        else:
            py = wd
        if d.weekday() == py:
            return True
    return False

def valid_date_range(settings: Dict[str, Any]) -> Tuple[date, date]:
    today = datetime.now().date()
    start = today + timedelta(days=settings["min_days"])
    end = today + timedelta(days=settings["max_days"])
    return start, end

def build_available_dates(settings: Dict[str, Any], days_limit: int = 30) -> List[date]:
    start, end = valid_date_range(settings)
    out = []
    cur = start
    while cur <= end and len(out) < days_limit:
        if not is_closed(cur, settings):
            out.append(cur)
        cur += timedelta(days=1)
    return out

# --------------------------
# Helpers: user state
# --------------------------
def get_state(user_id: str) -> Dict[str, Any]:
    if user_id not in USER_STATE:
        USER_STATE[user_id] = {
            "step": "idle",
            "cart": [],  # list of {item_key,label,flavor,qty,unit_price,subtotal}
            "pickup_method": None,  # 店取/宅配
            "pickup_date": None,
            "pickup_time": None,
            "expected_delivery_date": None,  # 宅配期望到貨日
            "name": None,
            "phone": None,
            "phone_confirmed": False,
            "address": None,
            "note": "",
            "last_order_id": None,
        }
    return USER_STATE[user_id]

def reset_order_state(st: Dict[str, Any]):
    st["step"] = "idle"
    st["cart"] = []
    st["pickup_method"] = None
    st["pickup_date"] = None
    st["pickup_time"] = None
    st["expected_delivery_date"] = None
    st["name"] = None
    st["phone"] = None
    st["phone_confirmed"] = False
    st["address"] = None
    st["note"] = ""
    st["last_order_id"] = None

# --------------------------
# Cart operations
# --------------------------
def cart_total(cart: List[Dict[str, Any]]) -> int:
    return sum(int(x.get("subtotal", 0)) for x in cart)

def upsert_cart_item(cart: List[Dict[str, Any]], item_key: str, qty_delta: int, flavor: str = ""):
    meta = ITEMS[item_key]
    label = meta["label"]
    unit_price = int(meta["unit_price"])
    fixed_qty = meta.get("fixed_qty")
    # canele6 只能 1 盒 1 盒買：qty 固定=1，但可以多筆加到 cart
    if fixed_qty:
        # 固定商品，每次 + 就新增一行（更直覺：一盒一行）
        qty = fixed_qty
        cart.append({
            "item_key": item_key,
            "label": label,
            "flavor": flavor or "",
            "qty": qty,
            "unit_price": unit_price,
            "subtotal": qty * unit_price
        })
        return

    # 一般商品：同 item_key + flavor 合併
    key_match = (item_key, flavor or "")
    for it in cart:
        if (it.get("item_key"), it.get("flavor","")) == key_match:
            it["qty"] = max(0, int(it["qty"]) + qty_delta)
            it["unit_price"] = unit_price
            it["subtotal"] = int(it["qty"]) * unit_price
            break
    else:
        if qty_delta > 0:
            cart.append({
                "item_key": item_key,
                "label": label,
                "flavor": flavor or "",
                "qty": qty_delta,
                "unit_price": unit_price,
                "subtotal": qty_delta * unit_price
            })
    # remove zero qty
    cart[:] = [x for x in cart if int(x.get("qty", 0)) > 0]

def cart_to_readable_lines(cart: List[Dict[str, Any]]) -> List[str]:
    lines = []
    for it in cart:
        label = it.get("label","")
        qty = it.get("qty",0)
        flavor = (it.get("flavor","") or "").strip()
        if flavor:
            lines.append(f"{label}｜{qty}｜{flavor}")
        else:
            lines.append(f"{label}｜{qty}")
    return lines

def cart_to_compact_text(cart: List[Dict[str, Any]]) -> str:
    # 給 A表 transaction_note / 訂單確認用
    parts = []
    for it in cart:
        label = it.get("label","")
        qty = it.get("qty",0)
        flavor = (it.get("flavor","") or "").strip()
        if flavor:
            parts.append(f"{label}｜{qty}｜{flavor}")
        else:
            parts.append(f"{label}｜{qty}")
    return "；".join(parts)

# --------------------------
# Order id
# --------------------------
def gen_order_id() -> str:
    now = datetime.now()
    return f"UOO-{now.strftime('%Y%m%d')}-{str(int(time.time()*1000))[-4:]}"

# --------------------------
# Flex builders
# --------------------------
def flex_menu_only() -> FlexMessage:
    # 「甜點」按鈕：只顯示菜單，不進入下單
    rows = []
    for k, meta in ITEMS.items():
        price = meta["unit_price"]
        rows.append({
            "type": "box",
            "layout": "baseline",
            "contents": [
                {"type": "text", "text": meta["label"], "flex": 6, "size": "md"},
                {"type": "text", "text": f"NT${price}", "flex": 3, "size": "md", "align": "end"},
            ]
        })

    bubble = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "今日甜點菜單", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "全部甜點需提前 3 天預訂", "size": "sm", "color": "#666666"},
                {"type": "separator"},
                *rows,
                {"type": "separator"},
                {"type": "text", "text": "要下單請點下方「我要下單」", "size": "sm", "color": "#666666"}
            ]
        }
    }
    return FlexMessage(alt_text="甜點菜單", contents=FlexContainer.from_json(json.dumps(bubble)))

def flex_start_order() -> FlexMessage:
    bubble = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "開始下單", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "請選擇取貨方式", "size": "sm", "color": "#666666"},
            ]
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {
                    "type": "button",
                    "style": "primary",
                    "action": {"type": "postback", "label": "店取", "data": "ACT:METHOD:店取"}
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "action": {"type": "postback", "label": "宅配", "data": "ACT:METHOD:宅配"}
                },
                {
                    "type": "button",
                    "style": "link",
                    "action": {"type": "postback", "label": "取消", "data": "ACT:CANCEL"}
                }
            ]
        }
    }
    return FlexMessage(alt_text="開始下單", contents=FlexContainer.from_json(json.dumps(bubble)))

def flex_phone_confirm(phone: str) -> FlexMessage:
    bubble = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "電話二次確認", "weight": "bold", "size": "xl"},
                {"type": "text", "text": f"你填的電話是：{phone}", "size": "md"},
                {"type": "text", "text": "請確認正確，避免通知不到你。", "size": "sm", "color": "#666666"},
            ]
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "button", "style": "primary",
                 "action": {"type": "postback", "label": "✅ 正確", "data": "ACT:PHONE_OK"}},
                {"type": "button", "style": "secondary",
                 "action": {"type": "postback", "label": "✏️ 重新輸入", "data": "ACT:PHONE_RETRY"}},
            ]
        }
    }
    return FlexMessage(alt_text="電話確認", contents=FlexContainer.from_json(json.dumps(bubble)))

def flex_pick_date(settings: Dict[str, Any], title: str, action_prefix: str) -> FlexMessage:
    # action_prefix: PICKDATE / DELIVDATE
    ds = build_available_dates(settings)
    buttons = []
    for d in ds[:10]:
        buttons.append({
            "type": "button",
            "style": "secondary",
            "action": {"type": "postback", "label": d.strftime("%m/%d (%a)"), "data": f"ACT:{action_prefix}:{d.strftime('%Y-%m-%d')}"}
        })
    bubble = {
        "type": "bubble",
        "body": {"type":"box","layout":"vertical","spacing":"md",
                 "contents":[
                     {"type":"text","text":title,"weight":"bold","size":"xl"},
                     {"type":"text","text":"公休日與不出貨日不會出現可選日期。","size":"sm","color":"#666666"},
                 ]},
        "footer": {"type":"box","layout":"vertical","spacing":"sm","contents": buttons + [
            {"type":"button","style":"link","action":{"type":"postback","label":"取消","data":"ACT:CANCEL"}}
        ]}
    }
    return FlexMessage(alt_text=title, contents=FlexContainer.from_json(json.dumps(bubble)))

def flex_pick_time() -> FlexMessage:
    buttons = []
    for s in PICKUP_SLOTS:
        buttons.append({
            "type":"button","style":"secondary",
            "action":{"type":"postback","label":s,"data":f"ACT:PICKTIME:{s}"}
        })
    bubble = {
        "type":"bubble",
        "body":{"type":"box","layout":"vertical","spacing":"md",
                "contents":[
                    {"type":"text","text":"選擇店取時段","weight":"bold","size":"xl"},
                    {"type":"text","text":"若某時段已滿，我會直接提示你改選其他時段。","size":"sm","color":"#666666"},
                ]},
        "footer":{"type":"box","layout":"vertical","spacing":"sm","contents": buttons + [
            {"type":"button","style":"link","action":{"type":"postback","label":"取消","data":"ACT:CANCEL"}}
        ]}
    }
    return FlexMessage(alt_text="店取時段", contents=FlexContainer.from_json(json.dumps(bubble)))

def flex_cart(st: Dict[str, Any]) -> FlexMessage:
    cart = st["cart"]
    lines = []
    for idx, it in enumerate(cart):
        label = it["label"]
        qty = it["qty"]
        flavor = (it.get("flavor","") or "").strip()
        sub = it["subtotal"]
        title = f"{label} × {qty}"
        if flavor:
            title += f"（{flavor}）"
        lines.append({
            "type":"box","layout":"vertical","spacing":"xs",
            "contents":[
                {"type":"text","text":title,"size":"md","wrap":True},
                {"type":"text","text":f"小計 NT${sub}","size":"sm","color":"#666666"},
                {
                    "type":"box","layout":"horizontal","spacing":"sm","contents":[
                        {"type":"button","height":"sm","style":"secondary",
                         "action":{"type":"postback","label":"➖ 減少數量","data":f"ACT:CART:DEC:{idx}"}},
                        {"type":"button","height":"sm","style":"secondary",
                         "action":{"type":"postback","label":"➕ 增加數量","data":f"ACT:CART:INC:{idx}"}},
                        {"type":"button","height":"sm","style":"link",
                         "action":{"type":"postback","label":"修改口味","data":f"ACT:CART:EDIT:{idx}"}}
                    ]
                },
                {"type":"separator"}
            ]
        })

    total = cart_total(cart)
    bubble = {
        "type":"bubble",
        "body":{"type":"box","layout":"vertical","spacing":"md",
                "contents":[
                    {"type":"text","text":"購物車","weight":"bold","size":"xl"},
                    {"type":"text","text":"你可以直接在這裡增減數量或修改。","size":"sm","color":"#666666"},
                    {"type":"separator"},
                    *lines if lines else [{"type":"text","text":"目前購物車是空的。","size":"md"}],
                    {"type":"text","text":f"合計：NT${total}","weight":"bold","size":"lg"},
                ]},
        "footer":{"type":"box","layout":"vertical","spacing":"sm",
                  "contents":[
                      {"type":"button","style":"primary","action":{"type":"postback","label":"前往結帳","data":"ACT:CHECKOUT"}},
                      {"type":"button","style":"secondary","action":{"type":"postback","label":"繼續加購","data":"ACT:ADD_MORE"}},
                      {"type":"button","style":"secondary","action":{"type":"postback","label":"清空重來","data":"ACT:CLEAR"}},
                      {"type":"button","style":"link","action":{"type":"postback","label":"取消","data":"ACT:CANCEL"}},
                  ]}
    }
    return FlexMessage(alt_text="購物車", contents=FlexContainer.from_json(json.dumps(bubble)))

def flex_item_picker() -> FlexMessage:
    # 下單流程中用的品項選擇（含前往結帳/清空）
    buttons = []
    for k, meta in ITEMS.items():
        buttons.append({
            "type":"button","style":"secondary",
            "action":{"type":"postback","label":f"{meta['label']}｜NT${meta['unit_price']}", "data":f"ACT:ITEM:{k}"}
        })
    bubble = {
        "type":"bubble",
        "body":{"type":"box","layout":"vertical","spacing":"md",
                "contents":[
                    {"type":"text","text":"請選擇商品","weight":"bold","size":"xl"},
                    {"type":"text","text":"點商品會加入購物車；可在購物車增減數量。", "size":"sm","color":"#666666"},
                ]},
        "footer":{"type":"box","layout":"vertical","spacing":"sm",
                  "contents": buttons + [
                      {"type":"separator"},
                      {"type":"button","style":"primary","action":{"type":"postback","label":"前往結帳","data":"ACT:SHOW_CART"}},
                      {"type":"button","style":"secondary","action":{"type":"postback","label":"清空重來","data":"ACT:CLEAR"}},
                      {"type":"button","style":"link","action":{"type":"postback","label":"取消","data":"ACT:CANCEL"}},
                  ]}
    }
    return FlexMessage(alt_text="選擇商品", contents=FlexContainer.from_json(json.dumps(bubble)))

def flex_checkout_confirm(st: Dict[str, Any]) -> FlexMessage:
    cart = st["cart"]
    total = cart_total(cart)
    lines = cart_to_readable_lines(cart)
    method = st["pickup_method"]
    name = st["name"] or ""
    phone = st["phone"] or ""
    note = st.get("note","") or ""

    if method == "店取":
        date_s = st["pickup_date"]
        time_s = st["pickup_time"]
        ship_line = f"店取：{date_s} {time_s}"
    else:
        date_s = st["expected_delivery_date"]
        addr = st["address"] or ""
        ship_line = f"宅配：期望到貨日 {date_s}\n地址：{addr}"

    body_text = "\n".join([f"• {x}" for x in lines]) if lines else "（購物車空）"
    bubble = {
        "type":"bubble",
        "body":{"type":"box","layout":"vertical","spacing":"md",
                "contents":[
                    {"type":"text","text":"請確認訂單內容", "weight":"bold","size":"xl"},
                    {"type":"text","text":"以下資訊確認後才會送出訂單。", "size":"sm","color":"#666666"},
                    {"type":"separator"},
                    {"type":"text","text":"【品項清單】", "weight":"bold","size":"md"},
                    {"type":"text","text":body_text, "wrap":True, "size":"md"},
                    {"type":"separator"},
                    {"type":"text","text":"【取貨方式】", "weight":"bold","size":"md"},
                    {"type":"text","text":ship_line, "wrap":True, "size":"md"},
                    {"type":"separator"},
                    {"type":"text","text":"【聯絡資訊】", "weight":"bold","size":"md"},
                    {"type":"text","text":f"取件人：{name}\n電話：{phone}", "wrap":True, "size":"md"},
                    {"type":"separator"},
                    {"type":"text","text":f"合計：NT${total}", "weight":"bold","size":"lg"},
                ]},
        "footer":{"type":"box","layout":"vertical","spacing":"sm",
                  "contents":[
                      {"type":"button","style":"primary","action":{"type":"postback","label":"✅ 確認送出訂單","data":"ACT:SUBMIT"}},
                      {"type":"button","style":"secondary","action":{"type":"postback","label":"✏️ 返回修改（購物車）","data":"ACT:SHOW_CART"}},
                      {"type":"button","style":"secondary","action":{"type":"postback","label":"☎️ 重新輸入電話","data":"ACT:PHONE_RETRY"}},
                      {"type":"button","style":"link","action":{"type":"postback","label":"取消","data":"ACT:CANCEL"}},
                  ]}
    }
    return FlexMessage(alt_text="訂單確認", contents=FlexContainer.from_json(json.dumps(bubble)))

def flex_payment_info(order_id: str, amount: int) -> FlexMessage:
    text = (
        f"訂單編號：{order_id}\n"
        f"應付金額：NT${amount}\n\n"
        f"請轉帳至：\n"
        f"{BANK_NAME}（{BANK_CORE}）\n"
        f"{BANK_ACCOUNT}\n\n"
        f"轉帳後請回傳末五碼或截圖，我們核對後會更新付款狀態。"
    )
    bubble = {
        "type":"bubble",
        "body":{"type":"box","layout":"vertical","spacing":"md",
                "contents":[
                    {"type":"text","text":"付款資訊", "weight":"bold","size":"xl"},
                    {"type":"text","text":text, "wrap":True, "size":"md"},
                ]}
    }
    return FlexMessage(alt_text="付款資訊", contents=FlexContainer.from_json(json.dumps(bubble)))

def flex_admin_notify_buttons(order_id: str, method: str) -> FlexMessage:
    # 店取：已做好通知；宅配：已出貨通知
    if method == "店取":
        btn_label = "📣 已做好，通知客人取貨"
        data = f"ADMIN:READY:{order_id}"
        hint = "按下後會推播「已可取貨」給客人，並寫入 C 表 status=READY"
    else:
        btn_label = "🚚 已出貨，通知客人"
        data = f"ADMIN:SHIPPED:{order_id}"
        hint = "按下後會推播「已出貨」給客人，並寫入 C 表 status=SHIPPED"
    bubble = {
        "type":"bubble",
        "body":{"type":"box","layout":"vertical","spacing":"md",
                "contents":[
                    {"type":"text","text":"商家通知按鈕", "weight":"bold","size":"xl"},
                    {"type":"text","text":hint, "size":"sm","color":"#666666","wrap":True},
                    {"type":"text","text":f"訂單：{order_id}", "size":"md"},
                ]},
        "footer":{"type":"box","layout":"vertical","spacing":"sm",
                  "contents":[
                      {"type":"button","style":"primary",
                       "action":{"type":"postback","label":btn_label,"data":data}},
                  ]}
    }
    return FlexMessage(alt_text="通知按鈕", contents=FlexContainer.from_json(json.dumps(bubble)))

# --------------------------
# Validation helpers
# --------------------------
def is_valid_phone(s: str) -> bool:
    s = s.strip()
    if not s.isdigit():
        return False
    if len(s) < 8 or len(s) > 10:
        return False
    return True

def require_fields_or_ask(st: Dict[str, Any]) -> Optional[str]:
    if not st["name"]:
        st["step"] = "ask_name"
        return "請輸入取件人姓名（店取/宅配都需要）"
    if not st["phone"]:
        st["step"] = "ask_phone"
        return "請輸入聯絡電話（店取/宅配都需要）"
    if not st["phone_confirmed"]:
        st["step"] = "phone_confirm"
        return None
    if st["pickup_method"] == "店取":
        if not st["pickup_date"]:
            st["step"] = "pick_date"
            return None
        if not st["pickup_time"]:
            st["step"] = "pick_time"
            return None
    else:
        if not st["expected_delivery_date"]:
            st["step"] = "deliv_date"
            return None
        if not st["address"]:
            st["step"] = "ask_address"
            return "請輸入宅配地址（含縣市/區/路名/號/樓層）"
    return None

# --------------------------
# Sheet writing (A/B/C)
# --------------------------
def ensure_headers():
    # 不強制改你表格，只提醒欄位順序要一致
    pass

def write_order_all_tables(user_id: str, display_name: str, order_id: str, st: Dict[str, Any]):
    sh = open_sheet()
    wsA = ws_get(sh, GSHEET_SHEET_NAME)          # orders
    wsB = ws_get(sh, SHEET_ITEMS_NAME)           # order_items_readable
    wsC = ws_get(sh, SHEET_CASHFLOW_NAME)        # cashflow

    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    method = st["pickup_method"]
    pay_status = "UNPAID"

    # A表：固定欄位順序（請讓你的 A表 header 也是這個順序）
    # created_at, user_id, display_name, order_id, items_json, pickup_method, pickup_date, pickup_time, note, amount, pay_status, transaction_note
    items_json = json.dumps({"cart": st["cart"]}, ensure_ascii=False)
    if method == "店取":
        pickup_date = st["pickup_date"]
        pickup_time = st["pickup_time"]
        note = f"取件人:{st['name']}｜電話:{st['phone']}"
    else:
        pickup_date = st["expected_delivery_date"]  # 期望到貨日放在 pickup_date 欄位（你原本就這樣用）
        pickup_time = ""                            # 宅配不需要時段
        note = f"收件人:{st['name']}｜電話:{st['phone']}｜地址:{st['address']}"

    amount = cart_total(st["cart"])
    transaction_note = cart_to_compact_text(st["cart"])

    rowA = [
        created_at, user_id, display_name, order_id,
        items_json, method, pickup_date, pickup_time,
        note, amount, pay_status, transaction_note
    ]
    append_row_safe(wsA, rowA)

    # B表：每個品項一列（讓你白話好看）
    # created_at, order_id, item-name, qty, unit_price, subtotal, pickup_method, pickup_date, pickup_time, pay_status, phone
    for it in st["cart"]:
        item_name = it["label"]
        if (it.get("flavor") or "").strip():
            item_name = f"{item_name}｜{it['flavor']}"
        rowB = [
            created_at, order_id, item_name, it["qty"], it["unit_price"], it["subtotal"],
            method, pickup_date, pickup_time, pay_status, st["phone"]
        ]
        append_row_safe(wsB, rowB)

    # C表：金流/狀態（你要的通知按鈕更新這張）
    # created_at, order_id, flow_type, method, amount, shipping_fee, grand_total, status, note
    shipping_fee = 0
    grand_total = amount + shipping_fee
    status = "ORDER"
    rowC = [
        created_at, order_id, "ORDER", method,
        amount, shipping_fee, grand_total,
        status, note
    ]
    append_row_safe(wsC, rowC)

def write_cashflow_status(order_id: str, new_status: str, note: str):
    sh = open_sheet()
    wsC = ws_get(sh, SHEET_CASHFLOW_NAME)
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 以 append 方式記錄狀態變更（不改舊列，最安全）
    # created_at, order_id, flow_type, method, amount, shipping_fee, grand_total, status, note
    row = [created_at, order_id, "STATUS", "", "", "", "", new_status, note]
    append_row_safe(wsC, row)

# --------------------------
# Dedup / Guard
# --------------------------
def dedup_event(event_id: str) -> bool:
    now = time.time()
    # clean
    for k, ts in list(EVENT_DEDUP.items()):
        if now - ts > 60:
            EVENT_DEDUP.pop(k, None)
    if event_id in EVENT_DEDUP:
        return True
    EVENT_DEDUP[event_id] = now
    return False

# --------------------------
# Main handlers
# --------------------------
async def reply(token: str, messages: List[Any]):
    with ApiClient(configuration) as api_client:
        line_api = MessagingApi(api_client)
        line_api.reply_message(ReplyMessageRequest(
            reply_token=token,
            messages=messages
        ))

async def push(user_id: str, messages: List[Any]):
    with ApiClient(configuration) as api_client:
        line_api = MessagingApi(api_client)
        line_api.push_message(PushMessageRequest(
            to=user_id,
            messages=messages
        ))

@app.post("/callback")
async def callback(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")
    try:
        events = parser.parse(body.decode("utf-8"), signature)
    except Exception as e:
        logger.error(f"parse error: {e}")
        raise HTTPException(status_code=400, detail="Invalid signature")

    for event in events:
        # Dedup by webhookEventId if exists
        event_id = getattr(event, "webhook_event_id", None)
        if event_id and dedup_event(event_id):
            continue

        if isinstance(event, FollowEvent):
            await handle_follow(event)
        elif isinstance(event, MessageEvent):
            await handle_message(event)
        elif isinstance(event, PostbackEvent):
            await handle_postback(event)

    return JSONResponse({"ok": True})

async def handle_follow(event: FollowEvent):
    user_id = event.source.user_id
    st = get_state(user_id)
    reset_order_state(st)
    await reply(event.reply_token, [
        TextMessage(text="歡迎加入 UooUoo cafe！\n\n點下方 Rich Menu：\n「甜點」看菜單\n「我要下單」開始訂購")
    ])

async def handle_message(event: MessageEvent):
    user_id = event.source.user_id
    st = get_state(user_id)
    text = (event.message.text or "").strip()

    # 管理員指令（可選）
    if text.startswith("/admin ") and ADMIN_TOKEN:
        parts = text.split()
        if len(parts) >= 2 and parts[1] == ADMIN_TOKEN:
            await reply(event.reply_token, [TextMessage(text="管理員模式已驗證。")])
        else:
            await reply(event.reply_token, [TextMessage(text="管理員驗證失敗。")])
        return

    # 流程輸入
    if st["step"] == "ask_name":
        st["name"] = text
        st["step"] = "ask_phone"
        await reply(event.reply_token, [TextMessage(text="請輸入聯絡電話（店取/宅配都需要）")])
        return

    if st["step"] == "ask_phone":
        if not is_valid_phone(text):
            await reply(event.reply_token, [TextMessage(text="電話格式看起來不對，請輸入純數字（例如 09xxxxxxxx）。")])
            return
        st["phone"] = text
        st["step"] = "phone_confirm"
        await reply(event.reply_token, [flex_phone_confirm(text)])
        return

    if st["step"] == "ask_address":
        if len(text) < 6:
            await reply(event.reply_token, [TextMessage(text="地址太短，請輸入完整地址（含縣市/區/路名/號/樓層）。")])
            return
        st["address"] = text
        # 下一步：商品選擇
        st["step"] = "pick_items"
        await reply(event.reply_token, [flex_item_picker()])
        return

    # 其它文字：當作一般訊息回覆
    await reply(event.reply_token, [TextMessage(text="你可以點下方選單：\n「甜點」看菜單\n「我要下單」開始訂購")])

async def handle_postback(event: PostbackEvent):
    user_id = event.source.user_id
    st = get_state(user_id)
    data = (event.postback.data or "").strip()

    # 商家通知按鈕（從你的客服端按）
    if data.startswith("ADMIN:"):
        # 這裡用最保守：只要能點到就執行（若你要加 ADMIN_TOKEN 驗證也可）
        _, action, order_id = data.split(":", 2)
        # 找訂單對應 user（這版先用「最後下單者」簡化；你要百分百精準，可在 A表多存一欄 user_id 查回）
        # 建議：在 A表已經有 user_id，可用 sheet 反查 order_id -> user_id
        target_user = find_user_id_by_order_id(order_id)
        if not target_user:
            await reply(event.reply_token, [TextMessage(text=f"找不到此訂單的客人：{order_id}")])
            return

        if action == "READY":
            await push(target_user, [TextMessage(text=f"你的訂單已完成，可以來取貨了。\n訂單編號：{order_id}")])
            write_cashflow_status(order_id, "READY", "店取已做好通知")
            await reply(event.reply_token, [TextMessage(text="已通知客人（READY）。")])
            return

        if action == "SHIPPED":
            await push(target_user, [TextMessage(text=f"你的訂單已出貨。\n訂單編號：{order_id}")])
            write_cashflow_status(order_id, "SHIPPED", "宅配已出貨通知")
            await reply(event.reply_token, [TextMessage(text="已通知客人（SHIPPED）。")])
            return

        await reply(event.reply_token, [TextMessage(text="未知的管理員動作。")])
        return

    # 一般 postback
    if data == "ACT:CANCEL":
        reset_order_state(st)
        await reply(event.reply_token, [TextMessage(text="已取消本次操作。")])
        return

    if data == "ACT:MENU":
        await reply(event.reply_token, [flex_menu_only()])
        return

    if data == "ACT:START":
        reset_order_state(st)
        st["step"] = "choose_method"
        await reply(event.reply_token, [flex_start_order()])
        return

    if data.startswith("ACT:METHOD:"):
        method = data.split(":", 2)[2]
        st["pickup_method"] = method
        # 先收基本資料（姓名/電話）
        st["step"] = "ask_name"
        await reply(event.reply_token, [TextMessage(text=f"你選擇：{method}\n\n請先輸入取件人/收件人姓名")])
        return

    if data == "ACT:PHONE_OK":
        st["phone_confirmed"] = True
        # 下一步：依方法選日期
        settings = load_settings()
        if st["pickup_method"] == "店取":
            st["step"] = "pick_date"
            await reply(event.reply_token, [flex_pick_date(settings, "選擇店取日期", "PICKDATE")])
        else:
            st["step"] = "deliv_date"
            await reply(event.reply_token, [flex_pick_date(settings, "選擇期望到貨日", "DELIVDATE")])
        return

    if data == "ACT:PHONE_RETRY":
        st["phone"] = None
        st["phone_confirmed"] = False
        st["step"] = "ask_phone"
        await reply(event.reply_token, [TextMessage(text="請重新輸入聯絡電話（純數字）")])
        return

    if data.startswith("ACT:PICKDATE:"):
        d = data.split(":", 2)[2]
        settings = load_settings()
        d_obj = datetime.strptime(d, "%Y-%m-%d").date()
        # 再驗一次（避免 client cache）
        if is_closed(d_obj, settings):
            await reply(event.reply_token, [TextMessage(text="這天是公休日/不出貨日，請重新選日期。")])
            await reply(event.reply_token, [flex_pick_date(settings, "選擇店取日期", "PICKDATE")])
            return
        st["pickup_date"] = d
        st["step"] = "pick_time"
        await reply(event.reply_token, [flex_pick_time()])
        return

    if data.startswith("ACT:DELIVDATE:"):
        d = data.split(":", 2)[2]
        settings = load_settings()
        d_obj = datetime.strptime(d, "%Y-%m-%d").date()
        if is_closed(d_obj, settings):
            await reply(event.reply_token, [TextMessage(text="這天是公休日/不出貨日，請重新選期望到貨日。")])
            await reply(event.reply_token, [flex_pick_date(settings, "選擇期望到貨日", "DELIVDATE")])
            return
        st["expected_delivery_date"] = d
        st["step"] = "ask_address"
        await reply(event.reply_token, [TextMessage(text="請輸入宅配地址（含縣市/區/路名/號/樓層）")])
        return

    if data.startswith("ACT:PICKTIME:"):
        slot = data.split(":", 2)[2]
        # 這裡可加「時段滿額」檢查：先略過，明天可接
        st["pickup_time"] = slot
        st["step"] = "pick_items"
        await reply(event.reply_token, [flex_item_picker()])
        return

    if data == "ACT:ADD_MORE":
        st["step"] = "pick_items"
        await reply(event.reply_token, [flex_item_picker()])
        return

    if data == "ACT:SHOW_CART":
        st["step"] = "cart"
        await reply(event.reply_token, [flex_cart(st)])
        return

    if data == "ACT:CLEAR":
        st["cart"] = []
        st["step"] = "pick_items"
        await reply(event.reply_token, [TextMessage(text="已清空購物車。"), flex_item_picker()])
        return

    if data.startswith("ACT:ITEM:"):
        item_key = data.split(":", 2)[2]
        if item_key not in ITEMS:
            await reply(event.reply_token, [TextMessage(text="此商品不存在。")])
            return

        meta = ITEMS[item_key]
        # 達克瓦茲需要口味：先詢問口味（用文字回覆）
        if meta.get("flavor_required"):
            st["step"] = f"ask_flavor::{item_key}"
            await reply(event.reply_token, [TextMessage(text=f"你選擇：{meta['label']}\n請輸入口味（例如：日式焙茶/原味/巧克力）")])
            return

        # 固定盒裝商品：直接 +1
        upsert_cart_item(st["cart"], item_key, 1, "")
        await reply(event.reply_token, [TextMessage(text=f"已加入：{meta['label']}\n目前合計 NT${cart_total(st['cart'])}"), flex_item_picker()])
        return

    # 口味輸入狀態
    if st["step"].startswith("ask_flavor::"):
        item_key = st["step"].split("::", 1)[1]
        flavor = (event.postback.data or "")  # 這裡通常不會進，因為口味用 message
        # 但保留結構，避免跑掉
        return

    if data.startswith("ACT:CART:"):
        # CART:DEC/INC/EDIT
        _, _, act, idx_s = data.split(":", 3)
        idx = int(idx_s)
        if idx < 0 or idx >= len(st["cart"]):
            await reply(event.reply_token, [TextMessage(text="購物車項目不存在，請重新開啟購物車。")])
            return

        it = st["cart"][idx]
        item_key = it["item_key"]
        flavor = it.get("flavor","")

        if act == "DEC":
            # 固定盒裝（可麗露）用「刪除該行」
            if ITEMS[item_key].get("fixed_qty"):
                st["cart"].pop(idx)
            else:
                upsert_cart_item(st["cart"], item_key, -1, flavor)
            await reply(event.reply_token, [flex_cart(st)])
            return

        if act == "INC":
            if ITEMS[item_key].get("fixed_qty"):
                # 盒裝多一盒：新增一行
                upsert_cart_item(st["cart"], item_key, 1, "")
            else:
                upsert_cart_item(st["cart"], item_key, +1, flavor)
            await reply(event.reply_token, [flex_cart(st)])
            return

        if act == "EDIT":
            # 只允許達克瓦茲修改口味
            if not ITEMS[item_key].get("flavor_required"):
                await reply(event.reply_token, [TextMessage(text="此品項不需要口味，不用修改。"), flex_cart(st)])
                return
            st["step"] = f"edit_flavor::{idx}"
            await reply(event.reply_token, [TextMessage(text="請輸入新的口味（例如：日式焙茶/原味/巧克力）")])
            return

    if data == "ACT:CHECKOUT":
        # 先確保購物車不空
        if not st["cart"]:
            await reply(event.reply_token, [TextMessage(text="購物車是空的，請先選商品。"), flex_item_picker()])
            return
        # 確保前置資料完整
        missing_text = require_fields_or_ask(st)
        if missing_text:
            await reply(event.reply_token, [TextMessage(text=missing_text)])
            return
        if st["step"] == "phone_confirm":
            await reply(event.reply_token, [flex_phone_confirm(st["phone"])])
            return
        if st["step"] == "pick_date":
            await reply(event.reply_token, [flex_pick_date(load_settings(), "選擇店取日期", "PICKDATE")])
            return
        if st["step"] == "pick_time":
            await reply(event.reply_token, [flex_pick_time()])
            return
        if st["step"] == "deliv_date":
            await reply(event.reply_token, [flex_pick_date(load_settings(), "選擇期望到貨日", "DELIVDATE")])
            return

        st["step"] = "confirm"
        await reply(event.reply_token, [flex_checkout_confirm(st)])
        return

    if data == "ACT:SUBMIT":
        if not st["cart"]:
            await reply(event.reply_token, [TextMessage(text="購物車是空的，無法送出。")])
            return

        # 再驗一次日期合法（避免公休日被選到）
        settings = load_settings()
        if st["pickup_method"] == "店取":
            if not st["pickup_date"]:
                await reply(event.reply_token, [flex_pick_date(settings, "選擇店取日期", "PICKDATE")])
                return
            d_obj = datetime.strptime(st["pickup_date"], "%Y-%m-%d").date()
            if is_closed(d_obj, settings):
                st["pickup_date"] = None
                await reply(event.reply_token, [TextMessage(text="你選的店取日是公休日/不出貨日，請重新選日期。"),
                                               flex_pick_date(settings, "選擇店取日期", "PICKDATE")])
                return
        else:
            if not st["expected_delivery_date"]:
                await reply(event.reply_token, [flex_pick_date(settings, "選擇期望到貨日", "DELIVDATE")])
                return
            d_obj = datetime.strptime(st["expected_delivery_date"], "%Y-%m-%d").date()
            if is_closed(d_obj, settings):
                st["expected_delivery_date"] = None
                await reply(event.reply_token, [TextMessage(text="你選的期望到貨日是公休日/不出貨日，請重新選日期。"),
                                               flex_pick_date(settings, "選擇期望到貨日", "DELIVDATE")])
                return

        order_id = gen_order_id()
        st["last_order_id"] = order_id

        # display_name 可能取不到，先留空
        display_name = ""
        amount = cart_total(st["cart"])

        try:
            write_order_all_tables(user_id, display_name, order_id, st)
        except Exception as e:
            logger.error(f"write sheets failed: {e}")
            await reply(event.reply_token, [TextMessage(text="系統寫入訂單時發生錯誤，請再試一次或直接私訊我們。")])
            return

        # 給客人：送出成功 + 付款資訊 + 商家通知按鈕提示
        await reply(event.reply_token, [
            TextMessage(text=f"✅ 訂單已送出成功！\n訂單編號：{order_id}\n合計：NT${amount}\n\n接下來請依付款資訊完成轉帳。"),
            flex_payment_info(order_id, amount),
        ])

        # 同時推播「商家通知按鈕」給你自己（如果你要推播到某個管理員 user_id，可在ENV加 ADMIN_USER_ID）
        # 這裡先回傳在同聊天室（客人也會看到）；若你不想客人看到，明天我改成推播到管理員ID
        await push(user_id, [flex_admin_notify_buttons(order_id, st["pickup_method"])])

        reset_order_state(st)
        return

    # fallback
    await reply(event.reply_token, [TextMessage(text="我沒有理解你的操作，請再點一次下方按鈕。")])

# --------------------------
# Extra: flavor handling + edit flavor via Message
# --------------------------
@app.post("/callback_text_patch")
async def callback_text_patch(request: Request):
    # 這個路由不用，保留避免你誤貼。
    return JSONResponse({"ok": True})

# MessageEvent flavor/edit flavor intercept
#（FastAPI + linebot v3 不易在同 handler 做兩段解析，所以用 handle_message 已涵蓋 ask_name/ask_phone/ask_address。
#  口味與修改口味會在 handle_message 內用 step 判斷。）

# Override handle_message to include flavor and edit_flavor
old_handle_message = handle_message

async def handle_message(event: MessageEvent):
    user_id = event.source.user_id
    st = get_state(user_id)
    text = (event.message.text or "").strip()

    # flavor input
    if st["step"].startswith("ask_flavor::"):
        item_key = st["step"].split("::", 1)[1]
        flavor = text
        upsert_cart_item(st["cart"], item_key, 1, flavor)
        st["step"] = "pick_items"
        await reply(event.reply_token, [
            TextMessage(text=f"已加入：{ITEMS[item_key]['label']}（{flavor}）\n目前合計 NT${cart_total(st['cart'])}"),
            flex_item_picker()
        ])
        return

    if st["step"].startswith("edit_flavor::"):
        idx = int(st["step"].split("::", 1)[1])
        if idx < 0 or idx >= len(st["cart"]):
            st["step"] = "cart"
            await reply(event.reply_token, [TextMessage(text="購物車項目不存在，請重新開啟購物車。"), flex_cart(st)])
            return
        it = st["cart"][idx]
        if not ITEMS[it["item_key"]].get("flavor_required"):
            st["step"] = "cart"
            await reply(event.reply_token, [TextMessage(text="此品項不需要口味。"), flex_cart(st)])
            return
        it["flavor"] = text
        st["step"] = "cart"
        await reply(event.reply_token, [TextMessage(text="已更新口味。"), flex_cart(st)])
        return

    # default to previous handler
    await old_handle_message(event)

# patch the function reference
globals()["handle_message"] = handle_message

# --------------------------
# Find user_id by order_id (for admin notify)
# --------------------------
def find_user_id_by_order_id(order_id: str) -> Optional[str]:
    try:
        sh = open_sheet()
        ws = ws_get(sh, GSHEET_SHEET_NAME)
        rows = ws.get_all_values()
        # find order_id in column D (index 3)
        for r in rows[1:]:
            if len(r) >= 4 and (r[3] or "").strip() == order_id:
                return (r[1] or "").strip()  # user_id column B
    except Exception as e:
        logger.error(f"find_user_id_by_order_id failed: {e}")
    return None
