import os
import json
import base64
import hmac
import hashlib
import random
import string
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Any, Optional, List, Tuple

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

SHEET_NAME = os.getenv("SHEET_NAME", "orders").strip()  # A表
SHEET_ITEMS_NAME = os.getenv("SHEET_ITEMS_NAME", "order_items_readable").strip()  # B表
SHEET_CASHFLOW_NAME = os.getenv("SHEET_CASHFLOW_NAME", "cashflow").strip()  # C表

TZ = timezone(timedelta(hours=8))  # Asia/Taipei
LINE_API_BASE = "https://api.line.me/v2/bot/message"

# 公休日（每週 + 指定日期）
# CLOSED_WEEKDAYS: 週一=1 ... 週日=7，可填多個逗號分隔，例如 "2,4"
CLOSED_WEEKDAYS_RAW = os.getenv("CLOSED_WEEKDAYS", "2").strip()
CLOSED_DATES_RAW = os.getenv("CLOSED_DATES", "").strip()  # 例如 "2026-01-20,2026-01-27"
ORDER_CUTOFF_HOURS = int((os.getenv("ORDER_CUTOFF_HOURS", "24").strip() or "24"))


def _parse_int_set(raw: str) -> set:
    out = set()
    for x in (raw or "").split(","):
        x = x.strip()
        if not x:
            continue
        try:
            out.add(int(x))
        except:
            pass
    return out


def _parse_date_set(raw: str) -> set:
    out = set()
    for x in (raw or "").split(","):
        x = x.strip()
        if not x:
            continue
        try:
            y, m, d = x.split("-")
            out.add(date(int(y), int(m), int(d)))
        except:
            pass
    return out


CLOSED_WEEKDAYS = _parse_int_set(CLOSED_WEEKDAYS_RAW)  # {2} = 週二
CLOSED_DATES = _parse_date_set(CLOSED_DATES_RAW)        # {date(2026,1,20), ...}


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

            "pickup_method": None,        # 店取 / 宅配
            "pickup_date": None,
            "pickup_time": None,
            "pickup_name": None,
            "pickup_phone": None,         # ✅ 店取也要電話

            "delivery_date": None,
            "delivery_name": None,
            "delivery_phone": None,
            "delivery_address": None,

            "phone_candidate": None,      # ✅ 電話二次確認用
            "phone_target": None,         # "PICKUP" or "DELIVERY"

            "edit_mode": None,
            "last_order_amount": None,    # 建單後暫存，供「已轉帳」記錄用
            "last_order_id": None,
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

PICKUP_ADDRESS = "新竹縣竹北市隘口六街65號"

BANK_TRANSFER_TEXT = (
    "付款方式：轉帳（對帳後依訂單號安排出貨/取貨）\n"
    "台灣銀行 004\n"
    "帳號：248-001-03430-6\n\n"
    "轉帳後請回傳：\n"
    "「已轉帳 訂單編號 末五碼12345」\n\n"
    f"提醒：訂單保留 {ORDER_CUTOFF_HOURS} 小時，逾時未完成轉帳可能會釋出名額。"
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
    f"{PICKUP_ADDRESS}\n\n"
    "提醒：所有甜點需提前3天預訂。"
)


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
    payload = {"replyToken": reply_token, "messages": messages}
    r = requests.post(
        f"{LINE_API_BASE}/reply",
        headers=line_headers(),
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        timeout=20,
    )
    if r.status_code >= 300:
        print("[ERROR] reply failed:", r.status_code, r.text)


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


def append_row(sheet_name: str, row: List[Any]) -> bool:
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
        print(f"[ERROR] append_row failed sheet={sheet_name}:", e)
        return False


# =========================
# Helpers
# =========================
def now_dt() -> datetime:
    return datetime.now(TZ)


def now_str() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")


def gen_order_id() -> str:
    d = now_dt().strftime("%Y%m%d")
    suffix = "".join(random.choices(string.digits, k=4))
    return f"UOO-{d}-{suffix}"


def cart_total(cart: List[dict]) -> int:
    return sum(int(x.get("subtotal", 0)) for x in cart)


def shipping_fee(total: int) -> int:
    return 0 if total >= 2500 else 180


def fmt_md_date(dt: datetime) -> str:
    wk = "一二三四五六日"[dt.weekday()]
    return f"{dt.month}/{dt.day}（{wk}）"


def ymd_to_date(ymd: str) -> Optional[date]:
    try:
        y, m, d = ymd.split("-")
        return date(int(y), int(m), int(d))
    except:
        return None


def is_closed(ymd: str) -> bool:
    d = ymd_to_date(ymd)
    if not d:
        return False
    if d in CLOSED_DATES:
        return True
    wk = d.weekday() + 1  # Mon=1 ... Sun=7
    return wk in CLOSED_WEEKDAYS


def build_date_buttons() -> List[Tuple[str, str]]:
    # 3~14 天內，排除公休/停出貨日
    today0 = now_dt().replace(hour=0, minute=0, second=0, microsecond=0)
    out = []
    for i in range(3, 15):
        d = today0 + timedelta(days=i)
        ymd = d.strftime("%Y-%m-%d")
        if is_closed(ymd):
            continue
        out.append((fmt_md_date(d), ymd))
    return out


def find_cart_line_label(x: dict) -> str:
    name = x["label"]
    if x.get("flavor"):
        name += f"（{x['flavor']}）"
    qty = x["qty"]
    unit = x["unit_price"]
    sub = x["subtotal"]
    return f"{name} ×{qty}（{unit}/單位）＝{sub}"


def find_cart_readable_short(x: dict) -> str:
    # 白話：品名｜數量｜口味(若有)
    name = x["label"]
    if x.get("flavor"):
        return f"{name}｜{x['qty']}｜{x['flavor']}"
    return f"{name}｜{x['qty']}"


def normalize_phone(s: str) -> str:
    out = []
    for ch in s.strip():
        if ch.isdigit() or ch == "+":
            out.append(ch)
    return "".join(out)


def looks_like_tw_phone(p: str) -> bool:
    p = normalize_phone(p)
    if p.startswith("+886"):
        p = "0" + p[4:]
    return (p.startswith("09") and len(p) == 10) or (p.startswith("0") and 9 <= len(p) <= 10)


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
                {"type": "text", "text": "請先點「我要下單」開始下單流程。\n想看品項可點「甜點」。", "wrap": True, "size": "sm", "color": "#666666"},
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
                {
                    "type": "button",
                    "style": "secondary",
                    "action": {"type": "postback", "label": "🧾 前往結帳", "data": "PB:CHECKOUT", "displayText": "前往結帳"},
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "action": {"type": "postback", "label": "🗑 清空重來", "data": "PB:RESET", "displayText": "清空重來"},
                },
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
                {"type": "text", "text": "（日期可直接按按鈕；公休日不開放）", "size": "sm", "color": "#666666"},
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
    date_ = sess.get("pickup_date") if method == "店取" else sess.get("delivery_date")
    date_ = date_ or "（未選）"
    time_ = sess.get("pickup_time") or ("—" if method != "店取" else "（未選）")

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
                {"type": "text", "text": f"日期：{date_}", "size": "sm", "color": "#666666"},
                {"type": "text", "text": f"時段：{time_}", "size": "sm", "color": "#666666"},
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


# =========================
# Cart ops
# =========================
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


def recalc_cart(sess: dict):
    for x in sess["cart"]:
        x["subtotal"] = int(x["unit_price"]) * int(x["qty"])


def can_dec_item(item_key: str, new_qty: int) -> bool:
    return new_qty >= ITEMS[item_key]["min_qty"]


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
# Order write (A/B)
# =========================
def create_order_and_write_sheets(user_id: str) -> str:
    sess = get_session(user_id)
    cart = sess["cart"]
    if not cart:
        return ""

    pickup_method = sess.get("pickup_method") or ""
    date_for_check = sess.get("pickup_date") if pickup_method == "店取" else (sess.get("delivery_date") or "")
    if date_for_check and is_closed(date_for_check):
        return ""

    order_id = gen_order_id()
    total = cart_total(cart)

    readable = "；".join([find_cart_readable_short(x) for x in cart])

    pickup_date = sess.get("pickup_date") or ""
    pickup_time = sess.get("pickup_time") or ""

    note = ""
    if pickup_method == "宅配":
        delivery_date = sess.get("delivery_date") or ""
        dn = sess.get("delivery_name") or ""
        dp = sess.get("delivery_phone") or ""
        da = sess.get("delivery_address") or ""
        note = f"{readable} | 希望到貨:{delivery_date} | 收件人:{dn} | 電話:{dp} | 地址:{da}"
        pickup_date = delivery_date
        pickup_time = ""
    else:
        pn = sess.get("pickup_name") or ""
        pp = sess.get("pickup_phone") or ""
        note = f"{readable} | 取件人:{pn} | 電話:{pp}"

    # A表（orders）
    append_row(SHEET_NAME, [
        now_str(), user_id, "", order_id,
        json.dumps({"cart": cart}, ensure_ascii=False),
        pickup_method, pickup_date, pickup_time,
        note, total, "UNPAID", ""
    ])

    # B表（order_items_readable）
    for x in cart:
        item_name = x["label"]
        if x.get("flavor"):
            item_name = f"{item_name}｜{x['flavor']}"
        append_row(SHEET_ITEMS_NAME, [
            now_str(), order_id, item_name, x["qty"], x["unit_price"], x["subtotal"],
            pickup_method, pickup_date, pickup_time
        ])

    sess["last_order_amount"] = total
    sess["last_order_id"] = order_id
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
    return {"ok": True, "service": "uoo-line-bot"}


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

    if etype == "message" and (ev.get("message") or {}).get("type") == "text":
        text = (ev["message"].get("text") or "").strip()

        if text in ["清空重來", "清空", "reset"]:
            reset_session(sess)
            line_reply(reply_token, [msg_text("已清空，重新開始。\n請點「我要下單」開始，或點「甜點」先看菜單。")])
            return

        # ✅ 甜點：只看菜單，不啟動下單
        if text == "甜點":
            line_reply(reply_token, [msg_flex("甜點菜單", flex_product_menu(ordering=sess["ordering"]))])
            return

        # ✅ 我要下單：才啟動 ordering
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

        if text == "付款說明":
            line_reply(reply_token, [msg_text(BANK_TRANSFER_TEXT)])
            return

        if text.startswith("已轉帳"):
            # C表記錄（簡版）
            oid = ""
            for token in text.replace("，", " ").replace(",", " ").split():
                if token.startswith("UOO-"):
                    oid = token
                    break
            amount = sess.get("last_order_amount") or ""
            append_row(SHEET_CASHFLOW_NAME, [now_str(), oid, amount, "轉帳", text])
            line_reply(reply_token, [msg_text("收到，我們會核對帳款後依訂單號安排出貨/取貨。\n若需補充資訊也可以直接留言。")])
            return

        handle_state_text(user_id, reply_token, text)
        return

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
    sess["phone_candidate"] = None
    sess["phone_target"] = None
    sess["edit_mode"] = None


# =========================
# Postback flows
# =========================
def handle_postback(user_id: str, reply_token: str, data: str):
    sess = get_session(user_id)

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
        try:
            add_to_cart(user_id, item_key, sess.get("pending_flavor"), qty)
        except Exception as e:
            line_reply(reply_token, [msg_text(f"加入失敗：{e}")])
            return
        sess["pending_item"] = None
        sess["pending_flavor"] = None
        sess["state"] = "IDLE"
        recalc_cart(sess)

        line_reply(reply_token, [msg_text("✅ 已加入購物車"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

    # PICKUP METHOD
    if data.startswith("PB:PICKUP:"):
        method = data.split("PB:PICKUP:", 1)[1].strip()
        sess["pickup_method"] = method

        date_buttons = build_date_buttons()
        if not date_buttons:
            line_reply(reply_token, [msg_text("近期 3～14 天內沒有可選日期（可能都遇到公休/停出貨）。\n請稍後再試或改天再下單。")])
            return
        quick_items = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]

        if method == "店取":
            sess["state"] = "WAIT_PICKUP_DATE"
            line_reply(reply_token, [msg_text("請選「店取日期」（3～14天內；公休日不開放）：", quick_items=quick_items)])
            return
        sess["state"] = "WAIT_DELIVERY_DATE"
        line_reply(reply_token, [msg_text("請選「希望到貨日期」（3～14天內；公休/停出貨日不開放；僅作希望日）：", quick_items=quick_items)])
        return

    # DATE
    if data.startswith("PB:DATE:"):
        ymd = data.split("PB:DATE:", 1)[1].strip()
        if is_closed(ymd):
            date_buttons = build_date_buttons()
            quick_items = [quick_postback(lbl, f"PB:DATE:{d2}", display_text=lbl) for (lbl, d2) in date_buttons]
            line_reply(reply_token, [msg_text("這天是公休/停出貨日，無法選擇。\n請改選其他日期：", quick_items=quick_items)])
            return

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
        line_reply(reply_token, [msg_text(f"✅ 店取資訊已選好：\n日期：{sess.get('pickup_date')}\n時段：{t}\n地址：{PICKUP_ADDRESS}\n\n請輸入取件人姓名：")])
        return

    # PHONE CONFIRM
    if data == "PB:PHONE:OK":
        cand = sess.get("phone_candidate") or ""
        if sess.get("phone_target") == "PICKUP":
            sess["pickup_phone"] = cand
        elif sess.get("phone_target") == "DELIVERY":
            sess["delivery_phone"] = cand
        sess["phone_candidate"] = None
        sess["phone_target"] = None
        sess["state"] = "IDLE"
        line_reply(reply_token, [msg_text("✅ 已確認電話"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

    if data == "PB:PHONE:RETRY":
        if sess.get("phone_target") == "PICKUP":
            sess["state"] = "WAIT_PICKUP_PHONE"
            line_reply(reply_token, [msg_text("好的，請重新輸入店取電話：")])
            return
        if sess.get("phone_target") == "DELIVERY":
            sess["state"] = "WAIT_DELIVERY_PHONE"
            line_reply(reply_token, [msg_text("好的，請重新輸入宅配電話：")])
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

        line_reply(reply_token, [msg_text("✅ 已更新結帳內容"), msg_flex("結帳內容", flex_checkout_summary(sess))])
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
                ds = build_date_buttons()
                qs = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in ds]
                line_reply(reply_token, [msg_text("請選店取日期：", quick_items=qs)])
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
                ds = build_date_buttons()
                qs = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in ds]
                line_reply(reply_token, [msg_text("請選希望到貨日期：", quick_items=qs)])
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

        dcheck = sess.get("pickup_date") if sess["pickup_method"] == "店取" else sess.get("delivery_date")
        if dcheck and is_closed(dcheck):
            ds = build_date_buttons()
            qs = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in ds]
            line_reply(reply_token, [msg_text("你選的日期是公休/停出貨日，無法出單。\n請改選其他日期：", quick_items=qs)])
            return

        order_id = create_order_and_write_sheets(user_id)
        if not order_id:
            line_reply(reply_token, [msg_text("目前無法建立訂單（可能選到公休/停出貨日）。\n請重新選日期或稍後再試。")])
            return

        total = cart_total(sess["cart"])
        fee = shipping_fee(total) if sess["pickup_method"] == "宅配" else 0
        grand = total + fee
        summary_lines = "\n".join([f"• {find_cart_line_label(x)}" for x in sess["cart"]])

        if sess["pickup_method"] == "店取":
            msg = (
                "✅ 訂單已建立\n"
                f"訂單編號：{order_id}\n\n"
                f"{summary_lines}\n\n"
                f"取貨方式：店取\n日期：{sess['pickup_date']}\n時段：{sess['pickup_time']}\n"
                f"取件人：{sess['pickup_name']}\n電話：{sess['pickup_phone']}\n"
                f"店取地址：{PICKUP_ADDRESS}\n\n"
                f"小計：NT${total}\n\n"
                + BANK_TRANSFER_TEXT
            )
        else:
            msg = (
                "✅ 訂單已建立\n"
                f"訂單編號：{order_id}\n\n"
                f"{summary_lines}\n\n"
                f"取貨方式：冷凍宅配\n希望到貨日期：{sess['delivery_date']}（不保證準時）\n"
                f"收件人：{sess['delivery_name']}\n電話：{sess['delivery_phone']}\n地址：{sess['delivery_address']}\n\n"
                f"小計：NT${total}\n運費：NT${fee}\n應付：NT${grand}\n\n"
                + DELIVERY_NOTICE + "\n\n" + BANK_TRANSFER_TEXT
            )

        # 清掉本單狀態
        sess["cart"] = []
        sess["state"] = "IDLE"
        sess["ordering"] = False
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
        sess["phone_candidate"] = None
        sess["phone_target"] = None
        sess["edit_mode"] = None

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

    # 店取姓名 -> 店取電話
    if sess["state"] == "WAIT_PICKUP_NAME":
        sess["pickup_name"] = text.strip()
        sess["state"] = "WAIT_PICKUP_PHONE"
        line_reply(reply_token, [msg_text("請輸入店取電話：")])
        return

    # 店取電話（二次確認）
    if sess["state"] == "WAIT_PICKUP_PHONE":
        p = normalize_phone(text)
        if not looks_like_tw_phone(p):
            line_reply(reply_token, [msg_text("電話格式看起來怪怪的，請再輸入一次（例如：0912345678）。")])
            return
        sess["phone_candidate"] = p
        sess["phone_target"] = "PICKUP"
        sess["state"] = "WAIT_PHONE_CONFIRM"
        q = [
            quick_postback("✅ 電話正確", "PB:PHONE:OK", display_text="電話正確"),
            quick_postback("✏️ 重新輸入", "PB:PHONE:RETRY", display_text="重新輸入"),
        ]
        line_reply(reply_token, [msg_text(f"請確認店取電話：{p}", quick_items=q)])
        return

    # 宅配姓名
    if sess["state"] == "WAIT_DELIVERY_NAME":
        sess["delivery_name"] = text.strip()
        sess["state"] = "WAIT_DELIVERY_PHONE"
        line_reply(reply_token, [msg_text("請輸入宅配電話：")])
        return

    # 宅配電話（二次確認）
    if sess["state"] == "WAIT_DELIVERY_PHONE":
        p = normalize_phone(text)
        if not looks_like_tw_phone(p):
            line_reply(reply_token, [msg_text("電話格式看起來怪怪的，請再輸入一次（例如：0912345678）。")])
            return
        sess["phone_candidate"] = p
        sess["phone_target"] = "DELIVERY"
        sess["state"] = "WAIT_PHONE_CONFIRM"
        q = [
            quick_postback("✅ 電話正確", "PB:PHONE:OK", display_text="電話正確"),
            quick_postback("✏️ 重新輸入", "PB:PHONE:RETRY", display_text="重新輸入"),
        ]
        line_reply(reply_token, [msg_text(f"請確認宅配電話：{p}", quick_items=q)])
        return

    # 宅配地址
    if sess["state"] == "WAIT_DELIVERY_ADDRESS":
        sess["delivery_address"] = text.strip()
        sess["state"] = "IDLE"
        line_reply(reply_token, [msg_text("✅ 已收到宅配資訊"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

    line_reply(reply_token, [msg_text("我有收到你的訊息，但目前建議用按鈕操作。\n若要開始下單請點「我要下單」，要看菜單請點「甜點」。")])
