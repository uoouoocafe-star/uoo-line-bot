import os
import json
import base64
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import parse_qs

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import (
    MessageEvent,
    TextMessageContent,
    PostbackEvent,
)

from linebot.v3.messaging import (
    ApiClient,
    Configuration,
    MessagingApi,
    ReplyMessageRequest,
    PushMessageRequest,
    TextMessage,
    FlexMessage,
)

from google.oauth2 import service_account
from googleapiclient.discovery import build


# =========================
# ENV
# =========================
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "").strip()
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "").strip()

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

# 你固定是 orders
SHEET_NAME = os.getenv("SHEET_NAME", "orders").strip()

TZ = timezone(timedelta(hours=8))  # GMT+8
PICKUP_ADDRESS = "新竹縣竹北市隘口六街65號"

DELIVERY_NOTICE = (
    "宅配：冷凍宅配（不保證準時到貨，日期僅為希望日）\n"
    "運費180元／滿2500免運\n"
)
PICKUP_NOTICE = f"店取地址：{PICKUP_ADDRESS}\n（所有甜點需提前3天預訂）"

BANK_TRANSFER_TEXT = (
    "付款方式：轉帳（對帳後依訂單號出貨/取貨）\n"
    "台灣銀行 004\n"
    "帳號：248-001-03430-6\n\n"
    "轉帳後請回傳：\n"
    "「已轉帳 訂單編號 末五碼12345」"
)


# =========================
# LINE
# =========================
app = FastAPI()

if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    print("[WARN] Missing LINE env vars (CHANNEL_ACCESS_TOKEN/CHANNEL_SECRET).")

handler = WebhookHandler(CHANNEL_SECRET) if CHANNEL_SECRET else None

line_config = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
api_client = ApiClient(line_config)
messaging_api = MessagingApi(api_client)


# =========================
# Session (in-memory)
# =========================
SESSIONS: Dict[str, Dict[str, Any]] = {}


def get_session(user_id: str) -> Dict[str, Any]:
    if user_id not in SESSIONS:
        SESSIONS[user_id] = {
            "ordering": False,   # ✅ 「我要下單」才會變 True
            "cart": [],
            "state": "IDLE",

            "pending_item": None,
            "pending_flavor": None,

            "pickup_method": None,  # 店取/宅配
            "pickup_date": None,
            "pickup_time": None,
            "pickup_name": None,

            "delivery_date": None,  # 希望到貨
            "delivery_name": None,
            "delivery_phone": None,
            "delivery_address": None,
        }
    return SESSIONS[user_id]


def reset_session(sess: dict):
    sess.update({
        "ordering": False,
        "cart": [],
        "state": "IDLE",
        "pending_item": None,
        "pending_flavor": None,

        "pickup_method": None,
        "pickup_date": None,
        "pickup_time": None,
        "pickup_name": None,

        "delivery_date": None,
        "delivery_name": None,
        "delivery_phone": None,
        "delivery_address": None,
    })


# =========================
# Menu
# =========================
FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

ITEMS = {
    "dacquoise": {"label": "達克瓦茲", "unit_price": 95, "has_flavor": True, "flavors": FLAVORS, "min_qty": 2, "max_qty": 12},
    "scone": {"label": "原味司康", "unit_price": 65, "has_flavor": False, "flavors": [], "min_qty": 1, "max_qty": 12},
    # ✅ 可麗露：六入/盒，只能一盒一盒買（qty=盒數）
    "canele_box": {"label": "可麗露六入/盒", "unit_price": 490, "has_flavor": False, "flavors": [], "min_qty": 1, "max_qty": 10},
    "toast": {"label": "伊思尼奶酥厚片", "unit_price": 85, "has_flavor": True, "flavors": FLAVORS, "min_qty": 1, "max_qty": 12},
}

STORE_TIME_SLOTS = ["10:00-12:00", "12:00-14:00", "14:00-16:00"]


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


def append_order_row(row: List[Any]) -> bool:
    if not GSHEET_ID:
        print("[WARN] GSHEET_ID missing, skip append.")
        return False

    service = get_sheets_service()
    if not service:
        print("[WARN] Google service account missing, skip append.")
        return False

    try:
        range_ = f"{SHEET_NAME}!A1"
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
        print("[ERROR] append_order_row failed:", e)
        return False


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


def parse_postback_data(data: str) -> Dict[str, str]:
    qs = parse_qs(data or "")
    return {k: (v[0] if v else "") for k, v in qs.items()}


def safe_reply(reply_token: str, messages: List[Any]):
    try:
        messaging_api.reply_message(
            ReplyMessageRequest(
                replyToken=reply_token,
                messages=messages,
            )
        )
    except Exception as e:
        print("[ERROR] reply_message failed:", e)


def safe_reply_text(reply_token: str, text: str):
    safe_reply(reply_token, [TextMessage(text=text)])


def safe_reply_flex(reply_token: str, alt_text: str, flex_content: dict, fallback_text: str = "系統忙碌中，請再按一次。"):
    alt = (alt_text or "").strip() or "訊息"
    if not isinstance(flex_content, dict) or not flex_content.get("type"):
        safe_reply_text(reply_token, fallback_text)
        return
    safe_reply(reply_token, [FlexMessage(altText=alt, contents=flex_content)])


def safe_push(to: str, messages: List[Any]):
    try:
        messaging_api.push_message(PushMessageRequest(to=to, messages=messages))
    except Exception as e:
        print("[ERROR] push_message failed:", e)


def format_mmdd_weekday(dt: datetime) -> str:
    wk = "一二三四五六日"[dt.weekday()]
    return f"{dt.month}/{dt.day}（{wk}）"


def build_date_options_10() -> List[Tuple[str, str]]:
    today0 = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    opts = []
    for i in range(3, 13):  # 10 個日期
        d = today0 + timedelta(days=i)
        opts.append((format_mmdd_weekday(d), d.strftime("%Y-%m-%d")))
    return opts


def calc_subtotal(item_key: str, qty: int) -> int:
    return int(ITEMS[item_key]["unit_price"]) * int(qty)


def find_cart_index(cart: List[dict], item_key: str, flavor: str) -> Optional[int]:
    for i, x in enumerate(cart):
        if x.get("item_key") == item_key and (x.get("flavor") or "") == (flavor or ""):
            return i
    return None


def cart_lines(cart: List[dict]) -> List[str]:
    lines = []
    for x in cart:
        name = x["label"]
        if x.get("flavor"):
            name += f"（{x['flavor']}）"
        # 可麗露盒：顯示「xN盒」
        if x["item_key"] == "canele_box":
            lines.append(f"• {name} x{x['qty']}盒 ＝ NT${x['subtotal']}")
        else:
            lines.append(f"• {name} x{x['qty']} ＝ NT${x['subtotal']}")
    return lines


# =========================
# Flex builders (全部用 Flex)
# =========================
def flex_button_postback(label: str, data: str, display_text: str, style: str = "primary", height: str = "md") -> dict:
    return {
        "type": "button",
        "height": height,
        "style": style,
        "action": {
            "type": "postback",
            "label": label,
            "data": data,
            "displayText": display_text,  # ✅ 不顯示程式碼，顯示人話
        },
    }


def build_menu_flex(ordering: bool) -> dict:
    note = "按「我要下單」後才可開始點選" if not ordering else "請直接點選商品開始加購"
    c = [
        {"type": "text", "text": "甜點菜單", "weight": "bold", "size": "xl"},
        {"type": "text", "text": f"（全部甜點需提前 3 天預訂）\n{note}", "size": "sm", "color": "#666666"},
        flex_button_postback("達克瓦茲｜NT$95", "act=item&key=dacquoise", "達克瓦茲", style="primary"),
        flex_button_postback("原味司康｜NT$65", "act=item&key=scone", "原味司康", style="primary"),
        flex_button_postback("可麗露六入/盒｜NT$490", "act=item&key=canele_box", "可麗露六入/盒", style="primary"),
        flex_button_postback("伊思尼奶酥厚片｜NT$85", "act=item&key=toast", "伊思尼奶酥厚片", style="primary"),
        {"type": "separator", "margin": "lg"},
        flex_button_postback("🧾 前往結帳", "act=checkout", "前往結帳", style="secondary"),
        flex_button_postback("🗑 清空重來", "act=reset", "清空重來", style="secondary"),
    ]
    return {"type": "bubble", "size": "mega", "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": c}}


def build_pickup_method_flex() -> dict:
    c = [
        {"type": "text", "text": "請選擇取貨方式", "weight": "bold", "size": "xl"},
        {"type": "text", "text": "日期將用按鈕選擇（不需手動輸入）", "size": "sm", "color": "#666666"},
        flex_button_postback("🏪 店取", "act=pickup&method=store", "店取", style="primary"),
        flex_button_postback("🚚 冷凍宅配", "act=pickup&method=ship", "冷凍宅配", style="primary"),
        {"type": "separator", "margin": "lg"},
        flex_button_postback("⬅️ 返回菜單", "act=show_menu", "返回菜單", style="secondary"),
    ]
    return {"type": "bubble", "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": c}}


def build_date_select_flex(title: str, act_name: str) -> dict:
    # act_name: store_date / ship_date
    opts = build_date_options_10()
    btns = []
    for label, value in opts:
        btns.append(
            flex_button_postback(
                label,
                f"act={act_name}&v={value}",
                f"{title}：{label}",
                style="secondary",
                height="sm",
            )
        )

    c = [
        {"type": "text", "text": title, "weight": "bold", "size": "xl"},
        {"type": "text", "text": "（3～14 天內，提供 10 個日期）", "size": "sm", "color": "#666666"},
        *btns,
        {"type": "separator", "margin": "lg"},
        flex_button_postback("⬅️ 返回取貨方式", "act=pickup_back", "返回取貨方式", style="secondary"),
    ]
    return {"type": "bubble", "size": "mega", "body": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": c}}


def build_time_select_flex() -> dict:
    btns = []
    for t in STORE_TIME_SLOTS:
        btns.append(flex_button_postback(t, f"act=store_time&v={t}", f"店取時段：{t}", style="secondary", height="sm"))
    c = [
        {"type": "text", "text": "店取時段", "weight": "bold", "size": "xl"},
        {"type": "text", "text": "請選擇時段", "size": "sm", "color": "#666666"},
        *btns,
        {"type": "separator", "margin": "lg"},
        flex_button_postback("⬅️ 重新選日期", "act=store_date_back", "重新選日期", style="secondary"),
    ]
    return {"type": "bubble", "body": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": c}}


def build_flavor_select_flex(item_key: str) -> dict:
    meta = ITEMS[item_key]
    btns = []
    for f in meta["flavors"]:
        btns.append(flex_button_postback(f, f"act=flavor&v={f}", f"口味：{f}", style="secondary", height="sm"))
    c = [
        {"type": "text", "text": f"{meta['label']} - 請選口味", "weight": "bold", "size": "xl"},
        *btns,
        {"type": "separator", "margin": "lg"},
        flex_button_postback("⬅️ 返回菜單", "act=show_menu", "返回菜單", style="secondary"),
    ]
    return {"type": "bubble", "size": "mega", "body": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": c}}


def build_qty_select_flex(item_key: str) -> dict:
    meta = ITEMS[item_key]
    min_q = meta["min_qty"]
    max_q = meta["max_qty"]
    btns = []
    for q in range(min_q, max_q + 1):
        # 可麗露：顯示「q盒」
        label = f"{q}盒" if item_key == "canele_box" else str(q)
        display = f"數量：{label}"
        btns.append(flex_button_postback(label, f"act=qty&v={q}", display, style="secondary", height="sm"))

    c = [
        {"type": "text", "text": f"{meta['label']} - 請選數量", "weight": "bold", "size": "xl"},
        {"type": "text", "text": f"最少 {min_q}，最多 {max_q}", "size": "sm", "color": "#666666"},
        *btns,
        {"type": "separator", "margin": "lg"},
        flex_button_postback("⬅️ 返回菜單", "act=show_menu", "返回菜單", style="secondary"),
    ]
    return {"type": "bubble", "size": "mega", "body": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": c}}


def build_cart_edit_flex(sess: dict) -> dict:
    cart = sess["cart"]
    if not cart:
        return {"type": "bubble", "body": {"type": "box", "layout": "vertical", "contents": [{"type": "text", "text": "購物車是空的", "weight": "bold", "size": "xl"}]}}

    content: List[dict] = [{"type": "text", "text": "結帳內容清單", "weight": "bold", "size": "xl"}]

    for idx, x in enumerate(cart):
        name = x["label"]
        if x.get("flavor"):
            name += f"（{x['flavor']}）"

        qty_label = f"{x['qty']}盒" if x["item_key"] == "canele_box" else str(x["qty"])
        content.append({"type": "text", "text": f"• {name}  x{qty_label} ＝ NT${x['subtotal']}", "size": "sm", "wrap": True})

        # ➖/➕
        row_btn = {
            "type": "box",
            "layout": "horizontal",
            "spacing": "sm",
            "contents": [
                flex_button_postback("➖ 減少數量", f"act=dec&idx={idx}", "減少數量", style="secondary", height="sm"),
                flex_button_postback("➕ 增加數量", f"act=inc&idx={idx}", "增加數量", style="secondary", height="sm"),
            ],
        }
        content.append(row_btn)
        content.append({"type": "separator", "margin": "md"})

    total = cart_total(cart)
    fee = shipping_fee(total) if sess.get("pickup_method") == "宅配" else 0
    grand = total + fee

    content.append({"type": "text", "text": f"目前小計：NT${total}", "weight": "bold", "size": "lg"})
    if sess.get("pickup_method") == "宅配":
        content.append({"type": "text", "text": f"運費：NT${fee}（滿2500免運）", "size": "sm", "color": "#666666"})
        content.append({"type": "text", "text": f"應付總額：NT${grand}", "weight": "bold", "size": "lg"})

    # 下一步按鈕
    content.append({"type": "separator", "margin": "lg"})
    content.append(flex_button_postback("🧾 繼續結帳", "act=checkout", "繼續結帳", style="primary"))
    content.append(flex_button_postback("＋ 繼續加購", "act=show_menu", "繼續加購", style="secondary"))
    content.append(flex_button_postback("🗑 清空重來", "act=reset", "清空重來", style="secondary"))

    return {"type": "bubble", "size": "mega", "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": content}}


def build_checkout_summary_flex(sess: dict) -> dict:
    cart = sess["cart"]
    lines = cart_lines(cart)
    total = cart_total(cart)

    method = sess.get("pickup_method") or ""
    date_str = ""
    time_str = ""

    if method == "店取":
        date_str = sess.get("pickup_date") or ""
        time_str = sess.get("pickup_time") or ""
    elif method == "宅配":
        date_str = sess.get("delivery_date") or ""

    fee = shipping_fee(total) if method == "宅配" else 0
    grand = total + fee

    content: List[dict] = [
        {"type": "text", "text": "✅ 結帳資訊", "weight": "bold", "size": "xl"},
        {"type": "text", "text": "結帳內容清單：", "weight": "bold", "size": "md"},
    ]

    if lines:
        for s in lines:
            content.append({"type": "text", "text": s, "size": "sm", "wrap": True})
    else:
        content.append({"type": "text", "text": "（購物車是空的）", "size": "sm", "color": "#666666"})

    content.append({"type": "separator", "margin": "md"})
    content.append({"type": "text", "text": f"目前小計：NT${total}", "weight": "bold", "size": "lg"})

    if method == "店取":
        content.append({"type": "text", "text": f"📅 日期：{date_str}", "size": "sm", "wrap": True})
        content.append({"type": "text", "text": f"🕒 時段：{time_str}", "size": "sm", "wrap": True})
        content.append({"type": "text", "text": f"📍 地址：{PICKUP_ADDRESS}", "size": "sm", "wrap": True})
        content.append({"type": "separator", "margin": "lg"})
        content.append(flex_button_postback("✍️ 填取件人姓名", "act=need_pickup_name", "填取件人姓名", style="primary"))
        content.append(flex_button_postback("🛠 修改品項/數量", "act=edit_cart", "修改品項/數量", style="secondary"))
        content.append(flex_button_postback("＋ 繼續加購", "act=show_menu", "繼續加購", style="secondary"))

    elif method == "宅配":
        content.append({"type": "text", "text": f"📅 希望到貨：{date_str}", "size": "sm", "wrap": True})
        content.append({"type": "text", "text": f"🚚 運費：NT${fee}（滿2500免運）", "size": "sm", "wrap": True})
        content.append({"type": "text", "text": f"💰 應付總額：NT${grand}", "weight": "bold", "size": "lg"})
        content.append({"type": "separator", "margin": "lg"})
        content.append(flex_button_postback("✍️ 填宅配資料", "act=need_ship_info", "填宅配資料", style="primary"))
        content.append(flex_button_postback("🛠 修改品項/數量", "act=edit_cart", "修改品項/數量", style="secondary"))
        content.append(flex_button_postback("＋ 繼續加購", "act=show_menu", "繼續加購", style="secondary"))

    else:
        content.append({"type": "text", "text": "尚未選擇取貨方式", "size": "sm", "color": "#666666"})
        content.append({"type": "separator", "margin": "lg"})
        content.append(flex_button_postback("選擇取貨方式", "act=checkout", "選擇取貨方式", style="primary"))

    return {"type": "bubble", "size": "mega", "body": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": content}}


# =========================
# Business logic
# =========================
def add_to_cart(user_id: str, item_key: str, flavor: str, qty: int):
    sess = get_session(user_id)
    meta = ITEMS[item_key]

    # enforce min/max
    if qty < meta["min_qty"]:
        qty = meta["min_qty"]
    if qty > meta["max_qty"]:
        qty = meta["max_qty"]

    label = meta["label"]
    unit = meta["unit_price"]

    # flavor required
    if meta["has_flavor"] and not flavor:
        raise ValueError("missing flavor")

    # merge same item+flavor
    idx = find_cart_index(sess["cart"], item_key, flavor or "")
    if idx is not None:
        new_qty = int(sess["cart"][idx]["qty"]) + int(qty)
        # max cap
        if new_qty > meta["max_qty"]:
            new_qty = meta["max_qty"]
        sess["cart"][idx]["qty"] = new_qty
        sess["cart"][idx]["subtotal"] = unit * new_qty
    else:
        sess["cart"].append({
            "item_key": item_key,
            "label": label,
            "flavor": flavor or "",
            "qty": qty,
            "unit_price": unit,
            "subtotal": unit * qty,
        })


def adjust_qty(sess: dict, idx: int, delta: int):
    cart = sess["cart"]
    if idx < 0 or idx >= len(cart):
        return
    item_key = cart[idx]["item_key"]
    meta = ITEMS.get(item_key)
    if not meta:
        return

    cur = int(cart[idx]["qty"])
    nxt = cur + int(delta)

    # min rule：達克瓦茲最少2；其他最少1；可麗露盒最少1
    if nxt < meta["min_qty"]:
        # 小於最少就直接刪掉
        cart.pop(idx)
        return

    if nxt > meta["max_qty"]:
        nxt = meta["max_qty"]

    cart[idx]["qty"] = nxt
    cart[idx]["subtotal"] = int(meta["unit_price"]) * nxt


def create_order_and_write_sheet(user_id: str, display_name: str) -> str:
    sess = get_session(user_id)
    cart = sess["cart"]
    if not cart:
        return ""

    order_id = gen_order_id()
    total = cart_total(cart)

    method = sess.get("pickup_method") or ""
    pickup_date = ""
    pickup_time = ""
    note = ""

    if method == "店取":
        pickup_date = sess.get("pickup_date") or ""
        pickup_time = sess.get("pickup_time") or ""
        note = f"取件人:{sess.get('pickup_name') or ''}"

    elif method == "宅配":
        pickup_date = sess.get("delivery_date") or ""
        note = (
            f"希望到貨:{sess.get('delivery_date') or ''} | "
            f"收件人:{sess.get('delivery_name') or ''} | "
            f"電話:{sess.get('delivery_phone') or ''} | "
            f"地址:{sess.get('delivery_address') or ''}"
        )

    row = [
        now_str(),          # created_at
        user_id,            # user_id
        display_name,       # display_name
        order_id,           # order_id
        json.dumps({"cart": cart}, ensure_ascii=False),  # items_json
        method,             # pickup_method
        pickup_date,        # pickup_date (宅配=希望到貨)
        pickup_time,        # pickup_time
        note,               # note
        total,              # amount
        "UNPAID",           # pay_status
        "",                 # transaction id
    ]

    ok = append_order_row(row)
    if not ok:
        print("[WARN] write sheet failed (but continue).")
    return order_id


# =========================
# Routes
# =========================
@app.get("/")
def root():
    return {"ok": True, "service": "uoo-line-bot"}


@app.post("/callback")
async def callback(request: Request):
    if not handler:
        raise HTTPException(status_code=500, detail="LINE handler not configured")

    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    body_text = body.decode("utf-8")

    try:
        handler.handle(body_text, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    return PlainTextResponse("OK")


# =========================
# Rich menu text triggers (MessageEvent)
# =========================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event: MessageEvent):
    user_id = event.source.user_id
    text = (event.message.text or "").strip()
    sess = get_session(user_id)

    print("[MSG]", user_id, "text=", text, "state=", sess.get("state"), "ordering=", sess.get("ordering"))

    # 1) Rich menu: 甜點
    if text == "甜點":
        # 只看菜單，不開啟 ordering
        safe_reply_flex(event.reply_token, "甜點菜單", build_menu_flex(ordering=sess["ordering"]))
        return

    # 2) Rich menu: 我要下單（開始 ordering）
    if text == "我要下單":
        sess["ordering"] = True
        sess["state"] = "IDLE"
        safe_reply_text(event.reply_token, "好的！請從菜單開始點選商品。")
        safe_push(user_id, [FlexMessage(altText="甜點菜單", contents=build_menu_flex(ordering=True))])
        return

    if text == "取貨說明":
        safe_reply_text(event.reply_token, PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE)
        return

    if text == "付款說明":
        safe_reply_text(event.reply_token, BANK_TRANSFER_TEXT)
        return

    if text in ["清空重來", "清空", "reset"]:
        reset_session(sess)
        safe_reply_text(event.reply_token, "已清空。按「我要下單」開始。")
        return

    # 付款回報
    if text.startswith("已轉帳"):
        safe_reply_text(event.reply_token, "收到，我們會核對帳款後依訂單號安排出貨/取貨。")
        return

    # === 以下為需要手動輸入的欄位（姓名/電話/地址）===
    if sess["state"] == "WAIT_PICKUP_NAME":
        sess["pickup_name"] = text
        # 建單
        order_id = create_order_and_write_sheet(user_id, "LINE用戶")
        summary = "\n".join(cart_lines(sess["cart"]))
        safe_reply_text(
            event.reply_token,
            "✅ 訂單已建立\n"
            f"訂單編號：{order_id}\n\n"
            f"{summary}\n\n"
            f"店取日期：{sess.get('pickup_date')}\n"
            f"店取時段：{sess.get('pickup_time')}\n"
            f"取件人：{sess.get('pickup_name')}\n"
            f"地址：{PICKUP_ADDRESS}\n\n"
            + BANK_TRANSFER_TEXT
        )
        # 完成後重置（保留 ordering= True 也可；這裡改回 False 比較乾淨）
        reset_session(sess)
        return

    if sess["state"] == "WAIT_DELIVERY_NAME":
        sess["delivery_name"] = text
        sess["state"] = "WAIT_DELIVERY_PHONE"
        safe_reply_text(event.reply_token, "請輸入宅配電話：")
        return

    if sess["state"] == "WAIT_DELIVERY_PHONE":
        sess["delivery_phone"] = text
        sess["state"] = "WAIT_DELIVERY_ADDRESS"
        safe_reply_text(event.reply_token, "請輸入宅配地址（完整地址）：")
        return

    if sess["state"] == "WAIT_DELIVERY_ADDRESS":
        sess["delivery_address"] = text
        # 建單
        order_id = create_order_and_write_sheet(user_id, "LINE用戶")
        total = cart_total(sess["cart"])
        fee = shipping_fee(total)
        grand = total + fee
        summary = "\n".join(cart_lines(sess["cart"]))
        safe_reply_text(
            event.reply_token,
            "✅ 訂單已建立\n"
            f"訂單編號：{order_id}\n\n"
            f"{summary}\n\n"
            f"希望到貨：{sess.get('delivery_date')}（不保證準時）\n"
            f"收件人：{sess.get('delivery_name')}\n"
            f"電話：{sess.get('delivery_phone')}\n"
            f"地址：{sess.get('delivery_address')}\n\n"
            f"小計：NT${total}\n運費：NT${fee}\n應付總額：NT${grand}\n\n"
            + DELIVERY_NOTICE
            + "\n"
            + BANK_TRANSFER_TEXT
        )
        reset_session(sess)
        return

    # fallback
    safe_reply_text(event.reply_token, "按下方選單操作：甜點 / 我要下單 / 取貨說明 / 付款說明")


# =========================
# Postback handlers (Flex buttons / Rich menu postback)
# =========================
@handler.add(PostbackEvent)
def handle_postback(event: PostbackEvent):
    user_id = event.source.user_id
    sess = get_session(user_id)
    data = parse_postback_data(event.postback.data)

    act = data.get("act", "")
    print("[POSTBACK]", user_id, "data=", data, "state=", sess.get("state"), "ordering=", sess.get("ordering"))

    # Rich menu 若用 postback：也支援
    if act == "rich_dessert":
        safe_reply_flex(event.reply_token, "甜點菜單", build_menu_flex(ordering=sess["ordering"]))
        return
    if act == "rich_order":
        sess["ordering"] = True
        safe_reply_text(event.reply_token, "好的！請從菜單開始點選商品。")
        safe_push(user_id, [FlexMessage(altText="甜點菜單", contents=build_menu_flex(ordering=True))])
        return
    if act == "rich_pickup":
        safe_reply_text(event.reply_token, PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE)
        return
    if act == "rich_pay":
        safe_reply_text(event.reply_token, BANK_TRANSFER_TEXT)
        return

    # common
    if act == "reset":
        reset_session(sess)
        safe_reply_text(event.reply_token, "已清空。按「我要下單」開始。")
        return

    if act == "show_menu":
        safe_reply_flex(event.reply_token, "甜點菜單", build_menu_flex(ordering=sess["ordering"]))
        return

    # 你希望「甜點只看、我要下單才可點」
    if act == "item":
        if not sess.get("ordering"):
            safe_reply_text(event.reply_token, "請先按「我要下單」開始點選商品。")
            return

        key = data.get("key", "")
        if key not in ITEMS:
            safe_reply_text(event.reply_token, "品項不存在，請回菜單重選。")
            return
        sess["pending_item"] = key
        sess["pending_flavor"] = None

        if ITEMS[key]["has_flavor"]:
            safe_reply_flex(event.reply_token, "選口味", build_flavor_select_flex(key))
            return
        else:
            safe_reply_flex(event.reply_token, "選數量", build_qty_select_flex(key))
            return

    if act == "flavor":
        v = data.get("v", "")
        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            safe_reply_text(event.reply_token, "流程已過期，請回菜單重選。")
            return
        if v not in ITEMS[item_key]["flavors"]:
            safe_reply_text(event.reply_token, "口味不正確，請重新選。")
            safe_reply_flex(event.reply_token, "選口味", build_flavor_select_flex(item_key))
            return
        sess["pending_flavor"] = v
        safe_reply_flex(event.reply_token, "選數量", build_qty_select_flex(item_key))
        return

    if act == "qty":
        v = data.get("v", "0")
        try:
            qty = int(v)
        except:
            qty = 0

        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            safe_reply_text(event.reply_token, "流程已過期，請回菜單重選。")
            return

        flavor = sess.get("pending_flavor") or ""
        try:
            add_to_cart(user_id, item_key, flavor, qty)
        except Exception as e:
            safe_reply_text(event.reply_token, f"加入失敗：{e}\n請回菜單重選。")
            return

        # 清 pending
        sess["pending_item"] = None
        sess["pending_flavor"] = None

        # 加入後：顯示購物車+結帳按鈕
        safe_reply_flex(event.reply_token, "結帳內容", build_cart_edit_flex(sess))
        return

    # 修改購物車
    if act == "inc":
        idx = int(data.get("idx", "-1"))
        adjust_qty(sess, idx, +1)
        safe_reply_flex(event.reply_token, "結帳內容", build_cart_edit_flex(sess))
        return

    if act == "dec":
        idx = int(data.get("idx", "-1"))
        adjust_qty(sess, idx, -1)
        safe_reply_flex(event.reply_token, "結帳內容", build_cart_edit_flex(sess))
        return

    if act == "edit_cart":
        safe_reply_flex(event.reply_token, "修改品項", build_cart_edit_flex(sess))
        return

    # checkout
    if act == "checkout":
        if not sess["cart"]:
            safe_reply_text(event.reply_token, "購物車是空的。請先按「我要下單」選商品。")
            return
        safe_reply_flex(event.reply_token, "取貨方式", build_pickup_method_flex())
        return

    if act == "pickup_back":
        safe_reply_flex(event.reply_token, "取貨方式", build_pickup_method_flex())
        return

    if act == "pickup":
        m = data.get("method", "")
        if m == "store":
            sess["pickup_method"] = "店取"
            safe_reply_flex(event.reply_token, "店取日期", build_date_select_flex("🌿 店取日期（3～14 天內）", "store_date"))
            return
        if m == "ship":
            sess["pickup_method"] = "宅配"
            safe_reply_flex(event.reply_token, "希望到貨日期", build_date_select_flex("🚚 希望到貨日期（3～14 天內）", "ship_date"))
            return

        safe_reply_text(event.reply_token, "取貨方式不正確，請重新選擇。")
        safe_reply_flex(event.reply_token, "取貨方式", build_pickup_method_flex())
        return

    if act == "store_date":
        v = data.get("v", "")
        sess["pickup_date"] = v
        safe_reply_flex(event.reply_token, "店取時段", build_time_select_flex())
        return

    if act == "store_date_back":
        safe_reply_flex(event.reply_token, "店取日期", build_date_select_flex("🌿 店取日期（3～14 天內）", "store_date"))
        return

    if act == "store_time":
        v = data.get("v", "")
        if v not in STORE_TIME_SLOTS:
            safe_reply_text(event.reply_token, "時段不正確，請重新選。")
            safe_reply_flex(event.reply_token, "店取時段", build_time_select_flex())
            return
        sess["pickup_time"] = v
        # 顯示結帳總覽（含清單＋小計＋下一步）
        safe_reply_flex(event.reply_token, "結帳資訊", build_checkout_summary_flex(sess))
        return

    if act == "ship_date":
        v = data.get("v", "")
        sess["delivery_date"] = v
        safe_reply_flex(event.reply_token, "結帳資訊", build_checkout_summary_flex(sess))
        return

    if act == "need_pickup_name":
        sess["state"] = "WAIT_PICKUP_NAME"
        safe_reply_text(event.reply_token, "請輸入店取取件人姓名：")
        return

    if act == "need_ship_info":
        sess["state"] = "WAIT_DELIVERY_NAME"
        safe_reply_text(event.reply_token, "請輸入宅配收件人姓名：")
        return

    # fallback
    safe_reply_text(event.reply_token, "此按鈕暫時無法處理，請回菜單重試。")
