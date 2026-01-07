import os
import json
import base64
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent, PostbackEvent

from linebot.v3.messaging import (
    ApiClient,
    Configuration,
    MessagingApi,
    ReplyMessageRequest,
    PushMessageRequest,
    TextMessage,
    FlexMessage,
    QuickReply,
    QuickReplyItem,
    PostbackAction,
    DatetimePickerAction,
)

from linebot.v3.messaging.models import FlexContainer

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

SHEET_NAME = os.getenv("SHEET_NAME", "sheet1").strip()

TZ = timezone(timedelta(hours=8))  # Asia/Taipei


# =========================
# App / LINE clients
# =========================
app = FastAPI()

if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    print("[WARN] Missing LINE env (CHANNEL_ACCESS_TOKEN/CHANNEL_SECRET). Bot will not reply.")

handler = WebhookHandler(CHANNEL_SECRET) if CHANNEL_SECRET else None

line_config = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
api_client = ApiClient(line_config)
messaging_api = MessagingApi(api_client)


# =========================
# In-memory session store
# =========================
SESSIONS: Dict[str, Dict[str, Any]] = {}


def get_session(user_id: str) -> Dict[str, Any]:
    if user_id not in SESSIONS:
        SESSIONS[user_id] = {
            "cart": [],
            "state": "IDLE",
            "pending_item": None,
            "pending_flavor": None,
            "pickup_method": None,   # 店取 / 宅配
            "pickup_date": None,
            "pickup_time": None,
            "pickup_name": None,     # 店取取件人姓名
            "delivery_date": None,   # 希望到貨日期
            "delivery_name": None,
            "delivery_phone": None,
            "delivery_address": None,
            "note": "",
        }
    return SESSIONS[user_id]


def reset_session(user_id: str):
    SESSIONS.pop(user_id, None)


# =========================
# Menu data
# =========================
DACQUOISE_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]
TOAST_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

ITEMS = {
    "dacquoise": {"label": "達克瓦茲", "unit_price": 95, "has_flavor": True, "flavors": DACQUOISE_FLAVORS, "min_qty": 2},
    "scone": {"label": "原味司康", "unit_price": 65, "has_flavor": False, "flavors": [], "min_qty": 1},
    "canele": {"label": "原味可麗露", "unit_price": 90, "has_flavor": False, "flavors": [], "min_qty": 1},
    "toast": {"label": "伊思尼奶酥厚片", "unit_price": 85, "has_flavor": True, "flavors": TOAST_FLAVORS, "min_qty": 1},
}

PICKUP_ADDRESS = "新竹縣竹北市隘口六街65號"

BANK_TRANSFER_TEXT = (
    "付款方式：轉帳（對帳後依訂單號出貨）\n"
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
    f"{PICKUP_ADDRESS}\n\n"
    "提醒：所有甜點需提前3天預訂。"
)


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
        print("[WARN] Google Sheet env missing, skip append.")
        return False

    try:
        range_ = f"'{SHEET_NAME}'!A1"
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


def pretty_date_tw(yyyy_mm_dd: str) -> str:
    # 2026-01-10 -> 1/10（六）
    try:
        dt = datetime.strptime(yyyy_mm_dd, "%Y-%m-%d")
        week = "一二三四五六日"[dt.weekday()]
        return f"{dt.month}/{dt.day}（{week}）"
    except Exception:
        return yyyy_mm_dd


# =========================
# LINE send helpers
# =========================
def reply_messages(reply_token: str, messages):
    if not isinstance(messages, list):
        messages = [messages]
    if not messages:
        messages = [TextMessage(text="⚠️ 系統忙碌中，請再試一次")]

    messaging_api.reply_message(
        ReplyMessageRequest(
            replyToken=reply_token,
            messages=messages,
        )
    )


def reply_text(reply_token: str, text: str):
    reply_messages(reply_token, TextMessage(text=text))


def push_text(user_id: str, text: str):
    messaging_api.push_message(
        PushMessageRequest(
            to=user_id,
            messages=[TextMessage(text=text)],
        )
    )


def flex_container_from_dict(flex_dict: dict) -> Optional[FlexContainer]:
    try:
        if not isinstance(flex_dict, dict) or "type" not in flex_dict:
            return None
        return FlexContainer.from_dict(flex_dict)
    except Exception as e:
        print("[ERROR] FlexContainer.from_dict failed:", repr(e))
        return None


def reply_flex_dict(reply_token: str, alt_text: str, flex_dict: dict):
    container = flex_container_from_dict(flex_dict)
    if not container:
        reply_text(reply_token, "⚠️ 卡片內容異常，請輸入「甜點」或「我要下單」重試。")
        return
    reply_messages(reply_token, FlexMessage(alt_text=alt_text, contents=container))


def push_flex_dict(user_id: str, alt_text: str, flex_dict: dict):
    container = flex_container_from_dict(flex_dict)
    if not container:
        push_text(user_id, "⚠️ 卡片內容異常，請輸入「甜點」或「我要下單」重試。")
        return
    messaging_api.push_message(
        PushMessageRequest(
            to=user_id,
            messages=[FlexMessage(alt_text=alt_text, contents=container)],
        )
    )


def push_quick_reply_postback(user_id: str, text: str, items: List[dict]):
    # items: [{"label": "...", "data": "..."}]
    qr_items = [QuickReplyItem(action=PostbackAction(label=it["label"], data=it["data"])) for it in items]
    messaging_api.push_message(
        PushMessageRequest(
            to=user_id,
            messages=[TextMessage(text=text, quickReply=QuickReply(items=qr_items))],
        )
    )


def push_date_picker(user_id: str, title_text: str, data_tag: str):
    """
    data_tag: "DATE:PICKUP" / "DATE:DELIVERY"
    """
    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    min_d = (today + timedelta(days=3)).strftime("%Y-%m-%d")
    max_d = (today + timedelta(days=14)).strftime("%Y-%m-%d")
    initial = min_d

    qr = QuickReply(
        items=[
            QuickReplyItem(
                action=DatetimePickerAction(
                    label="📅 選擇日期",
                    data=data_tag,
                    mode="date",
                    initial=initial,
                    min=min_d,
                    max=max_d,
                )
            )
        ]
    )

    messaging_api.push_message(
        PushMessageRequest(
            to=user_id,
            messages=[TextMessage(text=title_text, quickReply=qr)],
        )
    )


# =========================
# Flex builders
# =========================
def build_menu_preview_flex() -> dict:
    lines = [
        "達克瓦茲 NT$95（2入起）",
        "原味司康 NT$65",
        "原味可麗露 NT$90",
        "伊思尼奶酥厚片 NT$85",
    ]
    return {
        "type": "bubble",
        "size": "mega",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "甜點菜單", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "（全部甜點需提前 3 天預訂）", "size": "sm", "color": "#666666"},
                {"type": "text", "text": "\n".join([f"• {x}" for x in lines]), "wrap": True, "size": "md"},
                {"type": "separator", "margin": "lg"},
                {
                    "type": "button",
                    "style": "primary",
                    "action": {"type": "postback", "label": "我要下單", "data": "CMD:START_ORDER"},
                },
            ],
        },
    }


def build_order_menu_flex() -> dict:
    def btn(label: str, data: str) -> dict:
        return {"type": "button", "style": "primary", "action": {"type": "postback", "label": label, "data": data}}

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
                btn("達克瓦茲｜NT$95", "ITEM:dacquoise"),
                btn("原味司康｜NT$65", "ITEM:scone"),
                btn("原味可麗露｜NT$90", "ITEM:canele"),
                btn("伊思尼奶酥厚片｜NT$85", "ITEM:toast"),
                {"type": "separator", "margin": "lg"},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "🧾 前往結帳", "data": "CMD:CHECKOUT"}},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "🗑 清空重來", "data": "CMD:RESET"}},
            ],
        },
    }


def build_pickup_method_flex() -> dict:
    return {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "取貨方式", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "請選擇店取或宅配", "size": "sm", "color": "#666666"},
                {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🏪 店取", "data": "取貨:店取"}},
                {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🚚 冷凍宅配", "data": "取貨:宅配"}},
            ],
        },
    }


def build_confirm_card_pickup(sess: dict) -> dict:
    total = cart_total(sess.get("cart", []))
    date_text = pretty_date_tw(sess.get("pickup_date") or "")
    time_text = sess.get("pickup_time") or ""

    return {
        "type": "bubble",
        "size": "mega",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "✅ 店取資訊已選好", "weight": "bold", "size": "xl"},
                {"type": "text", "text": f"📅 日期：{date_text}", "wrap": True, "size": "md"},
                {"type": "text", "text": f"🕒 時段：{time_text}", "wrap": True, "size": "md"},
                {"type": "text", "text": f"📍 地址：{PICKUP_ADDRESS}", "wrap": True, "size": "sm", "color": "#666666"},
                {"type": "separator", "margin": "lg"},
                {"type": "text", "text": f"🧾 目前小計：NT${total}", "weight": "bold", "size": "lg"},
                {"type": "text", "text": "下一步請填取件人姓名，或返回加購。", "size": "sm", "color": "#666666", "wrap": True},
            ],
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "button", "style": "primary", "action": {"type": "postback", "label": "✍️ 填取件人姓名", "data": "CMD:ASK_PICKUP_NAME"}},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "➕ 繼續加購", "data": "CMD:START_ORDER"}},
            ],
        },
    }


def build_confirm_card_delivery(sess: dict) -> dict:
    total = cart_total(sess.get("cart", []))
    fee = shipping_fee(total)
    grand = total + fee
    date_text = pretty_date_tw(sess.get("delivery_date") or "")

    return {
        "type": "bubble",
        "size": "mega",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "✅ 宅配日期已選好", "weight": "bold", "size": "xl"},
                {"type": "text", "text": f"📅 希望到貨：{date_text}", "wrap": True, "size": "md"},
                {"type": "text", "text": "（僅希望日，不保證準時到貨）", "wrap": True, "size": "sm", "color": "#666666"},
                {"type": "separator", "margin": "lg"},
                {"type": "text", "text": f"🧾 小計：NT${total}", "size": "md", "wrap": True},
                {"type": "text", "text": f"🚚 運費：NT${fee}（滿2500免運）", "size": "md", "wrap": True},
                {"type": "text", "text": f"💰 應付總額：NT${grand}", "weight": "bold", "size": "lg", "wrap": True},
                {"type": "text", "text": "下一步請填收件人姓名，或返回加購。", "size": "sm", "color": "#666666", "wrap": True},
            ],
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "button", "style": "primary", "action": {"type": "postback", "label": "✍️ 填收件人姓名", "data": "CMD:ASK_DELIVERY_NAME"}},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "➕ 繼續加購", "data": "CMD:START_ORDER"}},
            ],
        },
    }


# =========================
# Business logic
# =========================
def show_menu_preview(user_id: str, reply_token: Optional[str] = None):
    flex = build_menu_preview_flex()
    if reply_token:
        reply_flex_dict(reply_token, "甜點菜單", flex)
    else:
        push_flex_dict(user_id, "甜點菜單", flex)


def show_order_menu(user_id: str, reply_token: Optional[str] = None):
    flex = build_order_menu_flex()
    if reply_token:
        reply_flex_dict(reply_token, "開始下單", flex)
    else:
        push_flex_dict(user_id, "開始下單", flex)


def ask_flavor(user_id: str, item_key: str):
    flavors = ITEMS[item_key]["flavors"]
    items = [{"label": f, "data": f"FLAVOR:{f}"} for f in flavors]
    push_quick_reply_postback(user_id, "你選了，請選口味：", items)


def ask_qty(user_id: str, item_key: str):
    min_qty = ITEMS[item_key]["min_qty"]
    items = [{"label": str(i), "data": f"QTY:{i}"} for i in range(min_qty, 13)]
    push_quick_reply_postback(user_id, f"請選數量（最少 {min_qty}）：", items)


def add_to_cart(user_id: str, item_key: str, flavor: Optional[str], qty: int):
    sess = get_session(user_id)
    meta = ITEMS[item_key]

    if meta["has_flavor"] and not flavor:
        raise ValueError("missing flavor")
    if qty < meta["min_qty"]:
        raise ValueError(f"qty must be >= {meta['min_qty']}")

    unit = meta["unit_price"]
    subtotal = unit * qty

    sess["cart"].append(
        {
            "item_key": item_key,
            "label": meta["label"],
            "flavor": flavor or "",
            "qty": qty,
            "unit_price": unit,
            "subtotal": subtotal,
        }
    )


def cart_summary_text(cart: List[dict]) -> str:
    lines = []
    for x in cart:
        name = x["label"]
        if x.get("flavor"):
            name += f"（{x['flavor']}）"
        lines.append(f"- {name} x{x['qty']} = {x['subtotal']}")
    total = cart_total(cart)
    return "\n".join(lines) + f"\n\n目前小計：{total}"


def after_added_actions(user_id: str):
    push_quick_reply_postback(
        user_id,
        "請選擇下一步 👇",
        [
            {"label": "➕ 繼續加購", "data": "CMD:START_ORDER"},
            {"label": "🧾 前往結帳", "data": "CMD:CHECKOUT"},
        ],
    )


def create_order_and_write_sheet(user_id: str, display_name: str) -> str:
    sess = get_session(user_id)
    cart = sess["cart"]
    if not cart:
        return ""

    order_id = gen_order_id()
    total = cart_total(cart)

    pickup_method = sess.get("pickup_method", "")
    pickup_date = sess.get("pickup_date", "")
    pickup_time = sess.get("pickup_time", "")
    note = sess.get("note", "")

    if pickup_method == "宅配":
        delivery_date = sess.get("delivery_date", "")
        dn = sess.get("delivery_name", "")
        dp = sess.get("delivery_phone", "")
        da = sess.get("delivery_address", "")
        note = (note + " | " if note else "") + f"希望到貨:{delivery_date} | 收件人:{dn} | 電話:{dp} | 地址:{da}"
        pickup_date = delivery_date
        pickup_time = ""

    if pickup_method == "店取":
        pn = sess.get("pickup_name", "")
        note = (note + " | " if note else "") + f"取件人:{pn}"

    row = [
        now_str(),
        user_id,
        display_name,
        order_id,
        json.dumps({"cart": cart}, ensure_ascii=False),
        pickup_method,
        pickup_date,
        pickup_time,
        note,
        total,
        "UNPAID",
        "",
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
# LINE Handlers — Text
# =========================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event: MessageEvent):
    user_id = event.source.user_id
    text = (event.message.text or "").strip()
    sess = get_session(user_id)

    display_name = "LINE用戶"

    if text in ["清空重來", "清空", "reset"]:
        reset_session(user_id)
        reply_text(event.reply_token, "已清空。輸入「甜點」看菜單，或輸入「我要下單」開始。")
        return

    if text == "甜點":
        show_menu_preview(user_id, reply_token=event.reply_token)
        return

    if text in ["我要下單", "下訂單", "開始下單"]:
        sess["state"] = "ORDERING"
        show_order_menu(user_id, reply_token=event.reply_token)
        return

    if text == "取貨說明":
        reply_text(event.reply_token, PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE)
        return

    if text == "付款說明":
        reply_text(event.reply_token, BANK_TRANSFER_TEXT)
        return

    if text.startswith("已轉帳"):
        reply_text(event.reply_token, "收到，我們會核對帳款後依訂單號安排出貨。若需補充資訊也可在此留言。")
        return

    # 店取/宅配姓名/電話/地址（這些仍需輸入）
    if sess["state"] == "WAIT_PICKUP_NAME":
        sess["pickup_name"] = text

        order_id = create_order_and_write_sheet(user_id, display_name)
        summary = cart_summary_text(sess["cart"])

        reply_text(
            event.reply_token,
            "✅ 訂單已建立\n"
            f"訂單編號：{order_id}\n\n"
            f"{summary}\n\n"
            f"取貨方式：店取\n取貨日期：{sess['pickup_date']}\n取貨時段：{sess['pickup_time']}\n"
            f"店取地址：{PICKUP_ADDRESS}\n\n"
            + BANK_TRANSFER_TEXT
        )

        sess["cart"] = []
        sess["state"] = "IDLE"
        return

    if sess["state"] == "WAIT_DELIVERY_NAME":
        sess["delivery_name"] = text
        sess["state"] = "WAIT_DELIVERY_PHONE"
        reply_text(event.reply_token, "請輸入宅配電話：")
        return

    if sess["state"] == "WAIT_DELIVERY_PHONE":
        sess["delivery_phone"] = text
        sess["state"] = "WAIT_DELIVERY_ADDRESS"
        reply_text(event.reply_token, "請輸入宅配地址（完整地址）：")
        return

    if sess["state"] == "WAIT_DELIVERY_ADDRESS":
        sess["delivery_address"] = text

        order_id = create_order_and_write_sheet(user_id, display_name)
        total = cart_total(sess["cart"])
        fee = shipping_fee(total)
        grand = total + fee
        summary = cart_summary_text(sess["cart"])

        reply_text(
            event.reply_token,
            "✅ 訂單已建立\n"
            f"訂單編號：{order_id}\n\n"
            f"{summary}\n\n"
            f"取貨方式：冷凍宅配\n希望到貨日期：{sess['delivery_date']}（不保證準時）\n"
            f"運費：{fee}\n應付總額：{grand}\n\n"
            f"收件人：{sess['delivery_name']}\n電話：{sess['delivery_phone']}\n地址：{sess['delivery_address']}\n\n"
            + DELIVERY_NOTICE
            + "\n\n"
            + BANK_TRANSFER_TEXT
        )

        sess["cart"] = []
        sess["state"] = "IDLE"
        return

    reply_text(event.reply_token, "輸入「甜點」看菜單，或輸入「我要下單」開始。")


# =========================
# LINE Handlers — Postback
# =========================
@handler.add(PostbackEvent)
def handle_postback(event: PostbackEvent):
    user_id = event.source.user_id
    data = (event.postback.data or "").strip()
    sess = get_session(user_id)
    rt = event.reply_token

    display_name = "LINE用戶"

    # ---- Date Picker 回傳（最先處理） ----
    params = event.postback.params or {}
    picked_date = params.get("date")

    if picked_date and data == "DATE:PICKUP":
        sess["pickup_date"] = picked_date
        sess["state"] = "WAIT_PICKUP_TIME"

        reply_text(rt, f"✅ 已選店取日期：{pretty_date_tw(picked_date)}\n請選店取時段（下方按鈕）。")
        push_quick_reply_postback(
            user_id,
            "請選店取時段：",
            [
                {"label": "10:00-12:00", "data": "時段:10:00-12:00"},
                {"label": "12:00-14:00", "data": "時段:12:00-14:00"},
                {"label": "14:00-16:00", "data": "時段:14:00-16:00"},
            ],
        )
        return

    if picked_date and data == "DATE:DELIVERY":
        sess["delivery_date"] = picked_date
        sess["state"] = "CONFIRM_DELIVERY_READY"
        reply_flex_dict(rt, "宅配資訊", build_confirm_card_delivery(sess))
        return

    # ---- 全域指令 ----
    if data == "CMD:RESET":
        reset_session(user_id)
        reply_text(rt, "已清空。輸入「甜點」看菜單，或輸入「我要下單」開始。")
        return

    if data == "CMD:START_ORDER":
        sess["state"] = "ORDERING"
        show_order_menu(user_id, reply_token=rt)
        return

    # ---- 結帳 ----
    if data == "CMD:CHECKOUT":
        if not sess["cart"]:
            reply_text(rt, "你的購物車是空的，請先輸入「我要下單」選商品。")
            return

        pickup_flex = build_pickup_method_flex()
        pickup_container = flex_container_from_dict(pickup_flex)
        if not pickup_container:
            reply_text(rt, "⚠️ 取貨卡片內容異常，請輸入「我要下單」再試一次。")
            return

        reply_messages(
            rt,
            [
                TextMessage(text="好，接著選取貨方式。"),
                FlexMessage(alt_text="取貨方式", contents=pickup_container),
            ],
        )
        sess["state"] = "WAIT_PICKUP_METHOD"
        return

    # ---- 取貨方式 ----
    if data.startswith("取貨:"):
        method = data.split(":", 1)[1].strip()
        if method not in ["店取", "宅配"]:
            reply_flex_dict(rt, "取貨方式", build_pickup_method_flex())
            return

        sess["pickup_method"] = method

        if method == "店取":
            sess["state"] = "WAIT_PICKUP_DATE"
            reply_text(rt, "店取：請用下方按鈕選擇取貨日期（3～14 天內）。")
            push_date_picker(user_id, "🌿 店取日期（3～14 天內）", "DATE:PICKUP")
            return

        if method == "宅配":
            sess["state"] = "WAIT_DELIVERY_DATE"
            reply_text(rt, "宅配：請用下方按鈕選擇「希望到貨日期」（3～14 天內，僅希望日不保證準時）。")
            push_date_picker(user_id, "🚚 希望到貨日期（3～14 天內）", "DATE:DELIVERY")
            return

    # ---- 店取時段 ----
    if data.startswith("時段:") and sess.get("state") == "WAIT_PICKUP_TIME":
        t = data.split(":", 1)[1].strip()
        sess["pickup_time"] = t
        sess["state"] = "CONFIRM_PICKUP_READY"
        reply_flex_dict(rt, "店取資訊", build_confirm_card_pickup(sess))
        return

    # ---- 確認卡按鈕：進入輸入姓名 ----
    if data == "CMD:ASK_PICKUP_NAME":
        sess["state"] = "WAIT_PICKUP_NAME"
        reply_text(rt, "請輸入店取取件人姓名：")
        return

    if data == "CMD:ASK_DELIVERY_NAME":
        sess["state"] = "WAIT_DELIVERY_NAME"
        reply_text(rt, "請輸入宅配收件人姓名：")
        return

    # ---- 選品項 ----
    if data.startswith("ITEM:"):
        item_key = data.split(":", 1)[1].strip()
        if item_key not in ITEMS:
            reply_text(rt, "品項不存在，請輸入「我要下單」重新開始。")
            return

        sess["pending_item"] = item_key
        sess["pending_flavor"] = None

        if ITEMS[item_key]["has_flavor"]:
            reply_text(rt, f"你選了：{ITEMS[item_key]['label']}，請選口味。")
            ask_flavor(user_id, item_key)  # push quick reply
            sess["state"] = "WAIT_FLAVOR"
        else:
            reply_text(rt, f"你選了：{ITEMS[item_key]['label']}，請選數量。")
            ask_qty(user_id, item_key)
            sess["state"] = "WAIT_QTY"
        return

    # ---- 選口味 ----
    if data.startswith("FLAVOR:"):
        flavor = data.split(":", 1)[1].strip()
        item_key = sess.get("pending_item")

        if not item_key or item_key not in ITEMS:
            reply_text(rt, "流程有點亂掉了，請輸入「我要下單」重新開始。")
            return

        if flavor not in ITEMS[item_key]["flavors"]:
            reply_text(rt, "口味不正確，請重新選口味。")
            ask_flavor(user_id, item_key)
            return

        sess["pending_flavor"] = flavor
        reply_text(rt, f"口味：{flavor}\n請選數量。")
        ask_qty(user_id, item_key)
        sess["state"] = "WAIT_QTY"
        return

    # ---- 選數量 ----
    if data.startswith("QTY:"):
        try:
            qty = int(data.split(":", 1)[1].strip())
        except Exception:
            reply_text(rt, "數量格式錯誤，請重新選擇。")
            return

        item_key = sess.get("pending_item")
        flavor = sess.get("pending_flavor")

        if not item_key or item_key not in ITEMS:
            reply_text(rt, "流程有點亂掉了，請輸入「我要下單」重新開始。")
            return

        try:
            add_to_cart(user_id, item_key, flavor, qty)
        except Exception as e:
            reply_text(rt, f"加入失敗：{e}\n請輸入「我要下單」重新開始。")
            return

        sess["pending_item"] = None
        sess["pending_flavor"] = None
        sess["state"] = "ORDERING"

        total = cart_total(sess["cart"])
        reply_text(rt, f"✅ 已加入購物車\n目前小計：{total}")
        after_added_actions(user_id)
        return

    reply_text(rt, "輸入「甜點」看菜單，或輸入「我要下單」開始。")
