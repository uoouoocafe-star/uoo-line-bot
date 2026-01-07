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
    QuickReply,
    QuickReplyItem,
    MessageAction,
    PostbackAction,
)

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

# ✅ 不要預設 sheet1，避免你又踩回去
SHEET_NAME = os.getenv("SHEET_NAME", "").strip()

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
# （Render free 可能重啟會清空，先讓你跑起來）
# =========================
SESSIONS: Dict[str, Dict[str, Any]] = {}


def get_session(user_id: str) -> Dict[str, Any]:
    if user_id not in SESSIONS:
        SESSIONS[user_id] = {
            "cart": [],
            "state": "IDLE",              # IDLE / ORDERING / WAIT_FLAVOR / WAIT_QTY / WAIT_PICKUP_METHOD / ...
            "pending_item": None,
            "pending_flavor": None,

            "pickup_method": None,        # 店取 / 宅配
            "pickup_date": None,          # YYYY-MM-DD
            "pickup_time": None,          # 10:00-12:00 ...
            "pickup_name": None,

            "delivery_date": None,        # YYYY-MM-DD (希望到貨)
            "delivery_name": None,
            "delivery_phone": None,
            "delivery_address": None,
        }
    return SESSIONS[user_id]


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
    if not SHEET_NAME:
        print("[WARN] SHEET_NAME missing, skip append.")
        return False

    service = get_sheets_service()
    if not service:
        print("[WARN] Google Sheet env missing, skip append.")
        return False

    try:
        # ✅ 分頁名用單引號最穩（中文/空格都不怕）
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


def weekday_zh(d: datetime) -> str:
    mapping = ["一", "二", "三", "四", "五", "六", "日"]
    return mapping[d.weekday()]


def pretty_date_tw(yyyy_mm_dd: str) -> str:
    try:
        dt = datetime.strptime(yyyy_mm_dd, "%Y-%m-%d").replace(tzinfo=TZ)
        return f"{dt.month}/{dt.day}（{weekday_zh(dt)}）"
    except Exception:
        return yyyy_mm_dd


def date_candidates_3_to_14_days() -> List[str]:
    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    dates = []
    for i in range(3, 15):  # 3..14
        dt = today + timedelta(days=i)
        dates.append(dt.strftime("%Y-%m-%d"))
    return dates


def reply_text(reply_token: str, text: str):
    messaging_api.reply_message(
        ReplyMessageRequest(
            replyToken=reply_token,
            messages=[TextMessage(text=text)],
        )
    )


def push_text(user_id: str, text: str):
    messaging_api.push_message(
        PushMessageRequest(
            to=user_id,
            messages=[TextMessage(text=text)],
        )
    )


def reply_flex(reply_token: str, alt_text: str, flex_content: dict):
    messaging_api.reply_message(
        ReplyMessageRequest(
            replyToken=reply_token,
            messages=[FlexMessage(altText=alt_text, contents=flex_content)],
        )
    )


def push_flex(user_id: str, alt_text: str, flex_content: dict):
    messaging_api.push_message(
        PushMessageRequest(
            to=user_id,
            messages=[FlexMessage(altText=alt_text, contents=flex_content)],
        )
    )


def reply_quickreply_postback(reply_token: str, text: str, buttons: List[dict]):
    """
    buttons: [{"label": "...", "data": "..."}]
    """
    qr = QuickReply(
        items=[
            QuickReplyItem(action=PostbackAction(label=b["label"], data=b["data"], displayText=b.get("displayText")))
            for b in buttons
        ]
    )
    messaging_api.reply_message(
        ReplyMessageRequest(
            replyToken=reply_token,
            messages=[TextMessage(text=text, quickReply=qr)],
        )
    )


def push_quickreply_postback(user_id: str, text: str, buttons: List[dict]):
    qr = QuickReply(
        items=[
            QuickReplyItem(action=PostbackAction(label=b["label"], data=b["data"], displayText=b.get("displayText")))
            for b in buttons
        ]
    )
    messaging_api.push_message(
        PushMessageRequest(
            to=user_id,
            messages=[TextMessage(text=text, quickReply=qr)],
        )
    )


def reset_order_flow(sess: dict):
    sess["cart"] = []
    sess["state"] = "IDLE"
    sess["pending_item"] = None
    sess["pending_flavor"] = None

    sess["pickup_method"] = None
    sess["pickup_date"] = None
    sess["pickup_time"] = None
    sess["pickup_name"] = None

    sess["delivery_date"] = None
    sess["delivery_name"] = None
    sess["delivery_phone"] = None
    sess["delivery_address"] = None


# =========================
# Cart operations
# =========================
def add_to_cart(sess: dict, item_key: str, flavor: Optional[str], qty: int):
    meta = ITEMS[item_key]
    if meta["has_flavor"] and not flavor:
        raise ValueError("請先選口味")
    if qty < meta["min_qty"]:
        raise ValueError(f"此品項最少需 {meta['min_qty']} 個")

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


def change_cart_qty(sess: dict, idx: int, delta: int) -> str:
    cart = sess.get("cart", [])
    if idx < 0 or idx >= len(cart):
        return "操作失敗：找不到該品項。"

    item = cart[idx]
    item_key = item.get("item_key")
    if item_key not in ITEMS:
        return "操作失敗：品項資料不完整。"

    min_qty = ITEMS[item_key]["min_qty"]
    old_qty = int(item.get("qty", 0))
    new_qty = old_qty + delta

    # new_qty <=0 -> delete
    if new_qty <= 0:
        removed = cart.pop(idx)
        name = removed.get("label", "")
        flavor = removed.get("flavor", "")
        return f"已刪除：{name}{('（'+flavor+'）') if flavor else ''}"

    # below min_qty -> reject
    if new_qty < min_qty:
        name = item.get("label", "")
        flavor = item.get("flavor", "")
        return f"不能再減了：{name}{('（'+flavor+'）') if flavor else ''} 最少需 {min_qty} 個。"

    unit = int(item.get("unit_price", 0))
    item["qty"] = new_qty
    item["subtotal"] = unit * new_qty
    return "已更新數量。"


def cart_summary_lines(cart: List[dict]) -> str:
    lines = []
    for x in cart:
        name = x["label"]
        if x.get("flavor"):
            name += f"（{x['flavor']}）"
        lines.append(f"- {name} x{x['qty']} = NT${x['subtotal']}")
    return "\n".join(lines)


# =========================
# Flex builders
# =========================
def build_dessert_menu_flex(mode: str = "BROWSE") -> dict:
    """
    mode:
      - BROWSE: 甜點瀏覽卡（含「我要下單」按鈕）
      - ORDER: 進入下單卡（商品可直接點）
    """
    title = "請選擇商品" if mode == "ORDER" else "甜點菜單"
    subtitle = "（全部甜點需提前 3 天預訂）"

    def item_btn(label: str, data: str) -> dict:
        return {
            "type": "button",
            "style": "primary",
            "action": {"type": "postback", "label": label, "data": data},
        }

    contents = [
        {"type": "text", "text": title, "weight": "bold", "size": "xl"},
        {"type": "text", "text": subtitle, "size": "sm", "color": "#666666"},
        item_btn("達克瓦茲｜NT$95", "ITEM:dacquoise"),
        item_btn("原味司康｜NT$65", "ITEM:scone"),
        item_btn("原味可麗露｜NT$90", "ITEM:canele"),
        item_btn("伊思尼奶酥厚片｜NT$85", "ITEM:toast"),
        {"type": "separator", "margin": "lg"},
    ]

    footer_btns = []
    if mode == "BROWSE":
        footer_btns = [
            {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🧾 我要下單", "data": "CMD:START_ORDER"}},
            {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "📌 取貨說明", "data": "CMD:INFO_PICKUP"}},
            {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "💰 付款說明", "data": "CMD:INFO_PAY"}},
        ]
    else:
        footer_btns = [
            {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "🧾 前往結帳", "data": "CMD:CHECKOUT"}},
            {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "🛠 修改內容", "data": "CMD:EDIT_CART"}},
            {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "🗑 清空重來", "data": "CMD:RESET"}},
        ]

    return {
        "type": "bubble",
        "size": "mega",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": contents},
        "footer": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": footer_btns},
    }


def build_pickup_method_flex() -> dict:
    return {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "請選擇店取或宅配", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "日期可選 3～14 天內", "size": "sm", "color": "#666666"},
                {
                    "type": "button",
                    "style": "primary",
                    "action": {"type": "postback", "label": "🏪 店取", "data": "PICKUP:STORE"},
                },
                {
                    "type": "button",
                    "style": "primary",
                    "action": {"type": "postback", "label": "🚚 冷凍宅配", "data": "PICKUP:DELIVERY"},
                },
            ],
        },
    }


def build_cart_lines_contents(cart: List[dict]) -> List[dict]:
    """
    用於「結帳確認卡」顯示清單
    """
    contents: List[dict] = []
    if not cart:
        return [{"type": "text", "text": "（購物車目前是空的）", "size": "sm", "color": "#666666"}]

    show = cart[:8]
    for idx, x in enumerate(show, start=1):
        name = x.get("label", "")
        flavor = x.get("flavor", "")
        qty = int(x.get("qty", 0))
        subtotal = int(x.get("subtotal", 0))

        title = f"{idx}. {name}" + (f"（{flavor}）" if flavor else "")
        contents.append({"type": "text", "text": title, "wrap": True, "size": "sm"})
        contents.append(
            {
                "type": "box",
                "layout": "baseline",
                "contents": [
                    {"type": "text", "text": f"x{qty}", "size": "sm", "color": "#666666", "flex": 0},
                    {"type": "text", "text": f"NT${subtotal}", "size": "sm", "align": "end", "flex": 1},
                ],
            }
        )
        contents.append({"type": "separator", "margin": "md"})

    if len(cart) > 8:
        contents.append({"type": "text", "text": f"…還有 {len(cart)-8} 筆未顯示", "size": "sm", "color": "#666666"})
    return contents


def build_confirm_card_pickup(sess: dict) -> dict:
    cart = sess.get("cart", [])
    total = cart_total(cart)
    date_text = pretty_date_tw(sess.get("pickup_date") or "")
    time_text = sess.get("pickup_time") or ""
    cart_contents = build_cart_lines_contents(cart)

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
                {"type": "text", "text": "🧾 結帳內容", "weight": "bold", "size": "lg"},
                {"type": "box", "layout": "vertical", "spacing": "sm", "contents": cart_contents},
                {"type": "separator", "margin": "lg"},
                {"type": "text", "text": f"目前小計：NT${total}", "weight": "bold", "size": "lg"},
                {"type": "text", "text": "下一步請填取件人姓名，或修改內容。", "size": "sm", "color": "#666666", "wrap": True},
            ],
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "button", "style": "primary", "action": {"type": "postback", "label": "✍️ 填取件人姓名", "data": "CMD:ASK_PICKUP_NAME"}},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "🛠 修改內容", "data": "CMD:EDIT_CART"}},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "➕ 繼續加購", "data": "CMD:START_ORDER"}},
            ],
        },
    }


def build_confirm_card_delivery(sess: dict) -> dict:
    cart = sess.get("cart", [])
    total = cart_total(cart)
    fee = shipping_fee(total)
    grand = total + fee
    date_text = pretty_date_tw(sess.get("delivery_date") or "")
    cart_contents = build_cart_lines_contents(cart)

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
                {"type": "text", "text": "🧾 結帳內容", "weight": "bold", "size": "lg"},
                {"type": "box", "layout": "vertical", "spacing": "sm", "contents": cart_contents},
                {"type": "separator", "margin": "lg"},
                {"type": "text", "text": f"小計：NT${total}", "size": "md", "wrap": True},
                {"type": "text", "text": f"運費：NT${fee}（滿2500免運）", "size": "md", "wrap": True},
                {"type": "text", "text": f"應付總額：NT${grand}", "weight": "bold", "size": "lg", "wrap": True},
                {"type": "text", "text": "下一步請填收件人資料，或修改內容。", "size": "sm", "color": "#666666", "wrap": True},
            ],
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "button", "style": "primary", "action": {"type": "postback", "label": "✍️ 填收件人姓名", "data": "CMD:ASK_DELIVERY_NAME"}},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "🛠 修改內容", "data": "CMD:EDIT_CART"}},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "➕ 繼續加購", "data": "CMD:START_ORDER"}},
            ],
        },
    }


def build_cart_edit_flex(sess: dict) -> dict:
    cart = sess.get("cart", [])
    total = cart_total(cart)

    contents: List[dict] = [
        {"type": "text", "text": "🛠 修改結帳內容", "weight": "bold", "size": "xl"},
        {"type": "text", "text": "可直接加減數量（達克瓦茲最少 2 個）。", "size": "sm", "color": "#666666", "wrap": True},
        {"type": "separator", "margin": "md"},
    ]

    if not cart:
        contents.append({"type": "text", "text": "購物車目前是空的。", "size": "sm", "color": "#666666"})
    else:
        for idx, x in enumerate(cart):
            name = x.get("label", "")
            flavor = x.get("flavor", "")
            qty = int(x.get("qty", 0))
            subtotal = int(x.get("subtotal", 0))

            title = f"{name}" + (f"（{flavor}）" if flavor else "")
            contents.append({"type": "text", "text": title, "weight": "bold", "size": "md", "wrap": True})
            contents.append({
                "type": "box",
                "layout": "baseline",
                "contents": [
                    {"type": "text", "text": f"數量：{qty}", "size": "sm", "color": "#666666", "flex": 0},
                    {"type": "text", "text": f"NT${subtotal}", "size": "sm", "align": "end", "flex": 1},
                ],
            })

            contents.append({
                "type": "box",
                "layout": "horizontal",
                "spacing": "sm",
                "contents": [
                    {"type": "button", "style": "secondary", "height": "sm", "action": {"type": "postback", "label": "➖", "data": f"QTY:-1:{idx}"}, "flex": 1},
                    {"type": "button", "style": "secondary", "height": "sm", "action": {"type": "postback", "label": "➕", "data": f"QTY:+1:{idx}"}, "flex": 1},
                    {"type": "button", "style": "secondary", "height": "sm", "action": {"type": "postback", "label": "🗑", "data": f"DEL:{idx}"}, "flex": 1},
                ],
            })

            contents.append({"type": "separator", "margin": "md"})

        contents.append({"type": "text", "text": f"目前小計：NT${total}", "weight": "bold", "size": "lg"})

    return {
        "type": "bubble",
        "size": "mega",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": contents},
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "button", "style": "primary", "action": {"type": "postback", "label": "返回結帳確認", "data": "CMD:BACK_TO_CONFIRM"}},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "清空購物車", "data": "CMD:RESET"}},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "➕ 繼續加購", "data": "CMD:START_ORDER"}},
            ],
        },
    }


# =========================
# Order persistence
# =========================
def create_order_and_write_sheet(user_id: str) -> str:
    sess = get_session(user_id)
    cart = sess["cart"]
    if not cart:
        return ""

    order_id = gen_order_id()
    total = cart_total(cart)

    pickup_method = sess.get("pickup_method") or ""
    pickup_date = sess.get("pickup_date") or ""
    pickup_time = sess.get("pickup_time") or ""

    note = ""
    if pickup_method == "宅配":
        # 希望到貨日期（delivery_date）
        pickup_date = sess.get("delivery_date") or ""
        pickup_time = ""
        note = f"希望到貨:{pickup_date} | 收件人:{sess.get('delivery_name','')} | 電話:{sess.get('delivery_phone','')} | 地址:{sess.get('delivery_address','')}"
    elif pickup_method == "店取":
        note = f"取件人:{sess.get('pickup_name','')}"

    row = [
        now_str(),
        user_id,
        "",  # display_name 先留空（你要抓 profile 我也可加）
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
# LINE handlers
# =========================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event: MessageEvent):
    user_id = event.source.user_id
    text = event.message.text.strip()
    sess = get_session(user_id)

    # 基本指令
    if text in ["清空", "清空重來", "reset"]:
        reset_order_flow(sess)
        reply_text(event.reply_token, "已清空。輸入「甜點」看菜單，或輸入「我要下單」開始下單。")
        return

    if text == "甜點":
        # 只顯示菜單（瀏覽）
        reply_flex(event.reply_token, "甜點菜單", build_dessert_menu_flex(mode="BROWSE"))
        sess["state"] = "IDLE"
        return

    if text in ["我要下單", "下單", "開始下單"]:
        # 進入下單
        sess["state"] = "ORDERING"
        reply_flex(event.reply_token, "開始下單", build_dessert_menu_flex(mode="ORDER"))
        return

    if text == "取貨說明":
        reply_text(event.reply_token, PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE)
        return

    if text == "付款說明":
        reply_text(event.reply_token, BANK_TRANSFER_TEXT)
        return

    # 依 state 收資料（姓名/電話/地址）
    if sess["state"] == "WAIT_PICKUP_NAME":
        sess["pickup_name"] = text
        sess["state"] = "CONFIRM_DONE"
        order_id = create_order_and_write_sheet(user_id)

        summary = cart_summary_lines(sess["cart"])
        total = cart_total(sess["cart"])

        reply_text(
            event.reply_token,
            "✅ 訂單已建立\n"
            f"訂單編號：{order_id}\n\n"
            f"{summary}\n\n"
            f"小計：NT${total}\n\n"
            f"取貨方式：店取\n"
            f"日期：{pretty_date_tw(sess.get('pickup_date') or '')}\n"
            f"時段：{sess.get('pickup_time')}\n"
            f"地址：{PICKUP_ADDRESS}\n\n"
            + BANK_TRANSFER_TEXT
        )

        # 完成後清空購物車
        reset_order_flow(sess)
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
        sess["state"] = "CONFIRM_DONE"
        order_id = create_order_and_write_sheet(user_id)

        total = cart_total(sess["cart"])
        fee = shipping_fee(total)
        grand = total + fee
        summary = cart_summary_lines(sess["cart"])

        reply_text(
            event.reply_token,
            "✅ 訂單已建立\n"
            f"訂單編號：{order_id}\n\n"
            f"{summary}\n\n"
            f"小計：NT${total}\n運費：NT${fee}\n應付總額：NT${grand}\n\n"
            f"取貨方式：冷凍宅配\n"
            f"希望到貨：{pretty_date_tw(sess.get('delivery_date') or '')}（不保證準時）\n"
            f"收件人：{sess.get('delivery_name')}\n"
            f"電話：{sess.get('delivery_phone')}\n"
            f"地址：{sess.get('delivery_address')}\n\n"
            + DELIVERY_NOTICE
            + "\n\n"
            + BANK_TRANSFER_TEXT
        )

        reset_order_flow(sess)
        return

    # 轉帳回覆
    if text.startswith("已轉帳"):
        reply_text(event.reply_token, "收到，我們會核對帳款後依訂單號安排出貨。若需補充資訊可直接留言。")
        return

    # fallback
    reply_text(event.reply_token, "請輸入「甜點」看菜單，或輸入「我要下單」開始下單。")


@handler.add(PostbackEvent)
def handle_postback(event: PostbackEvent):
    user_id = event.source.user_id
    sess = get_session(user_id)
    data = event.postback.data
    rt = event.reply_token

    # 全域
    if data == "CMD:RESET":
        reset_order_flow(sess)
        reply_text(rt, "已清空。輸入「甜點」看菜單，或按「我要下單」開始。")
        return

    if data == "CMD:INFO_PICKUP":
        reply_text(rt, PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE)
        return

    if data == "CMD:INFO_PAY":
        reply_text(rt, BANK_TRANSFER_TEXT)
        return

    if data == "CMD:START_ORDER":
        sess["state"] = "ORDERING"
        reply_flex(rt, "開始下單", build_dessert_menu_flex(mode="ORDER"))
        return

    # 下單未開始，禁止點商品
    if data.startswith("ITEM:") and sess.get("state") != "ORDERING":
        reply_text(rt, "要先按「我要下單」才會開始選購喔。")
        return

    # 商品選擇
    if data.startswith("ITEM:"):
        item_key = data.split(":", 1)[1]
        if item_key not in ITEMS:
            reply_text(rt, "品項不存在，請重新操作。")
            return

        sess["pending_item"] = item_key
        sess["pending_flavor"] = None

        meta = ITEMS[item_key]
        if meta["has_flavor"]:
            # 口味 quick reply（postback，不會送出文字）
            buttons = [{"label": f, "data": f"FLAVOR:{f}"} for f in meta["flavors"]]
            reply_quickreply_postback(rt, f"你選了：{meta['label']}\n請選口味：", buttons)
            sess["state"] = "WAIT_FLAVOR"
        else:
            # 直接選數量
            min_qty = meta["min_qty"]
            qty_buttons = [{"label": str(i), "data": f"QTY:{i}"} for i in range(min_qty, 13)]
            reply_quickreply_postback(rt, f"你選了：{meta['label']}\n請選數量（最少 {min_qty}）：", qty_buttons)
            sess["state"] = "WAIT_QTY"
        return

    # 口味
    if data.startswith("FLAVOR:"):
        flavor = data.split(":", 1)[1]
        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            reply_text(rt, "流程有點亂掉了，請按「我要下單」重新開始。")
            return

        if flavor not in ITEMS[item_key]["flavors"]:
            reply_text(rt, "口味不正確，請重新選。")
            return

        sess["pending_flavor"] = flavor
        min_qty = ITEMS[item_key]["min_qty"]
        qty_buttons = [{"label": str(i), "data": f"QTY:{i}"} for i in range(min_qty, 13)]
        reply_quickreply_postback(rt, f"已選口味：{flavor}\n請選數量（最少 {min_qty}）：", qty_buttons)
        sess["state"] = "WAIT_QTY"
        return

    # 數量（加入購物車）
    if data.startswith("QTY:") and sess.get("state") == "WAIT_QTY":
        qty_raw = data.split(":", 1)[1]
        try:
            qty = int(qty_raw)
        except Exception:
            reply_text(rt, "數量格式錯誤，請重新選擇。")
            return

        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            reply_text(rt, "流程有點亂掉了，請按「我要下單」重新開始。")
            return

        flavor = sess.get("pending_flavor")
        try:
            add_to_cart(sess, item_key, flavor, qty)
        except Exception as e:
            reply_text(rt, f"加入失敗：{e}")
            return

        # 清 pending
        sess["pending_item"] = None
        sess["pending_flavor"] = None
        sess["state"] = "ORDERING"

        total = cart_total(sess["cart"])
        reply_text(rt, f"✅ 已加入購物車\n目前小計：NT${total}")

        # 下一步（用 push，避免 reply token 壓力）
        push_flex(user_id, "下單中", build_dessert_menu_flex(mode="ORDER"))
        return

    # 前往結帳
    if data == "CMD:CHECKOUT":
        if not sess["cart"]:
            reply_text(rt, "購物車是空的，先加入商品喔。")
            return
        reply_flex(rt, "取貨方式", build_pickup_method_flex())
        sess["state"] = "WAIT_PICKUP_METHOD"
        return

    # 取貨方式
    if data == "PICKUP:STORE":
        sess["pickup_method"] = "店取"
        # 日期按鈕
        buttons = []
        for d in date_candidates_3_to_14_days():
            buttons.append({"label": pretty_date_tw(d), "data": f"DATE_PICKUP:{d}"})
        reply_quickreply_postback(rt, "🌿 店取日期（3～14天內）\n請點選日期：", buttons)
        sess["state"] = "WAIT_PICKUP_DATE"
        return

    if data == "PICKUP:DELIVERY":
        sess["pickup_method"] = "宅配"
        buttons = []
        for d in date_candidates_3_to_14_days():
            buttons.append({"label": pretty_date_tw(d), "data": f"DATE_DELIVERY:{d}"})
        reply_quickreply_postback(rt, "🚚 宅配希望到貨日（3～14天內）\n請點選日期：", buttons)
        sess["state"] = "WAIT_DELIVERY_DATE"
        return

    # 店取日期
    if data.startswith("DATE_PICKUP:"):
        d = data.split(":", 1)[1]
        sess["pickup_date"] = d
        # 時段按鈕
        buttons = [
            {"label": "10:00-12:00", "data": "TIME:10:00-12:00"},
            {"label": "12:00-14:00", "data": "TIME:12:00-14:00"},
            {"label": "14:00-16:00", "data": "TIME:14:00-16:00"},
        ]
        reply_quickreply_postback(rt, f"✅ 已選店取日期：{pretty_date_tw(d)}\n請選店取時段：", buttons)
        sess["state"] = "WAIT_PICKUP_TIME"
        return

    # 店取時段
    if data.startswith("TIME:") and sess.get("state") == "WAIT_PICKUP_TIME":
        t = data.split(":", 1)[1]
        sess["pickup_time"] = t
        # 顯示確認卡（含清單、小計、修改按鈕）
        reply_flex(rt, "店取確認", build_confirm_card_pickup(sess))
        sess["state"] = "WAIT_PICKUP_NAME_READY"
        return

    # 宅配日期
    if data.startswith("DATE_DELIVERY:"):
        d = data.split(":", 1)[1]
        sess["delivery_date"] = d
        reply_flex(rt, "宅配確認", build_confirm_card_delivery(sess))
        sess["state"] = "WAIT_DELIVERY_NAME_READY"
        return

    # 修改內容
    if data == "CMD:EDIT_CART":
        reply_flex(rt, "修改結帳內容", build_cart_edit_flex(sess))
        return

    # QTY:+1 / QTY:-1 (修改卡內)
    if data.startswith("QTY:") and sess.get("state") != "WAIT_QTY":
        # 格式：QTY:+1:idx 或 QTY:-1:idx
        try:
            _, delta_raw, idx_raw = data.split(":")
            delta = 1 if delta_raw == "+1" else -1
            idx = int(idx_raw)
        except Exception:
            reply_text(rt, "操作失敗：格式錯誤。")
            return

        msg = change_cart_qty(sess, idx, delta)
        reply_text(rt, msg)
        push_flex(user_id, "修改結帳內容", build_cart_edit_flex(sess))
        return

    # 刪除某筆
    if data.startswith("DEL:"):
        try:
            idx = int(data.split(":", 1)[1])
            cart = sess.get("cart", [])
            if 0 <= idx < len(cart):
                removed = cart.pop(idx)
                name = removed.get("label", "")
                flavor = removed.get("flavor", "")
                reply_text(rt, f"已刪除：{name}{('（'+flavor+'）') if flavor else ''}")
            else:
                reply_text(rt, "刪除失敗：找不到該品項。")
        except Exception:
            reply_text(rt, "刪除失敗：格式錯誤。")

        push_flex(user_id, "修改結帳內容", build_cart_edit_flex(sess))
        return

    # 返回確認卡
    if data == "CMD:BACK_TO_CONFIRM":
        if sess.get("pickup_method") == "店取" and sess.get("pickup_date") and sess.get("pickup_time"):
            reply_flex(rt, "店取確認", build_confirm_card_pickup(sess))
        elif sess.get("pickup_method") == "宅配" and sess.get("delivery_date"):
            reply_flex(rt, "宅配確認", build_confirm_card_delivery(sess))
        else:
            reply_text(rt, "尚未完成結帳資訊，請先前往結帳。")
        return

    # 問取件人姓名
    if data == "CMD:ASK_PICKUP_NAME":
        sess["state"] = "WAIT_PICKUP_NAME"
        reply_text(rt, "請輸入店取取件人姓名：")
        return

    # 問宅配姓名（接著電話、地址）
    if data == "CMD:ASK_DELIVERY_NAME":
        sess["state"] = "WAIT_DELIVERY_NAME"
        reply_text(rt, "請輸入宅配收件人姓名：")
        return

    # fallback
    reply_text(rt, "我沒看懂你的操作，請輸入「甜點」或「我要下單」。")
