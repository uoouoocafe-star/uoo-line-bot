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
from linebot.v3.webhooks import MessageEvent, TextMessageContent

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
# In-memory session store (簡單版)
# 你部署在 Render free 可能會重啟，購物車就會清掉（可接受先跑起來）
# =========================
SESSIONS: Dict[str, Dict[str, Any]] = {}


def get_session(user_id: str) -> Dict[str, Any]:
    if user_id not in SESSIONS:
        SESSIONS[user_id] = {
            "cart": [],  # list of items
            "state": "IDLE",
            "pending_item": None,
            "pending_flavor": None,
            "pickup_method": None,  # 店取 / 宅配
            "pickup_date": None,
            "pickup_time": None,
            "pickup_name": None,
            "delivery_date": None,  # 希望到貨日期
            "delivery_name": None,
            "delivery_phone": None,
            "delivery_address": None,
            "note": "",
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

    service = get_sheets_service()
    if not service:
        print("[WARN] Google Sheet env missing, skip append.")
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


def parse_date_yyyy_mm_dd(s: str) -> Optional[datetime]:
    try:
        dt = datetime.strptime(s.strip(), "%Y-%m-%d")
        return dt.replace(tzinfo=TZ)
    except Exception:
        return None


def date_in_range_3_to_14_days(dt: datetime) -> bool:
    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    min_d = today + timedelta(days=3)
    max_d = today + timedelta(days=14)
    target = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return min_d <= target <= max_d


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


def reply_flex_json(reply_token: str, alt_text: str, flex_content: dict):
    messaging_api.reply_message(
        ReplyMessageRequest(
            replyToken=reply_token,
            messages=[
                FlexMessage(
                    altText=alt_text,
                    contents=flex_content,
                )
            ],
        )
    )


def push_quick_reply(user_id: str, text: str, items: List[dict]):
    # items: [{"label": "...", "text": "..."}]
    qr = {
        "type": "text",
        "text": text,
        "quickReply": {
            "items": [
                {
                    "type": "action",
                    "action": {"type": "message", "label": it["label"], "text": it["text"]},
                }
                for it in items
            ]
        },
    }
    messaging_api.push_message(
        PushMessageRequest(
            to=user_id,
            messages=[qr],
        )
    )


# =========================
# Flex builders
# =========================
def build_product_menu_flex() -> dict:
    # 4 buttons + checkout / clear
    def btn(label: str, text: str) -> dict:
        return {
            "type": "button",
            "style": "primary",
            "action": {"type": "message", "label": label, "text": text},
        }

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
                {
                    "type": "separator",
                    "margin": "lg",
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "action": {"type": "message", "label": "🧾 前往結帳", "text": "前往結帳"},
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "action": {"type": "message", "label": "🗑 清空重來", "text": "清空重來"},
                },
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
                {
                    "type": "text",
                    "text": "請選擇店取或宅配",
                    "size": "sm",
                    "color": "#666666",
                },
                {
                    "type": "button",
                    "style": "primary",
                    "action": {"type": "message", "label": "🏪 店取", "text": "取貨:店取"},
                },
                {
                    "type": "button",
                    "style": "primary",
                    "action": {"type": "message", "label": "🚚 冷凍宅配", "text": "取貨:宅配"},
                },
            ],
        },
    }


# =========================
# Business logic
# =========================
def show_product_menu(user_id: str, reply_token: Optional[str] = None):
    flex = build_product_menu_flex()
    if reply_token:
        reply_flex_json(reply_token, "甜點選單", flex)
    else:
        messaging_api.push_message(
            PushMessageRequest(
                to=user_id,
                messages=[FlexMessage(altText="甜點選單", contents=flex)],
            )
        )


def ask_flavor(user_id: str, item_key: str):
    flavors = ITEMS[item_key]["flavors"]
    items = [{"label": f, "text": f"FLAVOR:{f}"} for f in flavors]
    push_quick_reply(user_id, "你選了，請選口味：", items)


def ask_qty(user_id: str, item_key: str):
    min_qty = ITEMS[item_key]["min_qty"]
    # 做 2~12 或 1~12
    start = min_qty
    end = 12
    items = [{"label": str(i), "text": f"QTY:{i}"} for i in range(start, end + 1)]
    push_quick_reply(user_id, f"請選數量（最少 {min_qty}）：", items)


def add_to_cart(user_id: str, item_key: str, flavor: Optional[str], qty: int):
    sess = get_session(user_id)
    meta = ITEMS[item_key]
    unit = meta["unit_price"]
    label = meta["label"]

    if meta["has_flavor"] and not flavor:
        raise ValueError("missing flavor")

    if qty < meta["min_qty"]:
        raise ValueError(f"qty must be >= {meta['min_qty']}")

    # 達克瓦茲「口味不可混」：這裡做法是每一筆都綁定一個 flavor，自然不會混
    subtotal = unit * qty
    sess["cart"].append(
        {
            "item_key": item_key,
            "label": label,
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
    # 加購 or 結帳 quick reply
    push_quick_reply(
        user_id,
        "請選擇下一步 👇",
        [
            {"label": "➕ 繼續加購", "text": "甜點"},
            {"label": "🧾 前往結帳", "text": "前往結帳"},
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

    # 宅配：把地址/電話/姓名一起塞在 note 裡（簡單好查）
    if pickup_method == "宅配":
        delivery_date = sess.get("delivery_date", "")
        dn = sess.get("delivery_name", "")
        dp = sess.get("delivery_phone", "")
        da = sess.get("delivery_address", "")
        note = (note + " | " if note else "") + f"希望到貨:{delivery_date} | 收件人:{dn} | 電話:{dp} | 地址:{da}"
        # pickup_date 欄位改存希望到貨日期，方便你在表格看
        pickup_date = delivery_date
        pickup_time = ""

    # 店取：把取件人姓名寫入 note（你希望店取要收取件人姓名）
    if pickup_method == "店取":
        pn = sess.get("pickup_name", "")
        note = (note + " | " if note else "") + f"取件人:{pn}"

    row = [
        now_str(),          # created_at
        user_id,            # user_id
        display_name,       # display_name
        order_id,           # order_id
        json.dumps({"cart": cart}, ensure_ascii=False),  # items_json
        pickup_method,      # pickup_method
        pickup_date,        # pickup_date (宅配＝希望到貨日期)
        pickup_time,        # pickup_time
        note,               # note
        total,              # amount
        "UNPAID",           # pay_status
        "",                 # linepay_transaction_id (先留空)
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
# LINE Webhook Handlers
# =========================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event: MessageEvent):
    user_id = event.source.user_id
    text = event.message.text.strip()
    sess = get_session(user_id)

    # 取 display name（可先不用抓 profile，避免多 API）
    display_name = "LINE用戶"

    # ---------- global commands ----------
    if text in ["清空重來", "清空", "reset"]:
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
        sess["note"] = ""
        reply_text(event.reply_token, "已清空，重新開始。輸入「甜點」開啟選單。")
        return

    if text in ["甜點", "我要下單", "選單"]:
        show_product_menu(user_id, reply_token=event.reply_token)
        return

    if text in ["取貨說明"]:
        reply_text(event.reply_token, PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE)
        return

    if text in ["付款說明"]:
        reply_text(event.reply_token, BANK_TRANSFER_TEXT)
        return

    # ---------- checkout entry ----------
    if text == "前往結帳":
        if not sess["cart"]:
            reply_text(event.reply_token, "你的購物車是空的，先輸入「甜點」選商品。")
            return
        reply_text(event.reply_token, "好，接著選取貨方式。")
        reply_flex_json(event.reply_token, "取貨方式", build_pickup_method_flex())
        sess["state"] = "WAIT_PICKUP_METHOD"
        return

    # ---------- item selection ----------
    if text.startswith("ITEM:"):
        item_key = text.split(":", 1)[1].strip()
        if item_key not in ITEMS:
            reply_text(event.reply_token, "品項不存在，請重新輸入「甜點」。")
            return
        sess["pending_item"] = item_key
        sess["pending_flavor"] = None

        if ITEMS[item_key]["has_flavor"]:
            reply_text(event.reply_token, f"你選了：{ITEMS[item_key]['label']}，請選口味。")
            ask_flavor(user_id, item_key)
            sess["state"] = "WAIT_FLAVOR"
        else:
            reply_text(event.reply_token, f"你選了：{ITEMS[item_key]['label']}，請選數量。")
            ask_qty(user_id, item_key)
            sess["state"] = "WAIT_QTY"
        return

    # ---------- flavor ----------
    if text.startswith("FLAVOR:"):
        flavor = text.split(":", 1)[1].strip()
        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            reply_text(event.reply_token, "流程有點亂掉了，請輸入「甜點」重新開始。")
            return

        if flavor not in ITEMS[item_key]["flavors"]:
            reply_text(event.reply_token, "口味不正確，請重新選口味。")
            ask_flavor(user_id, item_key)
            return

        sess["pending_flavor"] = flavor
        reply_text(event.reply_token, f"口味：{flavor}\n請選數量。")
        ask_qty(user_id, item_key)
        sess["state"] = "WAIT_QTY"
        return

    # ---------- qty ----------
    if text.startswith("QTY:"):
        qty_raw = text.split(":", 1)[1].strip()
        try:
            qty = int(qty_raw)
        except Exception:
            reply_text(event.reply_token, "數量格式錯誤，請重新選數量。")
            return

        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            reply_text(event.reply_token, "流程有點亂掉了，請輸入「甜點」重新開始。")
            return

        flavor = sess.get("pending_flavor")

        try:
            add_to_cart(user_id, item_key, flavor, qty)
        except Exception as e:
            reply_text(event.reply_token, f"加入失敗：{e}\n請重新選擇。輸入「甜點」開始。")
            return

        # 清 pending
        sess["pending_item"] = None
        sess["pending_flavor"] = None
        sess["state"] = "IDLE"

        # 回覆加入購物車 + 小計
        meta = ITEMS[item_key]
        name = meta["label"] + (f"（{flavor}）" if flavor else "")
        subtotal = meta["unit_price"] * qty
        total = cart_total(sess["cart"])

        reply_text(
            event.reply_token,
            "✅ 已加入購物車\n"
            f"- {name} x{qty} = {subtotal}\n\n"
            f"目前小計：{total}"
        )

        # 重要：加購 / 結帳選擇（用 push，避免跟 reply token 混亂）
        after_added_actions(user_id)
        return

    # ---------- pickup method ----------
    if text.startswith("取貨:"):
        method = text.split(":", 1)[1].strip()
        if method not in ["店取", "宅配"]:
            reply_text(event.reply_token, "取貨方式不正確，請重新選擇。")
            reply_flex_json(event.reply_token, "取貨方式", build_pickup_method_flex())
            return

        sess["pickup_method"] = method

        if method == "店取":
            sess["state"] = "WAIT_PICKUP_DATE"
            reply_text(
                event.reply_token,
                "店取：請輸入希望取貨日期（YYYY-MM-DD）\n"
                "只能選 3～14 天內（甜點需提前 3 天預訂）。"
            )
            return

        if method == "宅配":
            sess["state"] = "WAIT_DELIVERY_DATE"
            reply_text(
                event.reply_token,
                "宅配：請輸入「希望到貨日期」（YYYY-MM-DD）\n"
                "只能選 3～14 天內（不保證準時到貨，僅作為希望日）。"
            )
            return

    # ---------- date input ----------
    if sess["state"] in ["WAIT_PICKUP_DATE", "WAIT_DELIVERY_DATE"]:
        dt = parse_date_yyyy_mm_dd(text)
        if not dt:
            reply_text(event.reply_token, "日期格式請用 YYYY-MM-DD，例如 2026-01-15")
            return

        if not date_in_range_3_to_14_days(dt):
            reply_text(event.reply_token, "日期需在 3～14 天內，請重新輸入 YYYY-MM-DD")
            return

        if sess["state"] == "WAIT_PICKUP_DATE":
            sess["pickup_date"] = dt.strftime("%Y-%m-%d")
            sess["state"] = "WAIT_PICKUP_TIME"
            push_quick_reply(
                user_id,
                "請選店取時段：",
                [
                    {"label": "10:00-12:00", "text": "時段:10:00-12:00"},
                    {"label": "12:00-14:00", "text": "時段:12:00-14:00"},
                    {"label": "14:00-16:00", "text": "時段:14:00-16:00"},
                ],
            )
            reply_text(event.reply_token, "已收到取貨日期，請選時段（上方按鈕）。")
            return

        if sess["state"] == "WAIT_DELIVERY_DATE":
            sess["delivery_date"] = dt.strftime("%Y-%m-%d")
            sess["state"] = "WAIT_DELIVERY_NAME"
            reply_text(event.reply_token, "請輸入宅配收件人姓名：")
            return

    # ---------- pickup time ----------
    if text.startswith("時段:") and sess["state"] == "WAIT_PICKUP_TIME":
        t = text.split(":", 1)[1].strip()
        sess["pickup_time"] = t
        sess["state"] = "WAIT_PICKUP_NAME"
        reply_text(event.reply_token, "請輸入店取取件人姓名：")
        return

    # ---------- pickup name ----------
    if sess["state"] == "WAIT_PICKUP_NAME":
        sess["pickup_name"] = text
        sess["state"] = "CONFIRM"
        order_id = create_order_and_write_sheet(user_id, display_name)

        total = cart_total(sess["cart"])
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
        # 訂單完成後清空購物車（避免重複）
        sess["cart"] = []
        sess["state"] = "IDLE"
        return

    # ---------- delivery name/phone/address ----------
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
        sess["state"] = "CONFIRM"
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

    # ---------- payment confirm message (optional) ----------
    if text.startswith("已轉帳"):
        reply_text(
            event.reply_token,
            "收到，我們會核對帳款後依訂單號安排出貨。\n"
            "若需補充資訊，也可以直接在這裡留言。"
        )
        return

    # ---------- fallback ----------
    reply_text(
        event.reply_token,
        "請輸入「甜點」開啟選單。\n"
        "或輸入：取貨說明 / 付款說明"
    )
