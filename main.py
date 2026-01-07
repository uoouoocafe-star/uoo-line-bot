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
# Config / Env
# =========================
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "").strip()
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "").strip()

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

# 你說一直都是 orders：這裡直接以 orders 為預設
SHEET_NAME = os.getenv("SHEET_NAME", "orders").strip()

TZ = timezone(timedelta(hours=8))  # Asia/Taipei

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
            "cart": [],  # list of items {key,label,flavor,qty,unit,subtotal}
            "state": "IDLE",
            "pending_item": None,
            "pending_flavor": None,
            "pickup_method": None,   # 店取 / 宅配
            "pickup_date": None,
            "pickup_time": None,
            "pickup_name": None,
            "delivery_date": None,   # 希望到貨日期
            "delivery_name": None,
            "delivery_phone": None,
            "delivery_address": None,
        }
    return SESSIONS[user_id]


# =========================
# Menu data
# =========================
FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

ITEMS = {
    "dacquoise": {"label": "達克瓦茲", "unit_price": 95, "has_flavor": True, "flavors": FLAVORS, "min_qty": 2, "step": 1},
    "scone":     {"label": "原味司康", "unit_price": 65, "has_flavor": False, "flavors": [],     "min_qty": 1, "step": 1},
    # ✅ 可麗露：六顆/盒 490，只能一盒一盒買（qty=盒數）
    "canele_box":{"label": "可麗露六入/盒", "unit_price": 490, "has_flavor": False, "flavors": [], "min_qty": 1, "step": 1},
    "toast":     {"label": "伊思尼奶酥厚片", "unit_price": 85, "has_flavor": True, "flavors": FLAVORS, "min_qty": 1, "step": 1},
}

MAX_QTY_DEFAULT = 12
MAX_BOX_QTY = 10  # 可麗露盒最多給 10 盒（你說 10 張，這裡也符合）


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
        # ✅ 注意：不要加引號，不要 sheet1，直接用 orders
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


def format_mmdd_weekday(dt: datetime) -> str:
    wk = "一二三四五六日"[dt.weekday()]
    return f"{dt.month}/{dt.day}（{wk}）"


def build_date_options_10() -> List[Tuple[str, str]]:
    # +3 天起，連續 10 天
    today0 = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    options = []
    for i in range(3, 13):  # 3..12 共 10 個
        d = today0 + timedelta(days=i)
        value = d.strftime("%Y-%m-%d")
        label = format_mmdd_weekday(d)
        options.append((label, value))
    return options


def safe_is_valid_flex(contents: Any) -> bool:
    return isinstance(contents, dict) and bool(contents) and bool(contents.get("type"))


def safe_reply(reply_token: str, messages: List[Any]):
    # messages 可混：TextMessage / FlexMessage / dict
    try:
        messaging_api.reply_message(
            ReplyMessageRequest(
                replyToken=reply_token,
                messages=messages,
            )
        )
    except Exception as e:
        print("[ERROR] reply failed:", e)


def safe_reply_flex(reply_token: str, alt_text: str, flex_content: dict, fallback_text: str = "系統忙碌中，請再按一次或輸入：我要下單 / 甜點"):
    safe_alt = (alt_text or "").strip() or "訊息"
    if not safe_is_valid_flex(flex_content):
        safe_reply(reply_token, [TextMessage(text=fallback_text)])
        return
    safe_reply(reply_token, [FlexMessage(altText=safe_alt, contents=flex_content)])


def safe_push(user_id: str, messages: List[Any]):
    try:
        messaging_api.push_message(
            PushMessageRequest(
                to=user_id,
                messages=messages,
            )
        )
    except Exception as e:
        print("[ERROR] push failed:", e)


def parse_postback_data(data: str) -> Dict[str, str]:
    # data: "act=item&key=dacquoise"
    qs = parse_qs(data or "")
    return {k: (v[0] if v else "") for k, v in qs.items()}


def find_cart_index(cart: List[dict], idx_str: str) -> Optional[int]:
    try:
        i = int(idx_str)
        if 0 <= i < len(cart):
            return i
        return None
    except Exception:
        return None


# =========================
# Flex Builders (全部用 postback + displayText，避免顯示程式碼)
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
            "displayText": display_text,  # ✅ 顯示給使用者看的文字（不會出現程式碼）
        },
    }


def build_menu_flex() -> dict:
    body_contents = [
        {"type": "text", "text": "請選擇商品", "weight": "bold", "size": "xl"},
        {"type": "text", "text": "（全部甜點需提前 3 天預訂）", "size": "sm", "color": "#666666"},
        flex_button_postback("達克瓦茲｜NT$95", "act=item&key=dacquoise", "達克瓦茲"),
        flex_button_postback("原味司康｜NT$65", "act=item&key=scone", "原味司康"),
        flex_button_postback("可麗露六入/盒｜NT$490", "act=item&key=canele_box", "可麗露六入/盒"),
        flex_button_postback("伊思尼奶酥厚片｜NT$85", "act=item&key=toast", "伊思尼奶酥厚片"),
        {"type": "separator", "margin": "lg"},
        flex_button_postback("🧾 前往結帳", "act=checkout", "前往結帳", style="secondary"),
        flex_button_postback("🗑 清空重來", "act=reset", "清空重來", style="secondary"),
    ]

    return {
        "type": "bubble",
        "size": "mega",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": body_contents},
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
                {"type": "text", "text": "日期將以按鈕選擇（不需手動輸入）", "size": "sm", "color": "#666666"},
                flex_button_postback("🏪 店取", "act=pickup&method=store", "店取"),
                flex_button_postback("🚚 冷凍宅配", "act=pickup&method=ship", "冷凍宅配"),
            ],
        },
    }


def build_time_slots_quickreply() -> dict:
    # quickReply 也用 postback + displayText，避免顯示內碼
    items = []
    for slot in ["10:00-12:00", "12:00-14:00", "14:00-16:00"]:
        items.append({
            "type": "action",
            "action": {
                "type": "postback",
                "label": slot,
                "data": f"act=time&v={slot}",
                "displayText": slot
            }
        })

    return {
        "type": "text",
        "text": "請選店取時段：",
        "quickReply": {"items": items}
    }


def build_date_quickreply(title: str, act_name: str) -> dict:
    # act_name: store_date / ship_date
    opts = build_date_options_10()
    items = []
    for label, value in opts:
        items.append({
            "type": "action",
            "action": {
                "type": "postback",
                "label": label,
                "data": f"act={act_name}&v={value}",
                "displayText": f"{title}：{label}",
            }
        })

    return {
        "type": "text",
        "text": f"{title}（3～14 天內，提供 10 個日期）",
        "quickReply": {"items": items}
    }


def build_flavor_quickreply() -> dict:
    items = []
    for f in FLAVORS:
        items.append({
            "type": "action",
            "action": {
                "type": "postback",
                "label": f,
                "data": f"act=flavor&v={f}",
                "displayText": f,
            }
        })
    return {
        "type": "text",
        "text": "請選口味：",
        "quickReply": {"items": items}
    }


def build_qty_quickreply(item_key: str) -> dict:
    meta = ITEMS[item_key]
    min_qty = meta["min_qty"]

    # 可麗露盒：1~10 盒
    max_qty = MAX_BOX_QTY if item_key == "canele_box" else MAX_QTY_DEFAULT

    items = []
    for i in range(min_qty, max_qty + 1):
        items.append({
            "type": "action",
            "action": {
                "type": "postback",
                "label": str(i),
                "data": f"act=qty&v={i}",
                "displayText": str(i),
            }
        })

    return {
        "type": "text",
        "text": f"請選數量（最少 {min_qty}）：",
        "quickReply": {"items": items}
    }


def build_checkout_summary_flex(sess: dict) -> dict:
    cart = sess.get("cart", [])
    total = cart_total(cart)
    fee = shipping_fee(total) if sess.get("pickup_method") == "宅配" else 0
    grand = total + fee

    # 上方：取貨資訊
    info_lines = []
    if sess.get("pickup_method") == "店取":
        info_lines = [
            f"📅 日期：{sess.get('pickup_date')}",
            f"🕒 時段：{sess.get('pickup_time')}",
            f"📍 地址：{PICKUP_ADDRESS}",
        ]
    elif sess.get("pickup_method") == "宅配":
        info_lines = [
            f"📅 希望到貨：{sess.get('delivery_date')}（不保證準時）",
            "🚚 冷凍宅配",
        ]
    else:
        info_lines = ["（尚未選取貨資訊）"]

    # ✅ 清單（小計前）
    list_boxes = []
    for idx, it in enumerate(cart):
        name = it["label"] + (f"（{it['flavor']}）" if it.get("flavor") else "")
        qty = it["qty"]
        subtotal = it["subtotal"]

        # 每個品項：➖ ➕
        row = {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "margin": "md",
            "contents": [
                {"type": "text", "text": f"{name}", "wrap": True, "weight": "bold", "size": "sm"},
                {"type": "text", "text": f"數量：{qty}｜小計：NT${subtotal}", "size": "sm", "color": "#666666"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "sm",
                    "contents": [
                        flex_button_postback("➖ 減少數量", f"act=cart_dec&idx={idx}", "減少數量", style="secondary", height="sm"),
                        flex_button_postback("➕ 增加數量", f"act=cart_inc&idx={idx}", "增加數量", style="secondary", height="sm"),
                    ],
                }
            ],
        }
        list_boxes.append(row)

    # 底部：小計區
    pay_lines = [f"目前小計：NT${total}"]
    if sess.get("pickup_method") == "宅配":
        pay_lines.append(f"運費：NT${fee}")
        pay_lines.append(f"應付總額：NT${grand}")

    body_contents = [
        {"type": "text", "text": "✅ 結帳確認", "weight": "bold", "size": "xl"},
        {"type": "text", "text": "\n".join(info_lines), "size": "sm", "wrap": True, "color": "#444444"},
        {"type": "separator", "margin": "lg"},
        {"type": "text", "text": "🧾 結帳內容清單", "weight": "bold", "size": "md", "margin": "md"},
    ]
    body_contents.extend(list_boxes)

    body_contents.extend([
        {"type": "separator", "margin": "lg"},
        {"type": "text", "text": "\n".join(pay_lines), "weight": "bold", "size": "lg", "margin": "md"},
        {"type": "text", "text": "下一步請填姓名，或可返回加購。", "size": "sm", "color": "#666666", "margin": "sm"},
        flex_button_postback("✍️ 填取件人/收件人姓名", "act=ask_name", "填寫姓名", style="primary"),
        flex_button_postback("＋ 繼續加購", "act=show_menu", "繼續加購", style="secondary"),
    ])

    return {
        "type": "bubble",
        "size": "mega",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": body_contents},
    }


# =========================
# Business logic
# =========================
def reset_session(sess: dict):
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


def add_to_cart(sess: dict, item_key: str, flavor: Optional[str], qty: int):
    meta = ITEMS[item_key]
    if meta["has_flavor"] and not flavor:
        raise ValueError("缺少口味")
    if qty < meta["min_qty"]:
        raise ValueError(f"數量需 >= {meta['min_qty']}")

    # 可麗露盒 qty = 盒數（每盒六顆）
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


def recalc_cart(sess: dict):
    for it in sess["cart"]:
        it["subtotal"] = int(it["unit_price"]) * int(it["qty"])


def change_cart_qty(sess: dict, idx: int, delta: int) -> bool:
    cart = sess.get("cart", [])
    if not (0 <= idx < len(cart)):
        return False

    it = cart[idx]
    meta = ITEMS.get(it["item_key"])
    if not meta:
        return False

    new_qty = int(it["qty"]) + delta
    if new_qty < meta["min_qty"]:
        # 小於最小量：直接刪除該品項
        cart.pop(idx)
        return True

    # 上限：可麗露盒最多 10
    if it["item_key"] == "canele_box":
        new_qty = min(new_qty, MAX_BOX_QTY)
    else:
        new_qty = min(new_qty, MAX_QTY_DEFAULT)

    it["qty"] = new_qty
    recalc_cart(sess)
    return True


def create_order_and_write_sheet(user_id: str, display_name: str, sess: dict) -> str:
    cart = sess.get("cart", [])
    if not cart:
        return ""

    order_id = gen_order_id()
    total = cart_total(cart)

    pickup_method = sess.get("pickup_method", "")
    pickup_date = sess.get("pickup_date", "")
    pickup_time = sess.get("pickup_time", "")

    note = ""
    if pickup_method == "宅配":
        delivery_date = sess.get("delivery_date", "")
        dn = sess.get("delivery_name", "")
        dp = sess.get("delivery_phone", "")
        da = sess.get("delivery_address", "")
        note = f"希望到貨:{delivery_date} | 收件人:{dn} | 電話:{dp} | 地址:{da}"
        pickup_date = delivery_date
        pickup_time = ""

    if pickup_method == "店取":
        pn = sess.get("pickup_name", "")
        note = f"取件人:{pn}"

    row = [
        now_str(),  # created_at
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
# LINE Webhook Handlers
# =========================
@handler.add(MessageEvent, message=TextMessageContent)
def on_text(event: MessageEvent):
    user_id = event.source.user_id
    text = (event.message.text or "").strip()
    sess = get_session(user_id)

    # 你也可以之後改成抓 profile
    display_name = "LINE用戶"

    if text in ["清空重來", "清空", "reset"]:
        reset_session(sess)
        safe_reply(event.reply_token, [TextMessage(text="已清空，重新開始。輸入「我要下單」開始。")])
        return

    if text in ["甜點", "選單"]:
        # 只看菜單
        safe_reply_flex(event.reply_token, "甜點選單", build_menu_flex())
        return

    if text in ["我要下單", "下單", "開始下單"]:
        sess["state"] = "ORDERING"
        safe_reply_flex(event.reply_token, "開始下單", build_menu_flex())
        return

    if text in ["取貨說明"]:
        safe_reply(event.reply_token, [TextMessage(text=PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE)])
        return

    if text in ["付款說明"]:
        safe_reply(event.reply_token, [TextMessage(text=BANK_TRANSFER_TEXT)])
        return

    # 付款回報
    if text.startswith("已轉帳"):
        safe_reply(event.reply_token, [TextMessage(text="收到，我們會核對帳款後依訂單號安排出貨。")])
        return

    # fallback
    safe_reply(event.reply_token, [TextMessage(text="可輸入：「甜點」看菜單｜「我要下單」開始下單")])
    return


@handler.add(PostbackEvent)
def on_postback(event: PostbackEvent):
    user_id = event.source.user_id
    data = getattr(event.postback, "data", "") if getattr(event, "postback", None) else ""
    payload = parse_postback_data(data)
    act = payload.get("act", "")
    sess = get_session(user_id)

    # Debug（需要時打開）
    # print("[POSTBACK]", data, payload)

    # ===== reset/menu/checkout =====
    if act == "reset":
        reset_session(sess)
        safe_reply(event.reply_token, [TextMessage(text="已清空，重新開始。輸入「我要下單」開始。")])
        return

    if act == "show_menu":
        safe_reply_flex(event.reply_token, "甜點選單", build_menu_flex())
        return

    if act == "checkout":
        if not sess["cart"]:
            safe_reply(event.reply_token, [TextMessage(text="購物車是空的喔～先選甜點再結帳。")])
            return
        sess["state"] = "WAIT_PICKUP_METHOD"
        safe_reply_flex(event.reply_token, "取貨方式", build_pickup_method_flex())
        return

    # ===== cart modify =====
    if act in ["cart_inc", "cart_dec"]:
        idx = find_cart_index(sess["cart"], payload.get("idx", ""))
        if idx is None:
            safe_reply(event.reply_token, [TextMessage(text="找不到要修改的品項，請再試一次。")])
            return
        delta = +1 if act == "cart_inc" else -1
        change_cart_qty(sess, idx, delta)
        # 更新結帳卡片
        safe_reply_flex(event.reply_token, "結帳確認", build_checkout_summary_flex(sess))
        return

    # ===== pickup method =====
    if act == "pickup":
        method = payload.get("method", "")
        if method == "store":
            sess["pickup_method"] = "店取"
            sess["state"] = "WAIT_STORE_DATE"
            # ✅ 日期按鈕（10個）
            safe_reply(event.reply_token, [build_date_quickreply("🌿 店取日期", "store_date")])
            return

        if method == "ship":
            sess["pickup_method"] = "宅配"
            sess["state"] = "WAIT_SHIP_DATE"
            safe_reply(event.reply_token, [build_date_quickreply("🚚 宅配希望到貨日期", "ship_date")])
            return

        safe_reply(event.reply_token, [TextMessage(text="取貨方式有點怪怪的，請再選一次。")])
        safe_reply_flex(event.reply_token, "取貨方式", build_pickup_method_flex())
        return

    # ===== date selection =====
    if act == "store_date":
        v = payload.get("v", "")
        sess["pickup_date"] = v
        sess["state"] = "WAIT_STORE_TIME"
        # 店取時段按鈕
        safe_reply(event.reply_token, [
            TextMessage(text=f"✅ 已選店取日期：{v}\n請選店取時段（下方按鈕）。"),
            build_time_slots_quickreply()
        ])
        return

    if act == "ship_date":
        v = payload.get("v", "")
        sess["delivery_date"] = v
        sess["state"] = "WAIT_DELIVERY_NAME"
        safe_reply(event.reply_token, [TextMessage(text=f"✅ 已選希望到貨日期：{v}\n請輸入收件人姓名：")])
        return

    if act == "time":
        v = payload.get("v", "")
        sess["pickup_time"] = v
        sess["state"] = "READY_TO_NAME"
        # 先給結帳確認卡（可修改品項）
        safe_reply_flex(event.reply_token, "結帳確認", build_checkout_summary_flex(sess))
        return

    # ===== ask name from checkout summary =====
    if act == "ask_name":
        if sess.get("pickup_method") == "店取":
            sess["state"] = "WAIT_PICKUP_NAME"
            safe_reply(event.reply_token, [TextMessage(text="請輸入店取取件人姓名：")])
            return
        if sess.get("pickup_method") == "宅配":
            sess["state"] = "WAIT_DELIVERY_NAME"
            safe_reply(event.reply_token, [TextMessage(text="請輸入宅配收件人姓名：")])
            return
        safe_reply(event.reply_token, [TextMessage(text="請先完成取貨方式與日期選擇喔～")])
        safe_reply_flex(event.reply_token, "取貨方式", build_pickup_method_flex())
        return

    # ===== item selection =====
    if act == "item":
        item_key = payload.get("key", "")
        if item_key not in ITEMS:
            safe_reply(event.reply_token, [TextMessage(text="這個品項不存在，請重新選擇。")])
            safe_reply_flex(event.reply_token, "甜點選單", build_menu_flex())
            return

        # 如果使用者只是看菜單，也允許直接選（不強迫先打我要下單）
        sess["state"] = "ORDERING"

        sess["pending_item"] = item_key
        sess["pending_flavor"] = None

        meta = ITEMS[item_key]
        if meta["has_flavor"]:
            sess["state"] = "WAIT_FLAVOR"
            safe_reply(event.reply_token, [
                TextMessage(text=f"你選了：{meta['label']}\n請選口味（下方按鈕）"),
                build_flavor_quickreply()
            ])
            return

        sess["state"] = "WAIT_QTY"
        safe_reply(event.reply_token, [
            TextMessage(text=f"你選了：{meta['label']}\n請選數量（下方按鈕）"),
            build_qty_quickreply(item_key)
        ])
        return

    if act == "flavor":
        flavor = payload.get("v", "")
        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            safe_reply(event.reply_token, [TextMessage(text="流程有點亂掉了，請再選一次甜點。")])
            safe_reply_flex(event.reply_token, "甜點選單", build_menu_flex())
            return

        if flavor not in ITEMS[item_key]["flavors"]:
            safe_reply(event.reply_token, [TextMessage(text="口味不正確，請重新選。")])
            safe_reply(event.reply_token, [build_flavor_quickreply()])
            return

        sess["pending_flavor"] = flavor
        sess["state"] = "WAIT_QTY"
        safe_reply(event.reply_token, [
            TextMessage(text=f"✅ 已選口味：{flavor}\n請選數量（下方按鈕）"),
            build_qty_quickreply(item_key)
        ])
        return

    if act == "qty":
        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            safe_reply(event.reply_token, [TextMessage(text="流程有點亂掉了，請再選一次甜點。")])
            safe_reply_flex(event.reply_token, "甜點選單", build_menu_flex())
            return

        try:
            qty = int(payload.get("v", "0"))
        except Exception:
            safe_reply(event.reply_token, [TextMessage(text="數量格式錯誤，請重新選。")])
            safe_reply(event.reply_token, [build_qty_quickreply(item_key)])
            return

        flavor = sess.get("pending_flavor")
        try:
            add_to_cart(sess, item_key, flavor, qty)
        except Exception as e:
            safe_reply(event.reply_token, [TextMessage(text=f"加入失敗：{e}\n請重新選擇。")])
            safe_reply_flex(event.reply_token, "甜點選單", build_menu_flex())
            return

        # 清 pending
        sess["pending_item"] = None
        sess["pending_flavor"] = None
        sess["state"] = "ORDERING"

        meta = ITEMS[item_key]
        name = meta["label"] + (f"（{flavor}）" if flavor else "")
        subtotal = meta["unit_price"] * qty
        total = cart_total(sess["cart"])

        # 加購/結帳按鈕（不顯示內碼）
        next_step_flex = {
            "type": "bubble",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "contents": [
                    {"type": "text", "text": "✅ 已加入購物車", "weight": "bold", "size": "xl"},
                    {"type": "text", "text": f"{name} x{qty} = NT${subtotal}", "wrap": True},
                    {"type": "text", "text": f"目前小計：NT${total}", "weight": "bold", "size": "lg"},
                    {"type": "separator", "margin": "lg"},
                    flex_button_postback("＋ 繼續加購", "act=show_menu", "繼續加購", style="secondary"),
                    flex_button_postback("🧾 前往結帳", "act=checkout", "前往結帳", style="primary"),
                ],
            }
        }

        safe_reply_flex(event.reply_token, "已加入購物車", next_step_flex)
        return

    # ===== fallback =====
    safe_reply(event.reply_token, [TextMessage(text="我沒有看懂這個按鈕指令，請輸入「甜點」或「我要下單」。")])
    return


# =========================
# 文字輸入：姓名/地址等（少數必填還是要輸入）
# =========================
@handler.add(MessageEvent, message=TextMessageContent)
def on_text_stateful(event: MessageEvent):
    # 這個 handler 會跟上面 on_text 同時觸發；為了避免重複，
    # 我們只在「需要填資料的 state」才處理，其他直接 return
    user_id = event.source.user_id
    text = (event.message.text or "").strip()
    sess = get_session(user_id)

    display_name = "LINE用戶"

    # 只處理需要填寫的 state
    if sess.get("state") == "WAIT_PICKUP_NAME":
        sess["pickup_name"] = text
        # 建立訂單
        order_id = create_order_and_write_sheet(user_id, display_name, sess)
        summary = build_order_result_text_store(order_id, sess)
        reset_session(sess)
        safe_reply(event.reply_token, [TextMessage(text=summary)])
        return

    if sess.get("state") == "WAIT_DELIVERY_NAME":
        sess["delivery_name"] = text
        sess["state"] = "WAIT_DELIVERY_PHONE"
        safe_reply(event.reply_token, [TextMessage(text="請輸入宅配電話：")])
        return

    if sess.get("state") == "WAIT_DELIVERY_PHONE":
        sess["delivery_phone"] = text
        sess["state"] = "WAIT_DELIVERY_ADDRESS"
        safe_reply(event.reply_token, [TextMessage(text="請輸入宅配地址（完整地址）：")])
        return

    if sess.get("state") == "WAIT_DELIVERY_ADDRESS":
        sess["delivery_address"] = text
        order_id = create_order_and_write_sheet(user_id, display_name, sess)
        summary = build_order_result_text_ship(order_id, sess)
        reset_session(sess)
        safe_reply(event.reply_token, [TextMessage(text=summary)])
        return

    # 其他狀態不處理（交給前面 on_text）
    return


def build_cart_lines(sess: dict) -> str:
    lines = []
    for it in sess.get("cart", []):
        name = it["label"] + (f"（{it['flavor']}）" if it.get("flavor") else "")
        lines.append(f"- {name} x{it['qty']} = NT${it['subtotal']}")
    return "\n".join(lines) if lines else "（無）"


def build_order_result_text_store(order_id: str, sess: dict) -> str:
    total = cart_total(sess.get("cart", []))
    cart_lines = build_cart_lines(sess)
    return (
        "✅ 訂單已建立\n"
        f"訂單編號：{order_id}\n\n"
        "🧾 訂單內容：\n"
        f"{cart_lines}\n\n"
        f"目前小計：NT${total}\n\n"
        "🏪 店取資訊：\n"
        f"日期：{sess.get('pickup_date')}\n"
        f"時段：{sess.get('pickup_time')}\n"
        f"地址：{PICKUP_ADDRESS}\n\n"
        + BANK_TRANSFER_TEXT
    )


def build_order_result_text_ship(order_id: str, sess: dict) -> str:
    total = cart_total(sess.get("cart", []))
    fee = shipping_fee(total)
    grand = total + fee
    cart_lines = build_cart_lines(sess)
    return (
        "✅ 訂單已建立\n"
        f"訂單編號：{order_id}\n\n"
        "🧾 訂單內容：\n"
        f"{cart_lines}\n\n"
        f"目前小計：NT${total}\n"
        f"運費：NT${fee}\n"
        f"應付總額：NT${grand}\n\n"
        "🚚 宅配資訊：\n"
        f"希望到貨：{sess.get('delivery_date')}（不保證準時）\n"
        f"收件人：{sess.get('delivery_name')}\n"
        f"電話：{sess.get('delivery_phone')}\n"
        f"地址：{sess.get('delivery_address')}\n\n"
        + DELIVERY_NOTICE
        + "\n\n"
        + BANK_TRANSFER_TEXT
    )
