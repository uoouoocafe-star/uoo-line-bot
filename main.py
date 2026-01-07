import os
import json
import base64
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List

from fastapi import FastAPI, Request, HTTPException

from linebot.v3 import WebhookHandler
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
    FlexMessage,
    FlexContainer,
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent, PostbackEvent

from google.oauth2 import service_account
from googleapiclient.discovery import build

# =========================
# ENV
# =========================
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "")

GSHEET_ID = os.getenv("GSHEET_ID", "")
GSHEET_SHEET_NAME = os.getenv("GSHEET_SHEET_NAME", "sheet1")
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    raise RuntimeError("Missing CHANNEL_ACCESS_TOKEN / CHANNEL_SECRET")

TZ_TAIPEI = timezone(timedelta(hours=8))

# =========================
# BUSINESS
# =========================
STORE_ADDRESS = "新竹縣竹北市隘口六街65號"
BANK_TEXT = "台灣銀行：004 248-001-03430-6"

SHIPPING_FEE = 180
FREE_SHIPPING_THRESHOLD = 2500

ITEMS = {
    "dacquoise": {
        "label": "達克瓦茲",
        "unit_price": 95,
        "has_flavor": True,
        "min_qty": 2,
        "flavors": [
            ("original", "原味"),
            ("black_tea", "蜜香紅茶"),
            ("matcha", "日式抹茶"),
            ("hojicha", "日式焙茶"),
            ("cocoa", "法芙娜可可"),
        ],
        "note": "口味不可混（同一張訂單只能選一種口味）",
    },
    "scone": {
        "label": "原味司康",
        "unit_price": 65,
        "has_flavor": False,
        "min_qty": 1,
        "flavors": [],
        "note": "冷藏或冷凍保存",
    },
    "canele": {
        "label": "原味可麗露",
        "unit_price": 90,
        "has_flavor": False,
        "min_qty": 1,
        "flavors": [],
        "note": "限冷凍保存",
    },
    "toast": {
        "label": "伊思尼奶酥厚片",
        "unit_price": 85,
        "has_flavor": True,
        "min_qty": 1,
        "flavors": [
            ("original", "原味"),
            ("black_tea", "蜜香紅茶"),
            ("matcha", "日式抹茶"),
            ("hojicha", "日式焙茶"),
            ("cocoa", "法芙娜可可"),
        ],
        "note": "冷藏或冷凍保存",
    },
}

PICKUP_TIME_SLOTS = [
    "10:00-12:00",
    "12:00-14:00",
    "14:00-16:00",
    "16:00-18:00",
]

# =========================
# STATE (in-memory)
# =========================
STATE: Dict[str, Dict[str, Any]] = {}
# stages:
# idle
# await_delivery_name -> await_delivery_phone -> await_delivery_address
# await_store_pickup_name
# other picking via postback

def get_user_state(user_id: str) -> Dict[str, Any]:
    if user_id not in STATE:
        STATE[user_id] = {
            "stage": "idle",
            "cart": [],
            "dacquoise_flavor_locked": None,
            "pickup_method": None,   # store|delivery
            "pickup_date": None,     # YYYY-MM-DD
            "pickup_time": None,     # slot
            "note": "",              # for sheet (human readable)
            "delivery": {"name": "", "phone": "", "address": ""},
            "store": {"name": ""},
        }
    return STATE[user_id]


def reset_order(user_id: str) -> None:
    STATE[user_id] = {
        "stage": "idle",
        "cart": [],
        "dacquoise_flavor_locked": None,
        "pickup_method": None,
        "pickup_date": None,
        "pickup_time": None,
        "note": "",
        "delivery": {"name": "", "phone": "", "address": ""},
        "store": {"name": ""},
    }

# =========================
# Google Sheet
# =========================
def _load_service_account_info() -> Optional[dict]:
    if GOOGLE_SERVICE_ACCOUNT_JSON.strip():
        try:
            return json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        except Exception:
            return None
    if GOOGLE_SERVICE_ACCOUNT_B64.strip():
        try:
            raw = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64).decode("utf-8")
            return json.loads(raw)
        except Exception:
            return None
    return None


def append_order_row(row: List[Any]) -> None:
    if not GSHEET_ID:
        return
    info = _load_service_account_info()
    if not info:
        return

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)

    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    sheet = service.spreadsheets()

    rng = f"{GSHEET_SHEET_NAME}!A:Z"
    body = {"values": [row]}
    sheet.values().append(
        spreadsheetId=GSHEET_ID,
        range=rng,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()

# =========================
# LINE
# =========================
configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)
app = FastAPI()

def reply_text(reply_token: str, text: str):
    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[TextMessage(text=text)],
            )
        )

def reply_flex(reply_token: str, alt_text: str, flex_obj: dict):
    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[
                    FlexMessage(
                        alt_text=alt_text,
                        contents=FlexContainer.from_dict(flex_obj),
                    )
                ],
            )
        )

def safe_parse_postback(data: str) -> Dict[str, str]:
    out = {}
    for part in data.split("&"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k] = v
    return out

def generate_order_id() -> str:
    now = datetime.now(TZ_TAIPEI)
    ymd = now.strftime("%Y%m%d")
    suffix = random.randint(1000, 9999)
    return f"UOO-{ymd}-{suffix}"

def lock_dacquoise_rule(st: Dict[str, Any], flavor_key: str) -> bool:
    locked = st.get("dacquoise_flavor_locked")
    if locked is None:
        st["dacquoise_flavor_locked"] = flavor_key
        return True
    return locked == flavor_key

# =========================
# FLEX BUILDERS
# =========================
def build_main_menu_flex() -> dict:
    return {
        "type": "bubble",
        "size": "mega",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "UooUoo 甜點下單", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "請選擇服務：", "size": "sm", "color": "#666666"},
                {
                    "type": "button",
                    "style": "primary",
                    "action": {"type": "postback", "label": "我要下單", "data": "action=start_order", "displayText": "我要下單"},
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "action": {"type": "postback", "label": "取貨說明", "data": "action=pickup_info", "displayText": "取貨說明"},
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "action": {"type": "postback", "label": "付款說明", "data": "action=pay_info", "displayText": "付款說明"},
                },
            ],
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "contents": [{"type": "text", "text": "提醒：全部甜點需提前 3 天預訂", "size": "xs", "color": "#999999"}],
        },
    }

def build_item_picker_flex() -> dict:
    buttons = []
    for key in ["dacquoise", "scone", "canele", "toast"]:
        item = ITEMS[key]
        buttons.append(
            {
                "type": "button",
                "style": "secondary",
                "action": {"type": "postback", "label": f"{item['label']}｜NT${item['unit_price']}",
                           "data": f"action=choose_item&item={key}", "displayText": item["label"]},
            }
        )
    buttons += [
        {"type": "button", "style": "primary", "action": {"type": "postback", "label": "前往結帳", "data": "action=checkout", "displayText": "前往結帳"}},
        {"type": "button", "style": "link", "action": {"type": "postback", "label": "清空重來", "data": "action=reset", "displayText": "清空重來"}},
    ]
    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "請選擇商品", "weight": "bold", "size": "xl"},
            {"type": "text", "text": "（全部甜點需提前 3 天預訂）", "size": "sm", "color": "#666666"},
            {"type": "separator"},
            *buttons,
        ]},
    }

def build_flavor_picker_flex(item_key: str) -> dict:
    item = ITEMS[item_key]
    contents = [{"type": "text", "text": f"{item['label']}：請選口味", "weight": "bold", "size": "xl"}]
    if item_key == "dacquoise":
        contents.append({"type": "text", "text": "達克瓦茲口味不可混（同一張訂單只能一種口味）", "size": "sm", "color": "#C04A3A"})

    btns = []
    for f_key, f_label in item["flavors"]:
        btns.append({"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": f_label,
                                "data": f"action=choose_flavor&item={item_key}&flavor={f_key}",
                                "displayText": f_label}})
    btns.append({"type": "button", "style": "link", "action": {"type": "postback", "label": "回上一頁", "data": "action=back_to_items", "displayText": "回上一頁"}})

    return {"type": "bubble", "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": contents + [{"type": "separator"}, *btns]}}

def build_qty_picker_flex(item_key: str, flavor_key: Optional[str]) -> dict:
    item = ITEMS[item_key]
    title = item["label"]
    flavor_label = None
    if flavor_key:
        flavor_label = next((lab for k, lab in item["flavors"] if k == flavor_key), flavor_key)
        title = f"{title}（{flavor_label}）"

    min_qty = item["min_qty"]
    btns = []
    for q in range(min_qty, 11):
        btns.append({"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": f"{q}",
                                "data": f"action=choose_qty&item={item_key}&flavor={flavor_key or ''}&qty={q}",
                                "displayText": f"{q} 個"}})

    btns.append({"type": "button", "style": "link", "action": {"type": "postback", "label": "回上一頁", "data": f"action=back&item={item_key}", "displayText": "回上一頁"}})

    return {"type": "bubble", "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
        {"type": "text", "text": "請選數量", "weight": "bold", "size": "xl"},
        {"type": "text", "text": title, "size": "sm", "color": "#666666"},
        {"type": "separator"},
        *btns,
    ]}}

def cart_summary_text(st: Dict[str, Any]) -> str:
    if not st["cart"]:
        return "購物車目前是空的。"
    lines = ["✅ 已加入購物車："]
    total = 0
    for it in st["cart"]:
        name = it["label"]
        if it.get("flavor_label"):
            name = f"{name}（{it['flavor_label']}）"
        lines.append(f"- {name} x{it['qty']} = {it['subtotal']}")
        total += it["subtotal"]
    lines.append(f"\n目前小計：{total}")
    lines.append("\n你可以繼續選商品，或按「前往結帳」。")
    return "\n".join(lines)

def build_delivery_method_flex() -> dict:
    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "請選擇取貨方式", "weight": "bold", "size": "xl"},
            {"type": "separator"},
            {"type": "button", "style": "primary",
             "action": {"type": "postback", "label": "店取（竹北）", "data": "action=pickup_method&method=store", "displayText": "店取"}},
            {"type": "button", "style": "secondary",
             "action": {"type": "postback", "label": "宅配（冷凍）", "data": "action=pickup_method&method=delivery", "displayText": "宅配"}},
        ]},
        "footer": {"type": "box", "layout": "vertical", "contents": [
            {"type": "text", "text": "宅配：大榮冷凍運費 NT$180｜滿 2500 免運", "size": "xs", "color": "#999999"}
        ]},
    }

def build_pickup_date_flex(method: str) -> dict:
    today = datetime.now(TZ_TAIPEI).date()
    start = today + timedelta(days=3)
    end = today + timedelta(days=14)

    title = "希望取貨日期" if method == "store" else "希望到貨日期（不保證準時）"
    sub = "可選 3 天後～14 天內（實際依出貨/物流安排為準）"

    buttons = []
    d = start
    while d <= end:
        label = d.strftime("%m/%d (%a)")
        iso = d.isoformat()
        buttons.append({"type": "button", "style": "secondary",
                        "action": {"type": "postback", "label": label,
                                   "data": f"action=pickup_date&date={iso}",
                                   "displayText": f"{title} {label}"}})
        d += timedelta(days=1)

    return {"type": "bubble", "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
        {"type": "text", "text": title, "weight": "bold", "size": "xl"},
        {"type": "text", "text": sub, "size": "sm", "color": "#666666"},
        {"type": "separator"},
        *buttons
    ]}}

def build_pickup_time_flex() -> dict:
    btns = []
    for slot in PICKUP_TIME_SLOTS:
        btns.append({"type": "button", "style": "secondary",
                     "action": {"type": "postback", "label": slot, "data": f"action=pickup_time&time={slot}",
                                "displayText": f"取件時段 {slot}"}})
    return {"type": "bubble", "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
        {"type": "text", "text": "店取時段", "weight": "bold", "size": "xl"},
        {"type": "text", "text": f"地址：{STORE_ADDRESS}", "size": "sm", "color": "#666666"},
        {"type": "separator"},
        *btns
    ]}}

def build_checkout_confirm_flex(st: Dict[str, Any]) -> dict:
    subtotal = sum(it["subtotal"] for it in st["cart"])
    shipping = 0
    if st["pickup_method"] == "delivery":
        shipping = 0 if subtotal >= FREE_SHIPPING_THRESHOLD else SHIPPING_FEE
    total = subtotal + shipping

    items_lines = []
    for it in st["cart"]:
        name = it["label"]
        if it.get("flavor_label"):
            name = f"{name}（{it['flavor_label']}）"
        items_lines.append(f"{name} x{it['qty']}")
    detail = "\n".join(items_lines) if items_lines else "-"

    pm = "店取" if st["pickup_method"] == "store" else "宅配（冷凍）"
    date_label = "希望取貨日" if st["pickup_method"] == "store" else "希望到貨日（不保證準時）"

    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "結帳確認", "weight": "bold", "size": "xl"},
            {"type": "text", "text": "商品：", "size": "sm", "color": "#666666"},
            {"type": "text", "text": detail, "wrap": True},
            {"type": "separator"},
            {"type": "text", "text": f"取貨方式：{pm}", "wrap": True},
            {"type": "text", "text": f"{date_label}：{st['pickup_date'] or ''}", "wrap": True},
            *([{"type": "text", "text": f"店取時段：{st['pickup_time']}", "wrap": True}] if st["pickup_method"] == "store" else []),
            {"type": "text", "text": f"收件/取件資訊：{(st.get('note') or '-')[:200]}", "wrap": True, "size": "sm", "color": "#666666"},
            {"type": "separator"},
            {"type": "text", "text": f"小計：NT${subtotal}", "wrap": True},
            {"type": "text", "text": f"運費：NT${shipping}", "wrap": True},
            {"type": "text", "text": f"總計：NT${total}", "wrap": True, "weight": "bold"},
        ]},
        "footer": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": [
            {"type": "button", "style": "primary",
             "action": {"type": "postback", "label": "確認送出訂單", "data": "action=place_order", "displayText": "確認送出訂單"}},
            {"type": "button", "style": "link",
             "action": {"type": "postback", "label": "回去繼續加購", "data": "action=back_to_items", "displayText": "回去加購"}},
        ]},
    }

def build_payment_info_text() -> str:
    return "\n".join([
        "【付款方式】",
        "1) 轉帳匯款",
        f"- {BANK_TEXT}",
        "",
        "匯款後請回覆：",
        "「已轉帳 訂單編號 XXXXX 後五碼 12345」",
        "",
        "（我們核對後會依訂單編號陸續出貨/備貨）",
    ])

def build_pickup_info_text() -> str:
    return "\n".join([
        "【取貨方式】",
        f"店取：{STORE_ADDRESS}",
        "",
        "宅配：一律冷凍宅配（大榮）",
        "• 保持電話暢通：避免無人收件退件",
        "• 收到後立即開箱確認狀態，儘早冷藏/冷凍",
        "• 若嚴重損壞（糊爛、不成形）請拍照（含原箱）並當日聯繫店家",
        "• 未處理完前請保留原樣，勿丟棄/食用",
        "",
        "【風險認知】",
        "• 輕微位移、裝飾掉落通常不在理賠範圍，請理解並自行承擔",
        "• 天災可能導致物流延遲或暫停，無法保證準時送達",
    ])

# =========================
# FASTAPI
# =========================
@app.get("/")
def health():
    return {"ok": True}

@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    body_text = body.decode("utf-8")
    try:
        handler.handle(body_text, signature)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return "OK"

# =========================
# HANDLERS
# =========================
@handler.add(MessageEvent, message=TextMessageContent)
def on_message(event: MessageEvent):
    user_id = event.source.user_id
    text = (event.message.text or "").strip()
    st = get_user_state(user_id)

    # 入口
    if text in ["選單", "menu", "開始", "嗨", "hi", "你好"]:
        reply_flex(event.reply_token, "選單", build_main_menu_flex())
        return

    # 付款回報
    if text.startswith("已轉帳"):
        tokens = text.replace("：", " ").replace("，", " ").split()
        order_id = None
        last5 = None
        for i, t in enumerate(tokens):
            if t in ["訂單編號", "訂單", "order", "ORDER"] and i + 1 < len(tokens):
                order_id = tokens[i + 1]
            if t in ["後五碼", "後5碼", "末五碼"] and i + 1 < len(tokens):
                last5 = tokens[i + 1]

        if not order_id or not last5:
            reply_text(event.reply_token, "格式不完整。\n請回覆：已轉帳 訂單編號 XXXXX 後五碼 12345")
            return

        created_at = datetime.now(TZ_TAIPEI).strftime("%Y-%m-%d %H:%M:%S")
        row = [created_at, user_id, "(unknown)", order_id, "", "", "", "", f"PAY_REPORTED last5={last5}", "", "PAID_REPORTED", ""]
        append_order_row(row)

        reply_text(event.reply_token, f"收到你的轉帳回報✅\n訂單：{order_id}\n後五碼：{last5}\n\n我們核對後會依訂單編號安排出貨/備貨。")
        return

    # 宅配逐步表單
    if st["stage"] == "await_delivery_name":
        st["delivery"]["name"] = text
        st["stage"] = "await_delivery_phone"
        reply_text(event.reply_token, "收到✅ 請輸入「收件人電話」（例：0912xxxxxx）")
        return

    if st["stage"] == "await_delivery_phone":
        st["delivery"]["phone"] = text
        st["stage"] = "await_delivery_address"
        reply_text(event.reply_token, "收到✅ 請輸入「冷凍宅配地址」（請完整：縣市＋路名＋號＋樓層）")
        return

    if st["stage"] == "await_delivery_address":
        st["delivery"]["address"] = text
        st["note"] = f"收件人：{st['delivery']['name']}｜電話：{st['delivery']['phone']}｜地址：{st['delivery']['address']}"
        st["stage"] = "idle"
        reply_text(event.reply_token, "已收齊宅配資訊✅\n接著請選「希望到貨日期（不保證準時）」。")
        reply_flex(event.reply_token, "希望到貨日期", build_pickup_date_flex("delivery"))
        return

    # 店取取件人
    if st["stage"] == "await_store_pickup_name":
        st["store"]["name"] = text
        st["note"] = f"取件人：{st['store']['name']}"
        st["stage"] = "idle"
        reply_text(event.reply_token, "已收到取件人姓名✅\n接著請選「希望取貨日期」。")
        reply_flex(event.reply_token, "希望取貨日期", build_pickup_date_flex("store"))
        return

    # 其他文字
    reply_text(event.reply_token, "請輸入「選單」開啟按鈕。\n（我要下單 / 取貨說明 / 付款說明）")

@handler.add(PostbackEvent)
def on_postback(event: PostbackEvent):
    user_id = event.source.user_id
    st = get_user_state(user_id)

    q = safe_parse_postback(event.postback.data or "")
    action = q.get("action", "")

    if action == "pickup_info":
        reply_text(event.reply_token, build_pickup_info_text())
        return

    if action == "pay_info":
        reply_text(event.reply_token, build_payment_info_text())
        return

    if action == "start_order":
        reset_order(user_id)
        reply_text(event.reply_token, "好的，開始下單。先選商品（可多次加購）：")
        reply_flex(event.reply_token, "選商品", build_item_picker_flex())
        return

    if action == "reset":
        reset_order(user_id)
        reply_text(event.reply_token, "已清空✅ 重新開始下單。")
        reply_flex(event.reply_token, "選商品", build_item_picker_flex())
        return

    if action == "back_to_items":
        reply_flex(event.reply_token, "選商品", build_item_picker_flex())
        return

    if action == "choose_item":
        item_key = q.get("item", "")
        if item_key not in ITEMS:
            reply_text(event.reply_token, "商品不存在，請重新選擇。")
            reply_flex(event.reply_token, "選商品", build_item_picker_flex())
            return
        item = ITEMS[item_key]
        if item["has_flavor"]:
            reply_flex(event.reply_token, "選口味", build_flavor_picker_flex(item_key))
        else:
            reply_flex(event.reply_token, "選數量", build_qty_picker_flex(item_key, None))
        return

    if action == "choose_flavor":
        item_key = q.get("item", "")
        flavor_key = q.get("flavor", "")
        if item_key not in ITEMS:
            reply_text(event.reply_token, "商品不存在，請重新選擇。")
            reply_flex(event.reply_token, "選商品", build_item_picker_flex())
            return

        if item_key == "dacquoise":
            if not lock_dacquoise_rule(st, flavor_key):
                locked = st.get("dacquoise_flavor_locked")
                locked_label = next((lab for k, lab in ITEMS["dacquoise"]["flavors"] if k == locked), locked)
                reply_text(event.reply_token, f"這張訂單的達克瓦茲口味已鎖定為「{locked_label}」。\n如要換口味，請先按「清空重來」。")
                reply_flex(event.reply_token, "選商品", build_item_picker_flex())
                return

        reply_flex(event.reply_token, "選數量", build_qty_picker_flex(item_key, flavor_key))
        return

    if action == "choose_qty":
        item_key = q.get("item", "")
        flavor_key = (q.get("flavor", "") or None)
        qty = int(q.get("qty", "0") or "0")

        if item_key not in ITEMS:
            reply_text(event.reply_token, "商品不存在，請重新選擇。")
            reply_flex(event.reply_token, "選商品", build_item_picker_flex())
            return

        item = ITEMS[item_key]
        if qty < item["min_qty"]:
            reply_text(event.reply_token, f"{item['label']} 最少購買數量為 {item['min_qty']}。")
            return

        flavor_label = None
        if flavor_key:
            flavor_label = next((lab for k, lab in item["flavors"] if k == flavor_key), flavor_key)

        subtotal = qty * item["unit_price"]
        st["cart"].append({
            "item_key": item_key,
            "label": item["label"],
            "flavor": flavor_key,
            "flavor_label": flavor_label,
            "qty": qty,
            "unit_price": item["unit_price"],
            "subtotal": subtotal,
        })

        reply_text(event.reply_token, cart_summary_text(st))
        reply_flex(event.reply_token, "選商品", build_item_picker_flex())
        return

    if action == "checkout":
        if not st["cart"]:
            reply_text(event.reply_token, "購物車是空的，請先選商品。")
            reply_flex(event.reply_token, "選商品", build_item_picker_flex())
            return
        reply_text(event.reply_token, "好，接著選取貨方式。")
        reply_flex(event.reply_token, "取貨方式", build_delivery_method_flex())
        return

    if action == "pickup_method":
        method = q.get("method", "")
        if method not in ["store", "delivery"]:
            reply_text(event.reply_token, "取貨方式錯誤，請重新選擇。")
            reply_flex(event.reply_token, "取貨方式", build_delivery_method_flex())
            return

        st["pickup_method"] = method

        if method == "delivery":
            st["stage"] = "await_delivery_name"
            reply_text(event.reply_token, "請輸入「宅配收件人姓名」（必填）")
            return

        # store
        st["stage"] = "await_store_pickup_name"
        reply_text(event.reply_token, "請輸入「店取取件人姓名」（必填）")
        return

    if action == "pickup_date":
        st["pickup_date"] = q.get("date", "")

        if st["pickup_method"] == "store":
            reply_text(event.reply_token, "請選店取時段：")
            reply_flex(event.reply_token, "店取時段", build_pickup_time_flex())
            return

        # delivery -> directly confirm
        reply_text(event.reply_token, "收到希望到貨日✅ 接著請確認結帳內容。")
        reply_flex(event.reply_token, "結帳確認", build_checkout_confirm_flex(st))
        return

    if action == "pickup_time":
        st["pickup_time"] = q.get("time", "")
        # append to note
        st["note"] = f"{st['note']}｜店取時段：{st['pickup_time']}"
        reply_text(event.reply_token, "收到店取時段✅ 接著請確認結帳內容。")
        reply_flex(event.reply_token, "結帳確認", build_checkout_confirm_flex(st))
        return

    if action == "place_order":
        if not st["cart"] or not st["pickup_method"] or not st["pickup_date"]:
            reply_text(event.reply_token, "訂單資料不完整，請重新進入「我要下單」。")
            reply_flex(event.reply_token, "選單", build_main_menu_flex())
            return
        if st["pickup_method"] == "store" and not st["pickup_time"]:
            reply_text(event.reply_token, "店取需要選擇時段，請重新選擇。")
            reply_flex(event.reply_token, "店取時段", build_pickup_time_flex())
            return
        if st["pickup_method"] == "delivery":
            d = st["delivery"]
            if not (d["name"] and d["phone"] and d["address"]):
                reply_text(event.reply_token, "宅配資訊未填完（姓名/電話/地址）。請重新下單。")
                reply_flex(event.reply_token, "選單", build_main_menu_flex())
                return

        order_id = generate_order_id()
        created_at = datetime.now(TZ_TAIPEI).strftime("%Y-%m-%d %H:%M:%S")

        subtotal = sum(it["subtotal"] for it in st["cart"])
        shipping = 0
        if st["pickup_method"] == "delivery":
            shipping = 0 if subtotal >= FREE_SHIPPING_THRESHOLD else SHIPPING_FEE
        total = subtotal + shipping

        items_json = json.dumps(
            {"cart": st["cart"], "subtotal": subtotal, "shipping": shipping, "total": total},
            ensure_ascii=False
        )

        pickup_method_label = "店取" if st["pickup_method"] == "store" else "宅配"
        pay_status = "UNPAID"

        row = [
            created_at,
            user_id,
            "(unknown)",
            order_id,
            items_json,
            pickup_method_label,
            st["pickup_date"],
            st["pickup_time"] or "",
            st.get("note", ""),
            total,
            pay_status,
            "",
        ]
        append_order_row(row)

        date_label = "希望取貨日" if st["pickup_method"] == "store" else "希望到貨日（不保證準時）"
        pm_text = "店取" if st["pickup_method"] == "store" else "宅配（冷凍）"
        fee_text = f"運費 NT${shipping}" if st["pickup_method"] == "delivery" else "運費 NT$0"

        msg = "\n".join([
            "✅ 訂單已建立",
            f"訂單編號：{order_id}",
            f"取貨方式：{pm_text}",
            f"{date_label}：{st['pickup_date']}",
            *( [f"店取時段：{st['pickup_time']}"] if st["pickup_method"] == "store" else [] ),
            f"總計：NT${total}（{fee_text}）",
            "",
            "【付款方式】轉帳匯款",
            f"- {BANK_TEXT}",
            "",
            "匯款後請回覆：",
            f"已轉帳 訂單編號 {order_id} 後五碼 12345",
        ])
        reply_text(event.reply_token, msg)

        reset_order(user_id)
        return

    reply_text(event.reply_token, "我沒看懂這個操作，請輸入「選單」回到主選單。")
