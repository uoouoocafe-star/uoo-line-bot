import os
import json
import base64
import uuid
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, List

from fastapi import FastAPI, Request, HTTPException

from linebot.v3 import (
    WebhookParser
)
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import (
    MessageEvent,
    TextMessageContent,
    PostbackEvent
)
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
    QuickReply,
    QuickReplyItem,
    PostbackAction
)

from google.oauth2 import service_account
from googleapiclient.discovery import build


# =========================
# Env
# =========================
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "")
GSHEET_ID = os.getenv("GSHEET_ID", "")
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")  # optional fallback

BANK_TRANSFER_TEXT = "轉帳帳號：台灣銀行 004 248-001-03430-6"

STORE_PICKUP_ADDRESS = "店取地址：新竹縣竹北市隘口六街65號"

SHIPPING_FEE = 180
FREE_SHIPPING_THRESHOLD = 2500

# 取貨/希望到貨日限制：今天+3 到 今天+14
MIN_DAYS = 3
MAX_DAYS = 14

# 店取時段（你可自行改）
PICKUP_SLOTS = [
    "10:00-12:00",
    "12:00-14:00",
    "14:00-16:00",
    "16:00-18:00",
]

# =========================
# Menu / Products
# =========================
PRODUCTS = {
    "dacquoise": {
        "label": "達克瓦茲",
        "unit_price": 95,
        "min_qty": 2,
        "flavors": ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"],
        "flavor_mix_allowed": False,  # 口味不可混（每一筆只能一種口味）
        "storage": "冷藏或冷凍保存",
        "rule": "至少前三天下單；每口味至少 2 顆"
    },
    "scone": {
        "label": "原味司康",
        "unit_price": 65,
        "min_qty": 1,
        "flavors": [],  # no flavor
        "flavor_mix_allowed": True,
        "storage": "冷藏或冷凍保存",
        "rule": "至少前三天下單"
    },
    "canele": {
        "label": "原味可麗露",
        "unit_price": 90,
        "min_qty": 1,
        "flavors": [],
        "flavor_mix_allowed": True,
        "storage": "限冷凍保存",
        "rule": "至少前三天下單"
    },
    "toast": {
        "label": "伊思尼奶酥厚片",
        "unit_price": 85,
        "min_qty": 1,
        "flavors": ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"],
        "flavor_mix_allowed": True,
        "storage": "冷凍保存",
        "rule": "至少前三天下單"
    }
}


# =========================
# In-memory sessions
# Render 單機版先 OK；之後要更穩可以換 Redis
# =========================
SESSIONS: Dict[str, Dict[str, Any]] = {}
# session schema:
# {
#   "state": "...",
#   "cart": [ {item_key,label,flavor,qty,unit_price,subtotal}, ... ],
#   "pickup_method": "store"|"ship",
#   "pickup_date": "YYYY-MM-DD",
#   "pickup_time": "10:00-12:00",
#   "receiver_name": "...",
#   "receiver_phone": "...",
#   "receiver_address": "...",
#   "note": "...",
# }


def now_ts_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_date() -> date:
    return datetime.now().date()


def date_options() -> List[str]:
    start = today_date() + timedelta(days=MIN_DAYS)
    end = today_date() + timedelta(days=MAX_DAYS)
    days = []
    d = start
    while d <= end:
        days.append(d.isoformat())
        d += timedelta(days=1)
    return days


def get_session(user_id: str) -> Dict[str, Any]:
    if user_id not in SESSIONS:
        SESSIONS[user_id] = {
            "state": "IDLE",
            "cart": [],
            "pickup_method": None,
            "pickup_date": None,
            "pickup_time": None,
            "receiver_name": None,
            "receiver_phone": None,
            "receiver_address": None,
            "note": "",
        }
    return SESSIONS[user_id]


def reset_session(user_id: str) -> None:
    SESSIONS[user_id] = {
        "state": "IDLE",
        "cart": [],
        "pickup_method": None,
        "pickup_date": None,
        "pickup_time": None,
        "receiver_name": None,
        "receiver_phone": None,
        "receiver_address": None,
        "note": "",
    }


def cart_total(cart: List[Dict[str, Any]]) -> int:
    return sum(int(x.get("subtotal", 0)) for x in cart)


def calc_shipping(amount: int, pickup_method: str) -> int:
    if pickup_method != "ship":
        return 0
    return 0 if amount >= FREE_SHIPPING_THRESHOLD else SHIPPING_FEE


def make_order_id() -> str:
    # UOO-YYYYMMDD-xxxxx
    return f"UOO-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:5].upper()}"


# =========================
# Google Sheet
# =========================
def load_service_account_info() -> Dict[str, Any]:
    if GOOGLE_SERVICE_ACCOUNT_B64.strip():
        try:
            raw = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64.encode("utf-8")).decode("utf-8")
            info = json.loads(raw)
            # private_key sometimes has escaped \n
            if "private_key" in info and "\\n" in info["private_key"]:
                info["private_key"] = info["private_key"].replace("\\n", "\n")
            return info
        except Exception as e:
            raise RuntimeError(f"GOOGLE_SERVICE_ACCOUNT_B64 decode/json failed: {e}")

    if GOOGLE_SERVICE_ACCOUNT_JSON.strip():
        try:
            info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
            if "private_key" in info and "\\n" in info["private_key"]:
                info["private_key"] = info["private_key"].replace("\\n", "\n")
            return info
        except Exception as e:
            raise RuntimeError(f"GOOGLE_SERVICE_ACCOUNT_JSON json failed: {e}")

    raise RuntimeError("Google service account env missing.")


def append_order_row(
    created_at: str,
    user_id: str,
    display_name: str,
    order_id: str,
    items_json: str,
    pickup_method: str,
    pickup_date: str,
    pickup_time: str,
    note: str,
    amount: int,
    pay_status: str,
    linepay_transaction_id: str = "",
) -> None:
    if not GSHEET_ID:
        raise RuntimeError("GSHEET_ID missing")

    info = load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    values = [[
        created_at,
        user_id,
        display_name,
        order_id,
        items_json,
        pickup_method,
        pickup_date,
        pickup_time,
        note,
        amount,
        pay_status,
        linepay_transaction_id
    ]]

    body = {"values": values}
    service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range="sheet1!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()


# =========================
# LINE reply helpers
# =========================
configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN) if CHANNEL_ACCESS_TOKEN else None
parser = WebhookParser(CHANNEL_SECRET) if CHANNEL_SECRET else None

app = FastAPI()


def line_api() -> MessagingApi:
    if not configuration:
        raise RuntimeError("CHANNEL_ACCESS_TOKEN missing")
    return MessagingApi(ApiClient(configuration))


def qr_postbacks(items: List[Dict[str, str]]) -> QuickReply:
    # items: [{"label": "...", "data": "..."}]
    qr_items = []
    for it in items[:13]:  # LINE quick reply limit
        qr_items.append(
            QuickReplyItem(
                action=PostbackAction(label=it["label"], data=it["data"])
            )
        )
    return QuickReply(items=qr_items)


def reply_text(reply_token: str, text: str, quick_reply: Optional[QuickReply] = None) -> None:
    api = line_api()
    msg = TextMessage(text=text, quick_reply=quick_reply)
    api.reply_message(
        ReplyMessageRequest(
            reply_token=reply_token,
            messages=[msg]
        )
    )


def pretty_cart(cart: List[Dict[str, Any]]) -> str:
    if not cart:
        return "（購物車目前是空的）"
    lines = []
    for i, it in enumerate(cart, start=1):
        name = it["label"]
        flavor = it.get("flavor")
        qty = it["qty"]
        subtotal = it["subtotal"]
        if flavor:
            lines.append(f"{i}. {name}（{flavor}）x{qty} = {subtotal}")
        else:
            lines.append(f"{i}. {name} x{qty} = {subtotal}")
    return "\n".join(lines)


# =========================
# Flow controllers
# =========================
def start_order(reply_token: str, user_id: str) -> None:
    s = get_session(user_id)
    s["state"] = "CHOOSE_ITEM"
    items = []
    for key, p in PRODUCTS.items():
        items.append({"label": p["label"], "data": f"ACTION=CHOOSE_ITEM&item={key}"})
    items += [
        {"label": "查看購物車", "data": "ACTION=VIEW_CART"},
        {"label": "清空購物車", "data": "ACTION=CLEAR_CART"},
    ]
    reply_text(
        reply_token,
        "請選擇要購買的品項：",
        quick_reply=qr_postbacks(items)
    )


def ask_flavor(reply_token: str, user_id: str, item_key: str) -> None:
    s = get_session(user_id)
    p = PRODUCTS[item_key]
    s["state"] = "CHOOSE_FLAVOR"
    s["pending_item"] = item_key

    flavor_items = [{"label": f, "data": f"ACTION=CHOOSE_FLAVOR&flavor={f}"} for f in p["flavors"]]
    flavor_items += [
        {"label": "返回品項", "data": "ACTION=BACK_TO_ITEMS"},
        {"label": "查看購物車", "data": "ACTION=VIEW_CART"},
    ]

    rule = []
    if p.get("min_qty", 1) > 1:
        rule.append(f"最少 {p['min_qty']} 顆")
    if item_key == "dacquoise":
        rule.append("同一筆口味不可混（可分開加入多筆）")

    hint = f"你選了\n請選口味："
    if rule:
        hint += "\n（" + " / ".join(rule) + "）"

    reply_text(reply_token, hint, quick_reply=qr_postbacks(flavor_items))


def ask_qty(reply_token: str, user_id: str, item_key: str, flavor: Optional[str]) -> None:
    s = get_session(user_id)
    p = PRODUCTS[item_key]
    s["state"] = "CHOOSE_QTY"
    s["pending_item"] = item_key
    s["pending_flavor"] = flavor

    min_q = int(p.get("min_qty", 1))
    # 直接給常用數量按鈕（你也可改）
    qty_candidates = [min_q, min_q + 1, min_q + 2, min_q + 4, min_q + 8]
    qty_candidates = sorted(list(dict.fromkeys([q for q in qty_candidates if q <= 30])))

    items = [{"label": str(q), "data": f"ACTION=CHOOSE_QTY&qty={q}"} for q in qty_candidates]
    items += [
        {"label": "返回口味/品項", "data": "ACTION=BACK_STEP"},
        {"label": "查看購物車", "data": "ACTION=VIEW_CART"},
    ]

    if flavor:
        title = f"口味：{flavor}\n請選數量（最少 {min_q}）："
    else:
        title = f"請選數量（最少 {min_q}）："

    reply_text(reply_token, title, quick_reply=qr_postbacks(items))


def add_to_cart(user_id: str, item_key: str, flavor: Optional[str], qty: int) -> None:
    s = get_session(user_id)
    p = PRODUCTS[item_key]
    unit = int(p["unit_price"])
    subtotal = unit * int(qty)
    s["cart"].append({
        "item_key": item_key,
        "label": p["label"],
        "flavor": flavor or "",
        "qty": int(qty),
        "unit_price": unit,
        "subtotal": subtotal,
    })


def after_added(reply_token: str, user_id: str) -> None:
    s = get_session(user_id)
    total = cart_total(s["cart"])
    text = "✅ 已加入購物車\n\n" + pretty_cart(s["cart"]) + f"\n\n目前小計：{total} 元"

    items = [
        {"label": "繼續加購", "data": "ACTION=CONTINUE_SHOP"},
        {"label": "前往結帳", "data": "ACTION=CHECKOUT"},
        {"label": "清空購物車", "data": "ACTION=CLEAR_CART"},
    ]
    reply_text(reply_token, text, quick_reply=qr_postbacks(items))


def checkout(reply_token: str, user_id: str) -> None:
    s = get_session(user_id)
    if not s["cart"]:
        reply_text(reply_token, "你的購物車是空的，先選品項加入購物車喔。")
        start_order(reply_token, user_id)
        return

    s["state"] = "CHOOSE_PICKUP"
    items = [
        {"label": "店取", "data": "ACTION=PICKUP_METHOD&method=store"},
        {"label": "宅配（冷凍）", "data": "ACTION=PICKUP_METHOD&method=ship"},
        {"label": "返回加購", "data": "ACTION=CONTINUE_SHOP"},
    ]
    reply_text(reply_token, "結帳前請選取貨方式：", quick_reply=qr_postbacks(items))


def ask_date(reply_token: str, user_id: str) -> None:
    s = get_session(user_id)
    method = s["pickup_method"]
    s["state"] = "CHOOSE_DATE"

    opts = date_options()
    items = [{"label": d, "data": f"ACTION=CHOOSE_DATE&date={d}"} for d in opts[:10]]
    # 若日期很多，quick reply 放不下，就先給前 10 天，剩下用「下一頁」
    if len(opts) > 10:
        items.append({"label": "更多日期", "data": "ACTION=DATE_MORE&page=2"})
    items.append({"label": "返回取貨方式", "data": "ACTION=BACK_TO_PICKUP"})

    if method == "ship":
        title = "請選「希望到貨日」（不保證當日到，物流可能延遲）\n（僅能選今天+3 ～ 今天+14）"
    else:
        title = "請選取貨日期（僅能選今天+3 ～ 今天+14）"

    reply_text(reply_token, title, quick_reply=qr_postbacks(items))


def ask_date_page(reply_token: str, user_id: str, page: int) -> None:
    s = get_session(user_id)
    opts = date_options()
    per = 10
    start = (page - 1) * per
    chunk = opts[start:start + per]

    items = [{"label": d, "data": f"ACTION=CHOOSE_DATE&date={d}"} for d in chunk]
    if start + per < len(opts):
        items.append({"label": "更多日期", "data": f"ACTION=DATE_MORE&page={page+1}"})
    items.append({"label": "返回取貨方式", "data": "ACTION=BACK_TO_PICKUP"})
    reply_text(reply_token, "請選日期：", quick_reply=qr_postbacks(items))


def ask_time_or_receiver(reply_token: str, user_id: str) -> None:
    s = get_session(user_id)
    if s["pickup_method"] == "store":
        s["state"] = "CHOOSE_TIME"
        items = [{"label": t, "data": f"ACTION=CHOOSE_TIME&time={t}"} for t in PICKUP_SLOTS]
        items += [{"label": "返回日期", "data": "ACTION=BACK_TO_DATE"}]
        reply_text(reply_token, "請選店取時段：", quick_reply=qr_postbacks(items))
    else:
        # ship: ask receiver name first
        s["state"] = "ASK_RECEIVER_NAME"
        reply_text(reply_token, "請輸入收件人姓名（宅配必填）：")


def ask_store_receiver_name(reply_token: str, user_id: str) -> None:
    s = get_session(user_id)
    s["state"] = "ASK_STORE_RECEIVER_NAME"
    reply_text(reply_token, "請輸入取件人姓名（店取必填）：")


def ask_phone(reply_token: str, user_id: str) -> None:
    s = get_session(user_id)
    s["state"] = "ASK_PHONE"
    reply_text(reply_token, "請輸入電話（宅配必填，務必保持可聯繫）：")


def ask_address(reply_token: str, user_id: str) -> None:
    s = get_session(user_id)
    s["state"] = "ASK_ADDRESS"
    reply_text(reply_token, "請輸入宅配地址（宅配必填）：")


def ask_note(reply_token: str, user_id: str) -> None:
    s = get_session(user_id)
    s["state"] = "ASK_NOTE"
    reply_text(reply_token, "有需要備註嗎？（可直接回覆「無」或輸入備註內容）")


def confirm_order(reply_token: str, user_id: str) -> None:
    s = get_session(user_id)
    amount = cart_total(s["cart"])
    shipping = calc_shipping(amount, s["pickup_method"])
    total = amount + shipping

    lines = []
    lines.append("【訂單確認】")
    lines.append(pretty_cart(s["cart"]))
    lines.append(f"\n商品小計：{amount} 元")

    if s["pickup_method"] == "ship":
        ship_line = "運費："
        ship_line += "0 元（滿 2500 免運）" if shipping == 0 else f"{shipping} 元"
        lines.append(ship_line)
        lines.append(f"總計：{total} 元")
        lines.append(f"\n希望到貨日：{s['pickup_date']}（不保證當日到）")
        lines.append(f"收件人：{s['receiver_name']} / {s['receiver_phone']}")
        lines.append(f"地址：{s['receiver_address']}")
        lines.append("\n宅配提醒：保持電話暢通、收到立刻檢查、嚴重損壞請拍照含原箱並當日聯繫。")
        lines.append("風險認知：運送輕微位移/裝飾掉落通常不在理賠範圍；天災延誤無法保證準時。")
    else:
        lines.append(f"總計：{total} 元")
        lines.append(f"\n店取日期：{s['pickup_date']}")
        lines.append(f"店取時段：{s['pickup_time']}")
        lines.append(f"取件人：{s['receiver_name']}")
        lines.append(STORE_PICKUP_ADDRESS)

    if s.get("note"):
        lines.append(f"\n備註：{s['note']}")

    s["state"] = "CONFIRM"
    items = [
        {"label": "確認送出", "data": "ACTION=SUBMIT_ORDER"},
        {"label": "返回修改", "data": "ACTION=BACK_TO_CHECKOUT"},
        {"label": "取消/清空", "data": "ACTION=CLEAR_CART"},
    ]
    reply_text(reply_token, "\n".join(lines), quick_reply=qr_postbacks(items))


def submit_order(reply_token: str, user_id: str, display_name: str) -> None:
    s = get_session(user_id)
    if not s["cart"] or not s["pickup_method"] or not s["pickup_date"]:
        reply_text(reply_token, "訂單資料不完整，請重新結帳流程。")
        checkout(reply_token, user_id)
        return

    amount = cart_total(s["cart"])
    shipping = calc_shipping(amount, s["pickup_method"])
    total = amount + shipping

    order_id = make_order_id()

    # items_json 用 ensure_ascii=False，Google Sheet 看起來就不會像亂碼
    items_payload = {
        "cart": s["cart"],
        "shipping_fee": shipping,
        "rules": {
            "min_days": MIN_DAYS,
            "max_days": MAX_DAYS,
            "shipping_fee": SHIPPING_FEE,
            "free_shipping_threshold": FREE_SHIPPING_THRESHOLD
        }
    }
    items_json = json.dumps(items_payload, ensure_ascii=False)

    pickup_method_text = "店取" if s["pickup_method"] == "store" else "宅配"
    pickup_time = s["pickup_time"] or ""
    note = s.get("note") or ""

    # 宅配把地址/電話也寫進 note，方便你看單
    if s["pickup_method"] == "ship":
        note = (note + "\n" if note else "") + f"收件人:{s['receiver_name']}｜電話:{s['receiver_phone']}｜地址:{s['receiver_address']}"
    else:
        note = (note + "\n" if note else "") + f"取件人:{s['receiver_name']}"

    try:
        append_order_row(
            created_at=now_ts_str(),
            user_id=user_id,
            display_name=display_name or "",
            order_id=order_id,
            items_json=items_json,
            pickup_method=pickup_method_text,
            pickup_date=s["pickup_date"],
            pickup_time=pickup_time,
            note=note,
            amount=total,
            pay_status="UNPAID",
            linepay_transaction_id=""
        )
    except Exception as e:
        reply_text(reply_token, f"❌ 寫入訂單失敗：{e}\n請稍後再試，或把錯誤貼給我。")
        return

    # 送出成功
    pay_items = [
        {"label": "我已轉帳", "data": f"ACTION=PAY_DONE&order_id={order_id}"},
        {"label": "繼續下單", "data": "ACTION=START_ORDER"},
    ]

    reply_text(
        reply_token,
        "✅ 訂單已建立成功！\n"
        f"訂單編號：{order_id}\n\n"
        "付款方式（先用轉帳）：\n"
        f"{BANK_TRANSFER_TEXT}\n\n"
        "請轉帳後回來按「我已轉帳」，並輸入後五碼（對帳用）。",
        quick_reply=qr_postbacks(pay_items)
    )

    # 清空 session（保留也可以；這裡先清掉避免重複送單）
    reset_session(user_id)


# =========================
# Web routes
# =========================
@app.get("/")
def health():
    return {"ok": True}


@app.post("/callback")
async def callback(request: Request):
    if not parser:
        raise HTTPException(status_code=500, detail="CHANNEL_SECRET missing")

    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    body_text = body.decode("utf-8")

    try:
        events = parser.parse(body_text, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    for event in events:
        try:
            if isinstance(event, MessageEvent) and isinstance(event.message, TextMessageContent):
                await handle_text(event)
            elif isinstance(event, PostbackEvent):
                await handle_postback(event)
        except Exception as e:
            # 不讓 webhook 500，避免 LINE 停止推送
            try:
                reply_text(event.reply_token, f"系統處理中遇到問題：{e}\n請再試一次，或把錯誤貼給我們。")
            except Exception:
                pass

    return "OK"


async def handle_text(event: MessageEvent):
    user_id = event.source.user_id
    text = (event.message.text or "").strip()
    s = get_session(user_id)

    # 取得 display name（可省略；拿不到也不影響）
    display_name = ""
    try:
        # profile api in v3 is a separate API,但這裡先不做，避免多依賴
        display_name = ""
    except Exception:
        display_name = ""

    # 快捷啟動
    if text in ["我要下單", "下單", "甜點", "訂單", "開始", "START"]:
        start_order(event.reply_token, user_id)
        return

    if text in ["查看購物車", "購物車"]:
        reply_text(event.reply_token, pretty_cart(s["cart"]))
        return

    if text in ["清空", "清空購物車", "取消"]:
        reset_session(user_id)
        reply_text(event.reply_token, "已清空購物車。要下單請輸入「我要下單」。")
        return

    # 需要輸入文字的狀態
    if s["state"] == "ASK_RECEIVER_NAME":
        s["receiver_name"] = text
        ask_phone(event.reply_token, user_id)
        return

    if s["state"] == "ASK_STORE_RECEIVER_NAME":
        s["receiver_name"] = text
        ask_note(event.reply_token, user_id)
        return

    if s["state"] == "ASK_PHONE":
        s["receiver_phone"] = text
        ask_address(event.reply_token, user_id)
        return

    if s["state"] == "ASK_ADDRESS":
        s["receiver_address"] = text
        ask_note(event.reply_token, user_id)
        return

    if s["state"] == "ASK_NOTE":
        s["note"] = "" if text in ["無", "不用", "沒有", "-"] else text
        confirm_order(event.reply_token, user_id)
        return

    # 付款回報（轉帳後五碼）
    if text.startswith("後五碼"):
        # 這裡先示範回覆；下一版可做「更新 sheet pay_status」
        reply_text(event.reply_token, "收到後五碼，我們將人工對帳，確認後依訂單順序出貨。謝謝你！")
        return

    # default
    reply_text(
        event.reply_token,
        "你可以輸入「我要下單」開始。\n"
        "（建議用按鈕操作，會比輸入文字更快）"
    )


async def handle_postback(event: PostbackEvent):
    user_id = event.source.user_id
    s = get_session(user_id)

    data = event.postback.data or ""
    params = parse_kv(data)
    action = params.get("ACTION", "")

    if action == "START_ORDER":
        start_order(event.reply_token, user_id)
        return

    if action == "VIEW_CART":
        reply_text(event.reply_token, pretty_cart(s["cart"]))
        return

    if action == "CLEAR_CART":
        reset_session(user_id)
        reply_text(event.reply_token, "已清空購物車。要下單請輸入「我要下單」。")
        return

    if action == "BACK_TO_ITEMS":
        start_order(event.reply_token, user_id)
        return

    if action == "BACK_STEP":
        # 回到選口味或品項
        pending_item = s.get("pending_item")
        if pending_item and PRODUCTS[pending_item]["flavors"]:
            ask_flavor(event.reply_token, user_id, pending_item)
        else:
            start_order(event.reply_token, user_id)
        return

    if action == "CHOOSE_ITEM":
        item_key = params.get("item", "")
        if item_key not in PRODUCTS:
            reply_text(event.reply_token, "品項不存在，請重新選擇。")
            start_order(event.reply_token, user_id)
            return

        p = PRODUCTS[item_key]
        if p["flavors"]:
            ask_flavor(event.reply_token, user_id, item_key)
        else:
            ask_qty(event.reply_token, user_id, item_key, None)
        return

    if action == "CHOOSE_FLAVOR":
        flavor = params.get("flavor", "")
        item_key = s.get("pending_item")
        if not item_key or item_key not in PRODUCTS:
            start_order(event.reply_token, user_id)
            return
        ask_qty(event.reply_token, user_id, item_key, flavor)
        return

    if action == "CHOOSE_QTY":
        qty = int(params.get("qty", "0") or 0)
        item_key = s.get("pending_item")
        flavor = s.get("pending_flavor", "")
        if not item_key or item_key not in PRODUCTS:
            start_order(event.reply_token, user_id)
            return

        p = PRODUCTS[item_key]
        min_q = int(p.get("min_qty", 1))
        if qty < min_q:
            reply_text(event.reply_token, f"數量最少 {min_q}，請重新選擇。")
            ask_qty(event.reply_token, user_id, item_key, flavor if flavor else None)
            return

        add_to_cart(user_id, item_key, flavor if flavor else None, qty)
        # clear pending
        s["pending_item"] = None
        s["pending_flavor"] = None
        after_added(event.reply_token, user_id)
        return

    if action == "CONTINUE_SHOP":
        start_order(event.reply_token, user_id)
        return

    if action == "CHECKOUT":
        checkout(event.reply_token, user_id)
        return

    if action == "PICKUP_METHOD":
        method = params.get("method", "")
        if method not in ["store", "ship"]:
            reply_text(event.reply_token, "取貨方式無效，請重新選擇。")
            checkout(event.reply_token, user_id)
            return
        s["pickup_method"] = method
        ask_date(event.reply_token, user_id)
        return

    if action == "BACK_TO_PICKUP":
        checkout(event.reply_token, user_id)
        return

    if action == "DATE_MORE":
        page = int(params.get("page", "2") or 2)
        ask_date_page(event.reply_token, user_id, page)
        return

    if action == "CHOOSE_DATE":
        d = params.get("date", "")
        # validate range
        try:
            dd = datetime.strptime(d, "%Y-%m-%d").date()
        except Exception:
            reply_text(event.reply_token, "日期格式不正確，請重新選擇。")
            ask_date(event.reply_token, user_id)
            return

        min_d = today_date() + timedelta(days=MIN_DAYS)
        max_d = today_date() + timedelta(days=MAX_DAYS)
        if dd < min_d or dd > max_d:
            reply_text(event.reply_token, f"日期需在 {min_d} ～ {max_d} 之間。請重新選擇。")
a
            ask_date(event.reply_token, user_id)
            return

        s["pickup_date"] = d
        ask_time_or_receiver(event.reply_token, user_id)
        return

    if action == "BACK_TO_DATE":
        ask_date(event.reply_token, user_id)
        return

    if action == "CHOOSE_TIME":
        t = params.get("time", "")
        if t not in PICKUP_SLOTS:
            reply_text(event.reply_token, "時段無效，請重新選擇。")
            ask_time_or_receiver(event.reply_token, user_id)
            return
        s["pickup_time"] = t
        ask_store_receiver_name(event.reply_token, user_id)
        return

    if action == "BACK_TO_CHECKOUT":
        checkout(event.reply_token, user_id)
        return

    if action == "SUBMIT_ORDER":
        # display_name 先不拿 profile，避免依賴；你也可以之後再加
        submit_order(event.reply_token, user_id, display_name="")
        return

    if action == "PAY_DONE":
        order_id = params.get("order_id", "")
        reply_text(
            event.reply_token,
            f"請回覆「後五碼12345」這種格式，方便我們對帳。\n（訂單編號：{order_id}）"
        )
        return

    # fallback
    reply_text(event.reply_token, "未識別的操作，請輸入「我要下單」重新開始。")


def parse_kv(data: str) -> Dict[str, str]:
    # data like: "ACTION=CHOOSE_ITEM&item=dacquoise"
    out: Dict[str, str] = {}
    for part in data.split("&"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k] = v
    return out
