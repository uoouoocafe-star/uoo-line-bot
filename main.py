import os
import json
import base64
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta, date
from typing import Optional, Dict, Any, List, Tuple

from fastapi import FastAPI, Request, HTTPException

from linebot.v3.webhook import WebhookParser
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
    QuickReply,
    QuickReplyItem,
    MessageAction,
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# =========================
# ENV
# =========================
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "")

GSHEET_ID = os.getenv("GSHEET_ID", "")
GSHEET_TAB_NAME = os.getenv("GSHEET_TAB_NAME", "Orders")  # 分頁名稱
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

TZ_TAIPEI = timezone(timedelta(hours=8))

app = FastAPI()
line_config = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(CHANNEL_SECRET)

# =========================
# Business rules / prices
# =========================
PREORDER_MIN_DAYS = 3
PREORDER_MAX_DAYS = 14  # 你要 3~14 天

SHIP_FEE = 180
FREE_SHIP_THRESHOLD = 2500

PRICES = {
    "dacquoise": 95,
    "scone": 65,
    "canele": 90,
    "toast": 85,
}

FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

ITEM_LABELS = {
    "dacquoise": "達克瓦茲",
    "scone": "司康",
    "canele": "可麗露",
    "toast": "奶酥厚片",
}

# 店取時段（你可隨時改成更精準的）
PICKUP_TIME_SLOTS = [
    "10:00-12:00",
    "12:00-14:00",
    "14:00-16:00",
]

# =========================
# Texts
# =========================
MENU_TEXT = (
    "🍰【UooUoo 甜點訂單】\n\n"
    "請點底部選單「我要下單」用按鈕完成下單。\n\n"
    "你也可以輸入：\n"
    "- 甜點（看菜單）\n"
    "- 取貨說明\n"
    "- 付款說明"
)

DESSERT_MENU_TEXT = (
    "🍰【甜點菜單】（全品項需提前預訂）\n\n"
    "1) 達克瓦茲 / 95元/顆\n"
    f"口味：{'、'.join(FLAVORS)}\n"
    "（每個口味最低 2 顆）\n\n"
    "2) 原味司康 / 65元/顆\n\n"
    "3) 原味可麗露 / 90元/顆（限冷凍）\n\n"
    "4) 伊思尼奶酥厚片 / 85元/片\n"
    f"口味：{'、'.join(FLAVORS)}\n\n"
    f"📌 宅配：大榮冷凍 ${SHIP_FEE} / 滿${FREE_SHIP_THRESHOLD}免運\n"
    f"📌 取貨日期可選：下單日起第 {PREORDER_MIN_DAYS} 天～第 {PREORDER_MAX_DAYS} 天"
)

PICKUP_TEXT = (
    "📦【取貨說明】\n\n"
    "🏠 店取：新竹縣竹北市隘口六街65號\n\n"
    f"🚚 宅配：一律冷凍宅配（大榮）\n運費 ${SHIP_FEE} / 滿${FREE_SHIP_THRESHOLD}免運\n\n"
    "✅ 宅配注意事項：\n"
    "・保持電話暢通，避免無人收件退件\n"
    "・收到後立刻開箱確認狀態並盡快冷藏/冷凍\n"
    "・若嚴重損壞（糊爛、不成形），請拍照（含原箱）並當日聯繫\n"
    "・未處理完前請保留原狀，勿丟棄或食用\n\n"
    "⚠️ 風險認知：\n"
    "・運送輕微位移/裝飾掉落通常不在理賠範圍\n"
    "・天災物流可能暫停或延遲，無法保證準時送達"
)

PAY_TEXT = (
    "💸【付款說明】\n\n"
    "目前提供：銀行轉帳（對帳後依訂單號碼陸續出貨/通知取貨）\n\n"
    "🏦 台灣銀行（004）\n"
    "帳號：248-001-03430-6\n\n"
    "📩 匯款後請回覆：\n"
    "已轉帳 訂單編號 末五碼12345"
)

# =========================
# Google Sheet helpers
# =========================
def _load_service_account_info() -> Optional[Dict[str, Any]]:
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


def _get_sheets_service():
    info = _load_service_account_info()
    if not info:
        raise RuntimeError(
            "Google service account env missing/invalid: set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_B64"
        )
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return build("sheets", "v4", credentials=creds)


def append_order_row(row_values: list):
    if not GSHEET_ID.strip():
        raise RuntimeError("GSHEET_ID missing")

    service = _get_sheets_service()
    range_name = f"{GSHEET_TAB_NAME}!A:L"
    body = {"values": [row_values]}

    service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range=range_name,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


# =========================
# LINE reply helper
# =========================
def reply_text(reply_token: str, text: str, quick: Optional[QuickReply] = None):
    with ApiClient(line_config) as api_client:
        api = MessagingApi(api_client)
        msg = TextMessage(text=text, quickReply=quick) if quick else TextMessage(text=text)
        api.reply_message(
            ReplyMessageRequest(
                replyToken=reply_token,
                messages=[msg],
            )
        )


def make_quick_reply(buttons: List[Tuple[str, str]]) -> QuickReply:
    items = [QuickReplyItem(action=MessageAction(label=label, text=text)) for label, text in buttons]
    return QuickReply(items=items)


# =========================
# State machine (in-memory)
# =========================
@dataclass
class Session:
    stage: str
    cart: List[Dict[str, Any]]
    temp_item_key: Optional[str] = None
    temp_flavor: Optional[str] = None
    pickup_method: Optional[str] = None
    pickup_date: Optional[str] = None
    pickup_time: Optional[str] = None

    receiver_name: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None  # 宅配才需要
    note: str = ""


SESSIONS: Dict[str, Session] = {}


def get_session(user_id: str) -> Session:
    if user_id not in SESSIONS:
        SESSIONS[user_id] = Session(stage="IDLE", cart=[])
    return SESSIONS[user_id]


def reset_session(user_id: str):
    SESSIONS[user_id] = Session(stage="IDLE", cart=[])


def now_tpe_str() -> str:
    return datetime.now(TZ_TAIPEI).strftime("%Y-%m-%d %H:%M:%S")


def gen_order_id() -> str:
    return f"UOO-{datetime.now(TZ_TAIPEI).strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"


def calc_subtotal(cart: List[Dict[str, Any]]) -> int:
    return sum(int(x["subtotal"]) for x in cart)


def calc_ship_fee(subtotal: int, pickup_method: str) -> int:
    if pickup_method != "宅配":
        return 0
    return 0 if subtotal >= FREE_SHIP_THRESHOLD else SHIP_FEE


def cart_summary(cart: List[Dict[str, Any]]) -> str:
    if not cart:
        return "（目前尚未選擇任何品項）"
    lines = []
    for x in cart:
        f = f"（{x['flavor']}）" if x.get("flavor") else ""
        lines.append(f"- {x['label']}{f} x{x['qty']} = {x['subtotal']}")
    return "\n".join(lines)


def build_date_buttons_3_to_14() -> List[Tuple[str, str]]:
    """
    只提供：今天起 +3 天 ~ +14 天（共 12 天）
    Quick Reply 最多 13 個（含取消剛好 13）
    """
    today = datetime.now(TZ_TAIPEI).date()
    buttons: List[Tuple[str, str]] = []
    for offset in range(PREORDER_MIN_DAYS, PREORDER_MAX_DAYS + 1):
        dd = today + timedelta(days=offset)
        label = dd.strftime("%m/%d")
        text = f"DATE:{dd.strftime('%Y-%m-%d')}"
        buttons.append((label, text))
    buttons.append(("❌ 取消", "CANCEL"))
    return buttons


def qty_buttons(min_qty: int) -> List[Tuple[str, str]]:
    opts = [min_qty, min_qty + 1, min_qty + 2, min_qty + 3, min_qty + 4]
    buttons = [(str(n), f"QTY:{n}") for n in opts]
    buttons.append(("⬅️ 返回品項", "START_ORDER"))
    buttons.append(("❌ 取消", "CANCEL"))
    return buttons


def time_slot_buttons() -> List[Tuple[str, str]]:
    buttons = [(slot, f"TIME:{slot}") for slot in PICKUP_TIME_SLOTS]
    buttons.append(("⬅️ 返回日期", "BACK_TO_DATE"))
    buttons.append(("❌ 取消", "CANCEL"))
    return buttons


# =========================
# Flow handlers
# =========================
def handle_start_order(reply_token: str, user_id: str):
    s = get_session(user_id)
    s.stage = "CHOOSE_ITEM"
    s.temp_item_key = None
    s.temp_flavor = None

    qr = make_quick_reply([
        ("🍰 達克瓦茲", "ITEM:dacquoise"),
        ("🥐 司康", "ITEM:scone"),
        ("🍮 可麗露", "ITEM:canele"),
        ("🍞 奶酥厚片", "ITEM:toast"),
        ("➡️ 下一步", "NEXT_TO_PICKUP"),
        ("❌ 取消", "CANCEL"),
    ])
    reply_text(reply_token, "請選擇要購買的品項：", quick=qr)


def handle_choose_item(reply_token: str, user_id: str, item_key: str):
    s = get_session(user_id)
    if item_key not in ITEM_LABELS:
        reply_text(reply_token, "品項不正確，請重新選擇。")
        return

    s.temp_item_key = item_key
    s.temp_flavor = None

    if item_key in ["dacquoise", "toast"]:
        s.stage = "CHOOSE_FLAVOR"
        qr = make_quick_reply(
            [(f, f"FLAVOR:{f}") for f in FLAVORS] +
            [("⬅️ 返回品項", "START_ORDER"), ("❌ 取消", "CANCEL")]
        )
        reply_text(reply_token, f"你選了，請選口味：", quick=qr)
        return

    s.stage = "CHOOSE_QTY"
    qr = make_quick_reply(qty_buttons(1))
    reply_text(reply_token, f"你選了，請選數量：", quick=qr)


def handle_choose_flavor(reply_token: str, user_id: str, flavor: str):
    s = get_session(user_id)
    if s.temp_item_key not in ["dacquoise", "toast"]:
        reply_text(reply_token, "目前不在選口味流程，請點「我要下單」重新開始。")
        return
    if flavor not in FLAVORS:
        reply_text(reply_token, "口味不在清單內，請重新選擇。")
        return

    s.temp_flavor = flavor
    s.stage = "CHOOSE_QTY"

    min_qty = 2 if s.temp_item_key == "dacquoise" else 1
    qr = make_quick_reply(qty_buttons(min_qty))
    reply_text(reply_token, f"口味：{flavor}\n請選數量：", quick=qr)


def add_to_cart(user_id: str, item_key: str, flavor: Optional[str], qty: int):
    label = ITEM_LABELS[item_key]
    unit = PRICES[item_key]
    subtotal = qty * unit

    s = get_session(user_id)
    for x in s.cart:
        if x["item_key"] == item_key and x.get("flavor") == flavor:
            x["qty"] += qty
            x["subtotal"] += subtotal
            return

    s.cart.append({
        "item_key": item_key,
        "label": label,
        "flavor": flavor,
        "qty": qty,
        "unit_price": unit,
        "subtotal": subtotal,
    })


def handle_choose_qty(reply_token: str, user_id: str, qty: int):
    s = get_session(user_id)
    item_key = s.temp_item_key
    if not item_key:
        reply_text(reply_token, "尚未選擇品項，請點「我要下單」重新開始。")
        return

    if item_key == "dacquoise" and qty < 2:
        reply_text(reply_token, "達克瓦茲每個口味最低 2 顆，請重新選擇。")
        return

    flavor = s.temp_flavor if item_key in ["dacquoise", "toast"] else None
    add_to_cart(user_id, item_key, flavor, qty)

    s.temp_item_key = None
    s.temp_flavor = None
    s.stage = "CHOOSE_ITEM"

    subtotal = calc_subtotal(s.cart)
    msg = "✅ 已加入購物車\n\n" + cart_summary(s.cart) + f"\n\n目前小計：{subtotal}"
    qr = make_quick_reply([
        ("➕ 再加購", "START_ORDER"),
        ("➡️ 下一步", "NEXT_TO_PICKUP"),
        ("❌ 取消", "CANCEL"),
    ])
    reply_text(reply_token, msg, quick=qr)


def handle_next_to_pickup(reply_token: str, user_id: str):
    s = get_session(user_id)
    if not s.cart:
        qr = make_quick_reply([
            ("➕ 先選品項", "START_ORDER"),
            ("❌ 取消", "CANCEL"),
        ])
        reply_text(reply_token, "你目前還沒選品項喔，先選品項再結帳。", quick=qr)
        return

    s.stage = "CHOOSE_PICKUP"
    qr = make_quick_reply([
        ("🏠 店取", "PICKUP:店取"),
        ("🚚 宅配", "PICKUP:宅配"),
        ("⬅️ 返回加購", "START_ORDER"),
        ("❌ 取消", "CANCEL"),
    ])
    reply_text(reply_token, "請選擇取貨方式：", quick=qr)


def handle_pickup(reply_token: str, user_id: str, method: str):
    s = get_session(user_id)
    if method not in ["店取", "宅配"]:
        reply_text(reply_token, "取貨方式不正確，請重新選擇。")
        return

    s.pickup_method = method
    s.stage = "CHOOSE_DATE"
    s.pickup_date = None
    s.pickup_time = None

    qr = make_quick_reply(build_date_buttons_3_to_14())
    reply_text(reply_token, f"你選擇。\n請選擇取貨日期（僅提供 +3～+14 天）：", quick=qr)


def handle_date(reply_token: str, user_id: str, date_str: str):
    s = get_session(user_id)
    try:
        y, m, d = map(int, date_str.split("-"))
        dd = date(y, m, d)
    except Exception:
        reply_text(reply_token, "日期格式錯誤，請重新選擇。")
        return

    today = datetime.now(TZ_TAIPEI).date()
    delta = (dd - today).days
    if delta < PREORDER_MIN_DAYS or delta > PREORDER_MAX_DAYS:
        reply_text(reply_token, f"取貨日期僅提供下單日起第 {PREORDER_MIN_DAYS} 天～第 {PREORDER_MAX_DAYS} 天，請重新選擇。")
        return

    s.pickup_date = date_str

    if s.pickup_method == "店取":
        s.stage = "CHOOSE_TIME"
        qr = make_quick_reply(time_slot_buttons())
        reply_text(reply_token, "請選擇店取時段：", quick=qr)
        return

    # 宅配：先收姓名 → 電話 → 地址
    s.stage = "INPUT_NAME"
    reply_text(reply_token, "請輸入收件人姓名：")


def handle_back_to_date(reply_token: str, user_id: str):
    s = get_session(user_id)
    if not s.pickup_method:
        handle_next_to_pickup(reply_token, user_id)
        return
    s.stage = "CHOOSE_DATE"
    s.pickup_date = None
    s.pickup_time = None
    qr = make_quick_reply(build_date_buttons_3_to_14())
    reply_text(reply_token, f"請重新選擇取貨日期（僅提供 +3～+14 天）：", quick=qr)


def handle_time(reply_token: str, user_id: str, slot: str):
    s = get_session(user_id)
    if s.pickup_method != "店取":
        reply_text(reply_token, "目前不是店取流程，請重新開始。")
        return
    if slot not in PICKUP_TIME_SLOTS:
        reply_text(reply_token, "時段不正確，請重新選擇。")
        return

    s.pickup_time = slot
    s.stage = "INPUT_NAME"
    reply_text(reply_token, "請輸入取件人姓名：")


def handle_cancel(reply_token: str, user_id: str):
    reset_session(user_id)
    reply_text(reply_token, "已取消本次下單流程。需要再下單請點「我要下單」。")


def handle_input_name(reply_token: str, user_id: str, name: str):
    name = name.strip()
    if not name or len(name) > 20:
        reply_text(reply_token, "姓名格式不正確，請重新輸入（1～20字）。")
        return
    s = get_session(user_id)
    s.receiver_name = name
    s.stage = "INPUT_PHONE"
    reply_text(reply_token, "請輸入電話（例如 0912345678）：")


def handle_input_phone(reply_token: str, user_id: str, phone: str):
    phone = phone.strip()
    if not phone.startswith("09") or len(phone) != 10 or not phone.isdigit():
        reply_text(reply_token, "電話格式不正確，請輸入 09xxxxxxxx（10 碼數字）。")
        return

    s = get_session(user_id)
    s.phone = phone

    if s.pickup_method == "宅配":
        s.stage = "INPUT_ADDRESS"
        reply_text(reply_token, "請輸入宅配地址（縣市＋路名門牌＋樓層/房號）：")
        return

    # 店取：直接送出
    submit_order(reply_token, user_id, event_user_id=user_id)


def handle_input_address(reply_token: str, user_id: str, address: str):
    address = address.strip()
    if not address or len(address) < 6:
        reply_text(reply_token, "地址看起來太短，請輸入完整地址（縣市＋路名門牌＋樓層/房號）。")
        return

    s = get_session(user_id)
    s.address = address
    submit_order(reply_token, user_id, event_user_id=user_id)


def submit_order(reply_token: str, user_id: str, event_user_id: str):
    s = get_session(user_id)
    if not s.cart or not s.pickup_method or not s.pickup_date or not s.receiver_name or not s.phone:
        reply_text(reply_token, "訂單資訊不完整，請點「我要下單」重新開始。")
        return
    if s.pickup_method == "店取" and not s.pickup_time:
        reply_text(reply_token, "缺少店取時段，請重新選擇。")
        return
    if s.pickup_method == "宅配" and not s.address:
        reply_text(reply_token, "缺少宅配地址，請重新輸入。")
        return

    subtotal = calc_subtotal(s.cart)
    ship_fee = calc_ship_fee(subtotal, s.pickup_method)
    total = subtotal + ship_fee

    order_id = gen_order_id()
    created_at = now_tpe_str()

    items_json = json.dumps(
        {
            "cart": s.cart,
            "pickup_method": s.pickup_method,
            "pickup_date": s.pickup_date,
            "pickup_time": s.pickup_time or "",
            "receiver_name": s.receiver_name,
            "phone": s.phone,
            "address": s.address or "",
            "subtotal": subtotal,
            "ship_fee": ship_fee,
            "total": total,
        },
        ensure_ascii=False,
    )

    # note：留給你人工對帳/出貨最常用資訊
    if s.pickup_method == "店取":
        note = f"取件人:{s.receiver_name}｜電話:{s.phone}"
    else:
        note = f"收件人:{s.receiver_name}｜電話:{s.phone}｜地址:{s.address}"

    row = [
        created_at,            # created_at
        event_user_id or "",   # user_id
        "",                    # display_name
        order_id,              # order_id
        items_json,            # items_json
        s.pickup_method,       # pickup_method
        s.pickup_date,         # pickup_date
        s.pickup_time or "",   # pickup_time
        note,                  # note
        str(total),            # amount
        "UNPAID",              # pay_status
        "",                    # linepay_transaction_id
    ]

    try:
        append_order_row(row)
    except Exception as e:
        reply_text(reply_token, f"⚠️ 建單成功但寫入 Orders 失敗：{e}\n請把這段錯誤貼給我，我幫你修。")
        return

    ship_line = ""
    if s.pickup_method == "宅配":
        ship_line = f"\n宅配運費：{ship_fee}（滿{FREE_SHIP_THRESHOLD}免運）"

    time_line = ""
    if s.pickup_method == "店取":
        time_line = f"\n店取時段：{s.pickup_time}"

    msg = (
        "✅ 已建立訂單！\n\n"
        f"訂單編號：{order_id}\n"
        f"取貨方式：{s.pickup_method}\n"
        f"取貨日期：{s.pickup_date}"
        f"{time_line}\n\n"
        "🧾 訂單內容\n"
        f"{cart_summary(s.cart)}\n\n"
        f"小計：{subtotal}"
        f"{ship_line}\n"
        f"應付總額：{total}\n\n"
        "接下來請依「付款說明」完成匯款。\n"
        "匯款後回覆：已轉帳 訂單編號 末五碼12345\n"
        "（核帳後依序出貨/通知取貨）"
    )

    reset_session(user_id)
    reply_text(reply_token, msg)


# =========================
# Routes
# =========================
@app.get("/")
def health():
    return {"ok": True}


@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body_bytes = await request.body()
    body = body_bytes.decode("utf-8")

    try:
        events = parser.parse(body, signature)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid signature/body: {e}")

    for event in events:
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessageContent):
            user_text = (event.message.text or "").strip()
            user_id = event.source.user_id if event.source else ""
            s = get_session(user_id)

            # ---------- 基礎指令 ----------
            if user_text in ["menu", "選單", "開始", "hi", "hello", "你好"]:
                reply_text(event.reply_token, MENU_TEXT)
                continue

            if user_text in ["甜點", "菜單"]:
                reply_text(event.reply_token, DESSERT_MENU_TEXT)
                continue

            if user_text in ["取貨說明", "取貨"]:
                reply_text(event.reply_token, PICKUP_TEXT)
                continue

            if user_text in ["付款說明", "付款", "匯款"]:
                reply_text(event.reply_token, PAY_TEXT)
                continue

            # ---------- 啟動按鈕下單 ----------
            if user_text in ["我要下單", "下單", "START_ORDER"]:
                handle_start_order(event.reply_token, user_id)
                continue

            # ---------- 取消 ----------
            if user_text == "CANCEL":
                handle_cancel(event.reply_token, user_id)
                continue

            # ---------- Quick Reply 指令 ----------
            if user_text.startswith("ITEM:"):
                handle_choose_item(event.reply_token, user_id, user_text.split(":", 1)[1])
                continue

            if user_text.startswith("FLAVOR:"):
                handle_choose_flavor(event.reply_token, user_id, user_text.split(":", 1)[1])
                continue

            if user_text.startswith("QTY:"):
                try:
                    qty = int(user_text.split(":", 1)[1])
                except Exception:
                    reply_text(event.reply_token, "數量不正確，請重新選擇。")
                    continue
                handle_choose_qty(event.reply_token, user_id, qty)
                continue

            if user_text == "NEXT_TO_PICKUP":
                handle_next_to_pickup(event.reply_token, user_id)
                continue

            if user_text.startswith("PICKUP:"):
                handle_pickup(event.reply_token, user_id, user_text.split(":", 1)[1])
                continue

            if user_text.startswith("DATE:"):
                handle_date(event.reply_token, user_id, user_text.split(":", 1)[1])
                continue

            if user_text == "BACK_TO_DATE":
                handle_back_to_date(event.reply_token, user_id)
                continue

            if user_text.startswith("TIME:"):
                handle_time(event.reply_token, user_id, user_text.split(":", 1)[1])
                continue

            # ---------- 依 stage 接收文字輸入 ----------
            if s.stage == "INPUT_NAME":
                handle_input_name(event.reply_token, user_id, user_text)
                continue

            if s.stage == "INPUT_PHONE":
                handle_input_phone(event.reply_token, user_id, user_text)
                continue

            if s.stage == "INPUT_ADDRESS":
                handle_input_address(event.reply_token, user_id, user_text)
                continue

            # ---------- 其他：提示 ----------
            qr = make_quick_reply([
                ("🧾 我要下單", "START_ORDER"),
                ("🍰 看甜點", "甜點"),
                ("📦 取貨說明", "取貨說明"),
                ("💸 付款說明", "付款說明"),
            ])
            reply_text(event.reply_token, "我建議你用按鈕下單比較快：", quick=qr)

    return "OK"
