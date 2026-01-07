import os
import json
import base64
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List
from urllib.parse import parse_qs

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

# 你的分頁叫 orders（你剛說的）
SHEET_NAME = os.getenv("SHEET_NAME", "orders").strip()

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


def new_session() -> Dict[str, Any]:
    return {
        "cart": [],  # list of lines
        "state": "IDLE",

        # item picking
        "pending_item": None,
        "pending_flavor": None,

        # checkout
        "pickup_method": None,   # "店取" / "宅配"
        "pickup_date": None,     # YYYY-MM-DD
        "pickup_time": None,     # 10:00-12:00 etc
        "pickup_name": None,

        "delivery_date": None,   # YYYY-MM-DD
        "delivery_name": None,
        "delivery_phone": None,
        "delivery_address": None,

        "note": "",
    }


def get_session(user_id: str) -> Dict[str, Any]:
    if user_id not in SESSIONS:
        SESSIONS[user_id] = new_session()
    return SESSIONS[user_id]


# =========================
# Menu data
# =========================
DACQUOISE_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]
TOAST_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

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

# 商品：可麗露改成 6顆/盒 490，只能一盒一盒買
ITEMS = {
    "dacquoise": {
        "label": "達克瓦茲",
        "unit_price": 95,
        "has_flavor": True,
        "flavors": DACQUOISE_FLAVORS,
        "min_qty": 2,
        "qty_step": 1,
        "qty_max": 12,
        "unit_label": "顆",
    },
    "scone": {
        "label": "原味司康",
        "unit_price": 65,
        "has_flavor": False,
        "flavors": [],
        "min_qty": 1,
        "qty_step": 1,
        "qty_max": 12,
        "unit_label": "顆",
    },
    "canele_box": {
        "label": "可麗露 6顆/盒",
        "unit_price": 490,  # 一盒 490
        "has_flavor": False,
        "flavors": [],
        "min_qty": 1,       # 只能一盒一盒買 => qty 表示盒數
        "qty_step": 1,
        "qty_max": 10,      # 你說要 10 張/最多 10（這裡就用 10 盒）
        "unit_label": "盒",
    },
    "toast": {
        "label": "伊思尼奶酥厚片",
        "unit_price": 85,
        "has_flavor": True,
        "flavors": TOAST_FLAVORS,
        "min_qty": 1,
        "qty_step": 1,
        "qty_max": 12,
        "unit_label": "片",
    },
}


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
        # ✅ 重要：sheet 名稱用引號包起來，避免 parse range 失敗
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


def friendly_mmdd_weekday(dt: datetime) -> str:
    w = "一二三四五六日"[dt.weekday()]
    return f"{dt.month}/{dt.day}（{w}）"


def date_choices_3_to_14() -> List[datetime]:
    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    return [today + timedelta(days=i) for i in range(3, 15)]  # 3~14


def parse_postback(data: str) -> Dict[str, str]:
    # data: "act=ITEM&k=dacquoise"
    q = parse_qs(data, keep_blank_values=True)
    flat = {k: (v[0] if v else "") for k, v in q.items()}
    return flat


def reply(reply_token: str, messages: List[dict]):
    # ✅ 永遠只 reply 一次（避免你之前的 400）
    messaging_api.reply_message(
        ReplyMessageRequest(
            replyToken=reply_token,
            messages=messages,
        )
    )


def push(user_id: str, messages: List[dict]):
    messaging_api.push_message(
        PushMessageRequest(
            to=user_id,
            messages=messages,
        )
    )


# =========================
# Cart operations (merge lines)
# =========================
def find_line_index(cart: List[dict], item_key: str, flavor: str) -> Optional[int]:
    for i, x in enumerate(cart):
        if x["item_key"] == item_key and x.get("flavor", "") == flavor:
            return i
    return None


def add_to_cart(user_id: str, item_key: str, flavor: str, qty: int):
    sess = get_session(user_id)
    meta = ITEMS[item_key]
    if meta["has_flavor"] and not flavor:
        raise ValueError("missing flavor")
    if qty < meta["min_qty"]:
        raise ValueError(f"qty must be >= {meta['min_qty']}")

    unit = meta["unit_price"]
    idx = find_line_index(sess["cart"], item_key, flavor)

    if idx is None:
        sess["cart"].append({
            "item_key": item_key,
            "label": meta["label"],
            "flavor": flavor,
            "qty": qty,
            "unit_price": unit,
            "subtotal": unit * qty,
            "unit_label": meta.get("unit_label", ""),
        })
    else:
        sess["cart"][idx]["qty"] += qty
        sess["cart"][idx]["subtotal"] = sess["cart"][idx]["qty"] * unit


def set_line_qty(sess: Dict[str, Any], idx: int, new_qty: int):
    if idx < 0 or idx >= len(sess["cart"]):
        return
    item_key = sess["cart"][idx]["item_key"]
    meta = ITEMS[item_key]
    if new_qty < meta["min_qty"]:
        new_qty = meta["min_qty"]
    if new_qty > meta["qty_max"]:
        new_qty = meta["qty_max"]
    sess["cart"][idx]["qty"] = new_qty
    sess["cart"][idx]["subtotal"] = new_qty * sess["cart"][idx]["unit_price"]


def remove_line(sess: Dict[str, Any], idx: int):
    if idx < 0 or idx >= len(sess["cart"]):
        return
    sess["cart"].pop(idx)


def cart_lines_text(cart: List[dict]) -> str:
    lines = []
    for x in cart:
        name = x["label"]
        if x.get("flavor"):
            name += f"（{x['flavor']}）"
        unit_label = x.get("unit_label", "")
        lines.append(f"• {name}  x{x['qty']}{unit_label} ＝ NT${x['subtotal']}")
    return "\n".join(lines) if lines else "（目前購物車是空的）"


# =========================
# Flex Builders (Postback, no code shown to user)
# =========================
def pb_action(label: str, data: str, display_text: Optional[str] = None) -> dict:
    a = {
        "type": "postback",
        "label": label,
        "data": data,
    }
    if display_text:
        a["displayText"] = display_text
    return a


def btn_primary(label: str, data: str, display_text: Optional[str] = None) -> dict:
    return {"type": "button", "style": "primary", "action": pb_action(label, data, display_text)}


def btn_secondary(label: str, data: str, display_text: Optional[str] = None) -> dict:
    return {"type": "button", "style": "secondary", "action": pb_action(label, data, display_text)}


def bubble(title: str, body_contents: List[dict], size: str = "mega") -> dict:
    return {
        "type": "bubble",
        "size": size,
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [{"type": "text", "text": title, "weight": "bold", "size": "xl"}] + body_contents
        }
    }


def flex_message(alt: str, contents: dict) -> dict:
    # LINE 要求：altText 不可空、contents 不可空
    # 若 builder 回傳 None / {}，就不要送 flex，改送 text
    if not alt or not str(alt).strip():
        alt = "訊息"
    if not contents or not isinstance(contents, dict) or not contents.get("type"):
        return {"type": "text", "text": "系統忙碌中，請再按一次或輸入：我要下單 / 甜點"}
    return {"type": "flex", "altText": alt, "contents": contents}



def build_sweets_info_flex() -> dict:
    # 甜點：只介紹 + 我要下單
    body = [
        {"type": "text", "text": "全部甜點需提前 3 天預訂", "size": "sm", "color": "#666666"},
        {"type": "separator", "margin": "lg"},
        btn_primary("🛒 我要下單", "act=START", "我要下單"),
        btn_secondary("📦 取貨說明", "act=INFO_PICKUP", "取貨說明"),
        btn_secondary("💰 付款說明", "act=INFO_PAY", "付款說明"),
    ]
    return bubble("UooUoo 甜點訂購", body)


def build_product_menu_flex() -> dict:
    def item_btn(item_key: str) -> dict:
        meta = ITEMS[item_key]
        return {
            "type": "button",
            "style": "primary",
            "action": pb_action(
                f"{meta['label']}｜NT${meta['unit_price']}",
                f"act=ITEM&k={item_key}",
                f"選擇 {meta['label']}"
            )
        }

    body = [
        {"type": "text", "text": "請點選要購買的商品", "size": "sm", "color": "#666666"},
        item_btn("dacquoise"),
        item_btn("scone"),
        item_btn("canele_box"),
        item_btn("toast"),
        {"type": "separator", "margin": "lg"},
        btn_secondary("🧾 前往結帳", "act=CHECKOUT", "前往結帳"),
        btn_secondary("🗑 清空重來", "act=RESET", "清空重來"),
    ]
    return bubble("請選擇商品", body)


def build_flavor_flex(item_key: str) -> dict:
    meta = ITEMS[item_key]
    body = [
        {"type": "text", "text": f"你選了：{meta['label']}", "size": "sm", "color": "#666666"},
        {"type": "text", "text": "請選口味（口味不可混）", "size": "sm", "color": "#666666"},
        {"type": "separator", "margin": "lg"},
    ]

    # 口味按鈕（每個都用 postback）
    for f in meta["flavors"]:
        body.append(btn_primary(f, f"act=FLAVOR&k={item_key}&f={f}", f"口味 {f}"))

    body.append(btn_secondary("↩️ 返回選單", "act=START", "返回選單"))
    return bubble("請選口味", body)


def build_qty_flex(item_key: str, flavor: str) -> dict:
    meta = ITEMS[item_key]
    min_q = meta["min_qty"]
    max_q = meta["qty_max"]
    unit_label = meta.get("unit_label", "")

    body = [
        {"type": "text", "text": f"商品：{meta['label']}" + (f"（{flavor}）" if flavor else ""), "size": "sm"},
        {"type": "text", "text": f"請選數量（{min_q}～{max_q}）", "size": "sm", "color": "#666666"},
        {"type": "separator", "margin": "lg"},
    ]

    # 分兩頁（最多 12），避免太長；可麗露你要 10 => 也在這裡
    choices = list(range(min_q, max_q + 1))
    pages = [choices[:6], choices[6:12]] if len(choices) > 6 else [choices]

    bubbles = []
    for page in pages:
        page_body = body.copy()
        for q in page:
            page_body.append(btn_primary(
                f"{q}{unit_label}",
                f"act=QTY&k={item_key}&f={flavor}&q={q}",
                f"數量 {q}{unit_label}"
            ))
        page_body.append(btn_secondary("↩️ 返回選單", "act=START", "返回選單"))
        bubbles.append(bubble("請選數量", page_body))

    if len(bubbles) == 1:
        return bubbles[0]
    return {"type": "carousel", "contents": bubbles}


def build_cart_summary_flex(sess: Dict[str, Any]) -> dict:
    cart = sess["cart"]
    total = cart_total(cart)

    body = [
        {"type": "text", "text": "結帳內容清單", "weight": "bold", "size": "md"},
        {"type": "text", "text": cart_lines_text(cart), "size": "sm", "wrap": True},
        {"type": "separator", "margin": "lg"},
        {"type": "text", "text": f"目前小計：NT${total}", "weight": "bold", "size": "xl"},
        {"type": "text", "text": "下一步：你要繼續加購，或前往結帳", "size": "sm", "color": "#666666"},
        btn_primary("➕ 繼續加購", "act=START", "繼續加購"),
        btn_primary("🧾 前往結帳", "act=CHECKOUT", "前往結帳"),
        btn_secondary("✏️ 修改購物車", "act=EDIT_CART", "修改購物車"),
    ]
    return bubble("✅ 已加入購物車", body)


def build_cart_edit_flex(sess: Dict[str, Any]) -> dict:
    cart = sess["cart"]
    if not cart:
        return bubble("購物車是空的", [
            btn_primary("🛒 開始下單", "act=START", "我要下單"),
        ])

    bubbles = []
    for i, x in enumerate(cart):
        name = x["label"] + (f"（{x['flavor']}）" if x.get("flavor") else "")
        unit_label = x.get("unit_label", "")
        qty = x["qty"]
        subtotal = x["subtotal"]

        body = [
            {"type": "text", "text": name, "weight": "bold", "size": "lg", "wrap": True},
            {"type": "text", "text": f"數量：{qty}{unit_label}", "size": "sm", "color": "#666666"},
            {"type": "text", "text": f"小計：NT${subtotal}", "size": "sm", "color": "#666666"},
            {"type": "separator", "margin": "lg"},
            btn_primary("➖ 減少數量", f"act=DEC&i={i}", "減少數量"),
            btn_primary("➕ 增加數量", f"act=INC&i={i}", "增加數量"),
            btn_secondary("🗑 刪除此品項", f"act=DEL&i={i}", "刪除品項"),
        ]
        bubbles.append(bubble("修改購物車", body, size="mega"))

    # 最後一張總結
    total = cart_total(cart)
    bubbles.append(bubble("修改完成後", [
        {"type": "text", "text": f"目前小計：NT${total}", "weight": "bold", "size": "xl"},
        btn_primary("✅ 回到小計/結帳", "act=CART_SUMMARY", "回到小計"),
        btn_secondary("➕ 繼續加購", "act=START", "繼續加購"),
        btn_secondary("🗑 清空重來", "act=RESET", "清空重來"),
    ], size="mega"))

    return {"type": "carousel", "contents": bubbles}


def build_pickup_method_flex() -> dict:
    body = [
        {"type": "text", "text": "請選擇店取或宅配", "size": "sm", "color": "#666666"},
        btn_primary("🏪 店取", "act=PICKUP&method=store", "店取"),
        btn_primary("🚚 冷凍宅配", "act=PICKUP&method=delivery", "冷凍宅配"),
        btn_secondary("↩️ 返回小計", "act=CART_SUMMARY", "返回小計"),
    ]
    return bubble("取貨方式", body)


def build_date_picker_flex(kind: str) -> dict:
    # kind: "store" or "delivery"
    dates = date_choices_3_to_14()  # 12 dates
    title = "店取日期（3～14天內）" if kind == "store" else "宅配希望到貨日（3～14天內）"

    # 你說希望可愛按鈕 + 不用輸入；這裡用 carousel 分兩頁 6+6
    pages = [dates[:6], dates[6:]]
    bubbles = []

    for page in pages:
        body = [
            {"type": "text", "text": "請點選日期（不需輸入）", "size": "sm", "color": "#666666"},
            {"type": "separator", "margin": "lg"},
        ]
        for dt in page:
            dstr = dt.strftime("%Y-%m-%d")
            label = friendly_mmdd_weekday(dt)
            body.append(btn_primary(
                label,
                f"act=DATE&kind={kind}&d={dstr}",
                f"已選日期：{label}"
            ))
        body.append(btn_secondary("↩️ 返回取貨方式", "act=CHECKOUT", "返回取貨方式"))
        bubbles.append(bubble(title, body))

    return {"type": "carousel", "contents": bubbles}


def build_time_picker_flex() -> dict:
    body = [
        {"type": "text", "text": "請選店取時段", "size": "sm", "color": "#666666"},
        btn_primary("10:00-12:00", "act=TIME&t=10:00-12:00", "時段 10:00-12:00"),
        btn_primary("12:00-14:00", "act=TIME&t=12:00-14:00", "時段 12:00-14:00"),
        btn_primary("14:00-16:00", "act=TIME&t=14:00-16:00", "時段 14:00-16:00"),
        btn_secondary("↩️ 回選日期", "act=DATE_PICKER&kind=store", "回選日期"),
    ]
    return bubble("店取時段", body)


def build_checkout_summary_flex(sess: Dict[str, Any]) -> dict:
    cart = sess["cart"]
    total = cart_total(cart)
    fee = shipping_fee(total) if sess.get("pickup_method") == "宅配" else 0
    grand = total + fee

    # 顯示清單 + 小計（你要的小計前清單）
    lines = cart_lines_text(cart)

    if sess.get("pickup_method") == "店取":
        dt = sess.get("pickup_date", "")
        t = sess.get("pickup_time", "")
        body = [
            {"type": "text", "text": "✅ 店取資訊已選好", "weight": "bold", "size": "lg"},
            {"type": "text", "text": f"📅 日期：{dt}", "size": "sm", "wrap": True},
            {"type": "text", "text": f"🕒 時段：{t}", "size": "sm", "wrap": True},
            {"type": "text", "text": f"📍 地址：{PICKUP_ADDRESS}", "size": "sm", "wrap": True},
            {"type": "separator", "margin": "lg"},
            {"type": "text", "text": "結帳內容清單", "weight": "bold", "size": "md"},
            {"type": "text", "text": lines, "size": "sm", "wrap": True},
            {"type": "separator", "margin": "lg"},
            {"type": "text", "text": f"目前小計：NT${total}", "weight": "bold", "size": "xl"},
            {"type": "text", "text": "下一步請填取件人姓名（按下方按鈕）", "size": "sm", "color": "#666666"},
            btn_primary("✍️ 填取件人姓名", "act=ASK_NAME&kind=store", "填取件人姓名"),
            btn_secondary("✏️ 修改購物車", "act=EDIT_CART", "修改購物車"),
            btn_secondary("➕ 繼續加購", "act=START", "繼續加購"),
        ]
        return bubble("店取結帳", body)

    # 宅配
    d = sess.get("delivery_date", "")
    body = [
        {"type": "text", "text": "✅ 宅配資訊已選好", "weight": "bold", "size": "lg"},
        {"type": "text", "text": f"📅 希望到貨日：{d}（不保證準時）", "size": "sm", "wrap": True},
        {"type": "separator", "margin": "lg"},
        {"type": "text", "text": "結帳內容清單", "weight": "bold", "size": "md"},
        {"type": "text", "text": lines, "size": "sm", "wrap": True},
        {"type": "separator", "margin": "lg"},
        {"type": "text", "text": f"小計：NT${total}", "weight": "bold", "size": "lg"},
        {"type": "text", "text": f"運費：NT${fee}", "size": "sm", "color": "#666666"},
        {"type": "text", "text": f"應付總額：NT${grand}", "weight": "bold", "size": "xl"},
        {"type": "text", "text": "下一步請填寫收件資料（姓名 / 電話 / 地址）", "size": "sm", "color": "#666666"},
        btn_primary("✍️ 填宅配收件人姓名", "act=ASK_NAME&kind=delivery", "填宅配姓名"),
        btn_secondary("✏️ 修改購物車", "act=EDIT_CART", "修改購物車"),
        btn_secondary("➕ 繼續加購", "act=START", "繼續加購"),
    ]
    return bubble("宅配結帳", body)


# =========================
# Order persistence
# =========================
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
        now_str(),                              # created_at
        user_id,                                # user_id
        display_name,                           # display_name
        order_id,                               # order_id
        json.dumps({"cart": cart}, ensure_ascii=False),  # items_json
        pickup_method,                          # pickup_method
        pickup_date,                            # pickup_date (宅配=希望到貨日)
        pickup_time,                            # pickup_time
        note,                                   # note
        total,                                  # amount (不含運)
        "UNPAID",                               # pay_status
        "",                                     # linepay_transaction_id
    ]

    ok = append_order_row(row)
    if not ok:
        print("[WARN] write sheet failed (but continue).")

    return order_id


def reset_session(sess: Dict[str, Any]):
    sess.clear()
    sess.update(new_session())


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
# LINE Handlers
# =========================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event: MessageEvent):
    user_id = event.source.user_id
    text = event.message.text.strip()
    sess = get_session(user_id)

    # 你不想抓 profile，就先固定
    display_name = "LINE用戶"

    # 甜點：只顯示資訊＋我要下單
    if text in ["甜點"]:
        reply(event.reply_token, [flex_message("甜點訂購", build_sweets_info_flex())])
        return

    # 我要下單：直接開始
    if text in ["我要下單", "下單", "開始下單"]:
        sess["state"] = "ORDERING"
        reply(event.reply_token, [flex_message("甜點選單", build_product_menu_flex())])
        return

    # 付款/取貨說明
    if text in ["取貨說明"]:
        reply(event.reply_token, [{"type": "text", "text": PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE}])
        return
    if text in ["付款說明"]:
        reply(event.reply_token, [{"type": "text", "text": BANK_TRANSFER_TEXT}])
        return

    # 已轉帳訊息
    if text.startswith("已轉帳"):
        reply(event.reply_token, [{
            "type": "text",
            "text": "收到，我們會核對帳款後依訂單號安排出貨。\n若需補充資訊，也可以直接在這裡留言。"
        }])
        return

    # 文字輸入收件/取件資訊（日期不需要輸入了）
    if sess["state"] == "WAIT_PICKUP_NAME":
        sess["pickup_name"] = text
        order_id = create_order_and_write_sheet(user_id, display_name)

        total = cart_total(sess["cart"])
        summary = cart_lines_text(sess["cart"])

        reply(event.reply_token, [{
            "type": "text",
            "text":
                "✅ 訂單已建立\n"
                f"訂單編號：{order_id}\n\n"
                f"{summary}\n\n"
                f"取貨方式：店取\n"
                f"取貨日期：{sess.get('pickup_date','')}\n"
                f"取貨時段：{sess.get('pickup_time','')}\n"
                f"店取地址：{PICKUP_ADDRESS}\n\n"
                + BANK_TRANSFER_TEXT
        }])

        # 完成後清空
        reset_session(sess)
        return

    if sess["state"] == "WAIT_DELIVERY_NAME":
        sess["delivery_name"] = text
        sess["state"] = "WAIT_DELIVERY_PHONE"
        reply(event.reply_token, [{"type": "text", "text": "請輸入宅配電話："}])
        return

    if sess["state"] == "WAIT_DELIVERY_PHONE":
        sess["delivery_phone"] = text
        sess["state"] = "WAIT_DELIVERY_ADDRESS"
        reply(event.reply_token, [{"type": "text", "text": "請輸入宅配地址（完整地址）："}])
        return

    if sess["state"] == "WAIT_DELIVERY_ADDRESS":
        sess["delivery_address"] = text

        order_id = create_order_and_write_sheet(user_id, display_name)

        total = cart_total(sess["cart"])
        fee = shipping_fee(total)
        grand = total + fee
        summary = cart_lines_text(sess["cart"])

        reply(event.reply_token, [{
            "type": "text",
            "text":
                "✅ 訂單已建立\n"
                f"訂單編號：{order_id}\n\n"
                f"{summary}\n\n"
                f"取貨方式：冷凍宅配\n"
                f"希望到貨日期：{sess.get('delivery_date','')}（不保證準時）\n"
                f"運費：{fee}\n"
                f"應付總額：{grand}\n\n"
                f"收件人：{sess.get('delivery_name','')}\n"
                f"電話：{sess.get('delivery_phone','')}\n"
                f"地址：{sess.get('delivery_address','')}\n\n"
                + DELIVERY_NOTICE
                + "\n\n"
                + BANK_TRANSFER_TEXT
        }])

        reset_session(sess)
        return

    # fallback（避免客人看到任何程式碼）
    reply(event.reply_token, [{
        "type": "text",
        "text": "請點「甜點」查看資訊，或點「我要下單」開始選購。"
    }])


@handler.add(PostbackEvent)
def handle_postback(event: PostbackEvent):
    user_id = event.source.user_id
    sess = get_session(user_id)

    data = event.postback.data or ""
    p = parse_postback(data)
    act = p.get("act", "")

    # START / menu
    if act == "START":
        sess["state"] = "ORDERING"
        reply(event.reply_token, [flex_message("甜點選單", build_product_menu_flex())])
        return

    if act == "RESET":
        reset_session(sess)
        reply(event.reply_token, [{"type": "text", "text": "已清空，請按「我要下單」重新開始。"}])
        return

    if act == "INFO_PICKUP":
        reply(event.reply_token, [{"type": "text", "text": PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE}])
        return

    if act == "INFO_PAY":
        reply(event.reply_token, [{"type": "text", "text": BANK_TRANSFER_TEXT}])
        return

    # Item select
    if act == "ITEM":
        k = p.get("k", "")
        if k not in ITEMS:
            reply(event.reply_token, [{"type": "text", "text": "品項不存在，請重新開始。"}])
            return

        sess["pending_item"] = k
        sess["pending_flavor"] = None

        if ITEMS[k]["has_flavor"]:
            reply(event.reply_token, [flex_message("選口味", build_flavor_flex(k))])
        else:
            # 直接選數量
            reply(event.reply_token, [flex_message("選數量", build_qty_flex(k, ""))])
        return

    # Flavor select
    if act == "FLAVOR":
        k = p.get("k", "")
        f = p.get("f", "")
        if k not in ITEMS:
            reply(event.reply_token, [{"type": "text", "text": "流程有點亂掉了，請重新開始。"}])
            return
        if f not in ITEMS[k]["flavors"]:
            reply(event.reply_token, [{"type": "text", "text": "口味不正確，請重新選擇。"}])
            return

        sess["pending_item"] = k
        sess["pending_flavor"] = f
        reply(event.reply_token, [flex_message("選數量", build_qty_flex(k, f))])
        return

    # Qty select -> add cart
    if act == "QTY":
        k = p.get("k", "")
        f = p.get("f", "")
        q = int(p.get("q", "0") or 0)

        if k not in ITEMS:
            reply(event.reply_token, [{"type": "text", "text": "流程有點亂掉了，請重新開始。"}])
            return

        # 有口味的商品，f 必須存在
        if ITEMS[k]["has_flavor"] and not f:
            reply(event.reply_token, [{"type": "text", "text": "請先選口味。"}])
            return

        try:
            add_to_cart(user_id, k, f, q)
        except Exception as e:
            reply(event.reply_token, [{"type": "text", "text": f"加入失敗：{e}"}])
            return

        # 清 pending
        sess["pending_item"] = None
        sess["pending_flavor"] = None

        reply(event.reply_token, [flex_message("小計", build_cart_summary_flex(sess))])
        return

    # Cart edit
    if act == "EDIT_CART":
        reply(event.reply_token, [flex_message("修改購物車", build_cart_edit_flex(sess))])
        return

    if act == "CART_SUMMARY":
        reply(event.reply_token, [flex_message("小計", build_cart_summary_flex(sess))])
        return

    if act in ["DEC", "INC", "DEL"]:
        i = int(p.get("i", "-1") or -1)
        if act == "DEL":
            remove_line(sess, i)
        else:
            if 0 <= i < len(sess["cart"]):
                cur = sess["cart"][i]["qty"]
                new_qty = cur - 1 if act == "DEC" else cur + 1
                set_line_qty(sess, i, new_qty)

        reply(event.reply_token, [flex_message("修改購物車", build_cart_edit_flex(sess))])
        return

    # Checkout
    if act == "CHECKOUT":
        if not sess["cart"]:
            reply(event.reply_token, [{"type": "text", "text": "購物車是空的，請先按「我要下單」選商品。"}])
            return
        reply(event.reply_token, [flex_message("取貨方式", build_pickup_method_flex())])
        return

    if act == "PICKUP":
        m = p.get("method", "")
        if m == "store":
            sess["pickup_method"] = "店取"
            # 顯示日期按鈕（不需輸入）
            reply(event.reply_token, [flex_message("店取日期", build_date_picker_flex("store"))])
            return
        if m == "delivery":
            sess["pickup_method"] = "宅配"
            reply(event.reply_token, [flex_message("宅配日期", build_date_picker_flex("delivery"))])
            return

        reply(event.reply_token, [{"type": "text", "text": "取貨方式不正確，請重選。"}])
        return

    if act == "DATE_PICKER":
        kind = p.get("kind", "")
        if kind not in ["store", "delivery"]:
            kind = "store"
        reply(event.reply_token, [flex_message("選日期", build_date_picker_flex(kind))])
        return

    if act == "DATE":
        kind = p.get("kind", "")
        d = p.get("d", "")

        if kind == "store":
            sess["pickup_date"] = d
            # 選時段
            reply(event.reply_token, [flex_message("店取時段", build_time_picker_flex())])
            return

        if kind == "delivery":
            sess["delivery_date"] = d
            # 直接到結帳摘要（接著要填姓名）
            sess["pickup_method"] = "宅配"
            reply(event.reply_token, [flex_message("宅配結帳", build_checkout_summary_flex(sess))])
            return

        reply(event.reply_token, [{"type": "text", "text": "日期選擇失敗，請重選。"}])
        return

    if act == "TIME":
        t = p.get("t", "")
        sess["pickup_time"] = t
        sess["pickup_method"] = "店取"
        reply(event.reply_token, [flex_message("店取結帳", build_checkout_summary_flex(sess))])
        return

    if act == "ASK_NAME":
        kind = p.get("kind", "")
        if kind == "store":
            sess["state"] = "WAIT_PICKUP_NAME"
            reply(event.reply_token, [{"type": "text", "text": "請輸入店取取件人姓名："}])
            return
        if kind == "delivery":
            sess["state"] = "WAIT_DELIVERY_NAME"
            reply(event.reply_token, [{"type": "text", "text": "請輸入宅配收件人姓名："}])
            return

        reply(event.reply_token, [{"type": "text", "text": "流程有點亂掉了，請重新開始。"}])
        return

    # fallback
    reply(event.reply_token, [{"type": "text", "text": "請按「甜點」或「我要下單」開始。"}])
