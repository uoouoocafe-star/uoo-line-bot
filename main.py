import os
import json
import base64
import uuid
import re
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
GSHEET_TAB_NAME = os.getenv("GSHEET_TAB_NAME", "sheet1")
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

# 台灣時區
TZ_TAIPEI = timezone(timedelta(hours=8))

app = FastAPI()

# LINE
line_config = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(CHANNEL_SECRET)

# =========================
# Business rules / prices
# =========================
PREORDER_DAYS = 3
SHIP_FEE = 180
FREE_SHIP_THRESHOLD = 2500

PRICES = {
    "dacquoise": 95,
    "scone": 65,
    "canele": 90,
    "toast": 85,
}

DACQUOISE_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]
TOAST_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

# =========================
# Texts
# =========================
MENU_TEXT = (
    "🍰【UooUoo 甜點訂單】\n\n"
    "你可以輸入：\n"
    "1) 甜點（看菜單）\n"
    "2) 我要下單（看下單格式）\n"
    "3) 取貨說明\n"
    "4) 付款說明\n\n"
    "也可以直接貼上「下單格式」文字，我會建立訂單並寫入 Google Sheet。"
)

DESSERT_MENU_TEXT = (
    "🍰【甜點菜單】（全品項需前三天預訂）\n\n"
    "1) 達克瓦茲 / 95元/顆（口味不可混、同口味至少2顆）\n"
    "口味：原味、蜜香紅茶、日式抹茶、日式焙茶、法芙娜可可\n\n"
    "2) 原味司康 / 65元/顆\n\n"
    "3) 原味可麗露 / 90元/顆（限冷凍）\n\n"
    "4) 伊思尼奶酥厚片 / 85元/片\n"
    "口味：原味、蜜香紅茶、日式抹茶、日式焙茶、法芙娜可可\n\n"
    f"📌 宅配：大榮冷凍 ${SHIP_FEE} / 滿${FREE_SHIP_THRESHOLD}免運"
)

ORDER_HELP_TEXT = (
    "🧾【下單格式】（直接複製貼上填寫）\n\n"
    "【品項】\n"
    "達克瓦茲 口味：____  數量：__（同口味不可混、同口味至少2顆）\n"
    "司康 原味  數量：__\n"
    "可麗露 原味  數量：__\n"
    "奶酥厚片 口味：____  數量：__\n\n"
    "【取貨方式】店取 / 宅配\n"
    "【取貨日期】YYYY-MM-DD\n"
    "【取貨時間】HH:MM（店取可填，宅配可不填）\n"
    "【電話】09xxxxxxxx\n"
    "【備註】\n\n"
    f"📌 全品項需前三天預訂（至少 {PREORDER_DAYS} 天前）"
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
def reply_text(reply_token: str, text: str):
    with ApiClient(line_config) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(
            ReplyMessageRequest(
                replyToken=reply_token,
                messages=[TextMessage(text=text)],
            )
        )


# =========================
# Parsing / Validation
# =========================
@dataclass
class ParsedOrder:
    pickup_method: str
    pickup_date: str
    pickup_time: str
    phone: str
    note: str
    items: Dict[str, Any]          # structured items
    subtotal: int
    ship_fee: int
    total: int


def _norm(s: str) -> str:
    return s.replace("：", ":").replace("／", "/").strip()


def _extract_pickup_method(text: str) -> Optional[str]:
    t = text
    if "店取" in t:
        return "店取"
    if "宅配" in t:
        return "宅配"
    return None


def _extract_date(text: str) -> Optional[str]:
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text)
    if not m:
        return None
    y, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        d = date(y, mm, dd)
    except ValueError:
        return None
    return d.strftime("%Y-%m-%d")


def _extract_time(text: str) -> Optional[str]:
    m = re.search(r"\b([01]\d|2[0-3]):([0-5]\d)\b", text)
    if not m:
        return None
    return f"{m.group(1)}:{m.group(2)}"


def _extract_phone(text: str) -> Optional[str]:
    # 優先抓台灣手機 09xxxxxxxx
    m = re.search(r"\b(09\d{8})\b", text)
    if m:
        return m.group(1)
    # 其次抓數字/連字號（避免太寬鬆）
    m2 = re.search(r"\b(\d{2,4}-\d{3,4}-\d{3,4})\b", text)
    if m2:
        return m2.group(1)
    return None


def _parse_qty(line: str) -> int:
    m = re.search(r"數量\s*[:：]?\s*(\d+)", line)
    if m:
        return int(m.group(1))
    # 也容許「x2」或「2顆/2個/2片」
    m2 = re.search(r"(?:x|X)\s*(\d+)", line)
    if m2:
        return int(m2.group(1))
    m3 = re.search(r"\b(\d+)\s*(?:顆|個|片)\b", line)
    if m3:
        return int(m3.group(1))
    return 0


def _parse_flavor(line: str) -> Optional[str]:
    # 口味:____
    m = re.search(r"口味\s*[:：]\s*([^\s]+)", line)
    if m:
        return m.group(1).strip()
    # 或者行內直接出現口味字樣
    for f in DACQUOISE_FLAVORS + TOAST_FLAVORS:
        if f in line:
            return f
    return None


def _has_mixed_flavors_in_one_field(flavor_str: str) -> bool:
    # 口味不可混：檢查是否含「、/ /, +」這類混合符號
    return any(sep in flavor_str for sep in ["、", "/", ",", "+", "＋", "與", "and", "And"])


def _validate_preorder(pickup_date_str: str) -> Tuple[bool, str]:
    try:
        y, mm, dd = map(int, pickup_date_str.split("-"))
        pickup = date(y, mm, dd)
    except Exception:
        return False, "取貨日期格式錯誤，請用 YYYY-MM-DD"

    today = datetime.now(TZ_TAIPEI).date()
    delta = (pickup - today).days
    if delta < PREORDER_DAYS:
        return False, f"全品項需前三天預訂（至少 {PREORDER_DAYS} 天前）。你填的取貨日距今天只有 {delta} 天。"
    return True, ""


def parse_order_text_strict(text: str) -> Tuple[Optional[ParsedOrder], List[str]]:
    """
    解析 + 驗證（不過就回 errors）
    """
    errors: List[str] = []
    raw = text.strip()

    pickup_method = _extract_pickup_method(raw)
    if not pickup_method:
        errors.append("缺少【取貨方式】請填：店取 或 宅配")

    pickup_date = _extract_date(raw)
    if not pickup_date:
        errors.append("缺少【取貨日期】請填：YYYY-MM-DD")

    pickup_time = _extract_time(raw) or ""
    phone = _extract_phone(raw)
    if not phone:
        errors.append("缺少【電話】請填：09xxxxxxxx")

    # 解析品項：逐行掃
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    dacq_by_flavor: Dict[str, int] = {}
    scone_qty = 0
    canele_qty = 0
    toast_by_flavor: Dict[str, int] = {}

    for line in lines:
        ln = _norm(line)

        if "達克瓦茲" in ln:
            qty = _parse_qty(ln)
            flavor = _parse_flavor(ln)
            if not flavor:
                errors.append("達克瓦茲需填【口味】")
                continue
            if _has_mixed_flavors_in_one_field(flavor):
                errors.append("達克瓦茲【口味不可混】請一行只填一種口味（例如：達克瓦茲 口味：原味 數量：2）")
                continue
            if flavor not in DACQUOISE_FLAVORS:
                errors.append(f"達克瓦茲口味不在清單內：{flavor}")
                continue
            if qty <= 0:
                errors.append("達克瓦茲需填【數量】且大於 0")
                continue
            dacq_by_flavor[flavor] = dacq_by_flavor.get(flavor, 0) + qty

        elif "司康" in ln:
            qty = _parse_qty(ln)
            if qty <= 0:
                errors.append("司康需填【數量】且大於 0")
                continue
            scone_qty += qty

        elif "可麗露" in ln:
            qty = _parse_qty(ln)
            if qty <= 0:
                errors.append("可麗露需填【數量】且大於 0")
                continue
            canele_qty += qty

        elif ("奶酥厚片" in ln) or ("厚片" in ln and "奶酥" in ln):
            qty = _parse_qty(ln)
            flavor = _parse_flavor(ln)
            if not flavor:
                errors.append("奶酥厚片需填【口味】")
                continue
            if _has_mixed_flavors_in_one_field(flavor):
                errors.append("奶酥厚片口味請一行只填一種口味")
                continue
            if flavor not in TOAST_FLAVORS:
                errors.append(f"奶酥厚片口味不在清單內：{flavor}")
                continue
            if qty <= 0:
                errors.append("奶酥厚片需填【數量】且大於 0")
                continue
            toast_by_flavor[flavor] = toast_by_flavor.get(flavor, 0) + qty

    if not dacq_by_flavor and scone_qty == 0 and canele_qty == 0 and not toast_by_flavor:
        errors.append("沒有解析到任何品項。請照【下單格式】填寫。")

    # 達克瓦茲規則：同口味至少2顆
    for f, q in dacq_by_flavor.items():
        if q < 2:
            errors.append(f"達克瓦茲（{f}）同口味最低購買 2 顆，目前是 {q} 顆")

    # 三天預訂檢查（要有日期才檢）
    if pickup_date:
        ok, msg = _validate_preorder(pickup_date)
        if not ok:
            errors.append(msg)

    if errors:
        return None, errors

    # 計算金額
    dacq_total_qty = sum(dacq_by_flavor.values())
    toast_total_qty = sum(toast_by_flavor.values())

    subtotal = (
        dacq_total_qty * PRICES["dacquoise"]
        + scone_qty * PRICES["scone"]
        + canele_qty * PRICES["canele"]
        + toast_total_qty * PRICES["toast"]
    )

    ship_fee = 0
    if pickup_method == "宅配":
        ship_fee = 0 if subtotal >= FREE_SHIP_THRESHOLD else SHIP_FEE

    total = subtotal + ship_fee

    items = {
        "dacquoise": [{"flavor": f, "qty": q, "unit_price": PRICES["dacquoise"]} for f, q in dacq_by_flavor.items()],
        "scone": {"qty": scone_qty, "unit_price": PRICES["scone"]},
        "canele": {"qty": canele_qty, "unit_price": PRICES["canele"]},
        "toast": [{"flavor": f, "qty": q, "unit_price": PRICES["toast"]} for f, q in toast_by_flavor.items()],
        "shipping": {"method": pickup_method, "fee": ship_fee, "free_threshold": FREE_SHIP_THRESHOLD},
        "subtotal": subtotal,
        "total": total,
    }

    # note：把電話/取貨資訊也保留（方便你對帳）
    note = raw

    parsed = ParsedOrder(
        pickup_method=pickup_method,
        pickup_date=pickup_date,
        pickup_time=pickup_time if pickup_method == "店取" else "",
        phone=phone,
        note=note,
        items=items,
        subtotal=subtotal,
        ship_fee=ship_fee,
        total=total,
    )
    return parsed, []


def build_error_reply(errors: List[str]) -> str:
    lines = ["⚠️ 你的下單資訊有缺/不符合規則，請修正後再貼一次：", ""]
    for e in errors[:10]:
        lines.append(f"・{e}")
    lines.append("")
    lines.append("請用這個格式：")
    lines.append(ORDER_HELP_TEXT)
    return "\n".join(lines)


def build_success_reply(order_id: str, parsed: ParsedOrder) -> str:
    ship_line = ""
    if parsed.pickup_method == "宅配":
        ship_line = f"\n宅配運費：{parsed.ship_fee}（滿{FREE_SHIP_THRESHOLD}免運）"

    return (
        "✅ 已建立訂單並登記成功！\n\n"
        f"訂單編號：{order_id}\n"
        f"取貨方式：{parsed.pickup_method}\n"
        f"取貨日期：{parsed.pickup_date}\n"
        + (f"取貨時間：{parsed.pickup_time}\n" if parsed.pickup_method == "店取" and parsed.pickup_time else "")
        + f"小計：{parsed.subtotal}"
        + ship_line
        + f"\n應付總額：{parsed.total}\n\n"
        "接下來請依「付款說明」完成匯款。\n"
        "匯款後回覆：已轉帳 訂單編號 末五碼12345\n\n"
        "（對帳後會依序出貨/通知取貨）"
    )


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

            # 固定指令（按鈕/文字）
            if user_text in ["menu", "選單", "開始", "hi", "hello", "你好"]:
                reply_text(event.reply_token, MENU_TEXT)
                continue

            if user_text in ["甜點", "菜單"]:
                reply_text(event.reply_token, DESSERT_MENU_TEXT)
                continue

            if user_text in ["我要下單", "下單"]:
                reply_text(event.reply_token, ORDER_HELP_TEXT)
                continue

            if user_text in ["取貨說明", "取貨"]:
                reply_text(event.reply_token, PICKUP_TEXT)
                continue

            if user_text in ["付款說明", "付款", "匯款"]:
                reply_text(event.reply_token, PAY_TEXT)
                continue

            # 其他文字：當作下單內容，做嚴格解析
            parsed, errors = parse_order_text_strict(user_text)
            if errors:
                reply_text(event.reply_token, build_error_reply(errors))
                continue

            # 建立訂單 + 寫入
            order_id = f"UOO-{datetime.now(TZ_TAIPEI).strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"
            created_at = datetime.now(TZ_TAIPEI).strftime("%Y-%m-%d %H:%M:%S")
            user_id = event.source.user_id if event.source else ""
            display_name = ""  # 下一版可加 profile 取得

            items_json = json.dumps(parsed.items, ensure_ascii=False)

            row = [
                created_at,             # created_at
                user_id,                # user_id
                display_name,           # display_name
                order_id,               # order_id
                items_json,             # items_json (structured)
                parsed.pickup_method,   # pickup_method
                parsed.pickup_date,     # pickup_date
                parsed.pickup_time,     # pickup_time
                parsed.note,            # note (含電話/全部原文)
                str(parsed.total),      # amount
                "UNPAID",               # pay_status
                "",                     # linepay_transaction_id
            ]

            try:
                append_order_row(row)
            except Exception as e:
                reply_text(
                    event.reply_token,
                    "⚠️ 我收到你的訊息了，但寫入訂單失敗。\n\n"
                    f"錯誤：{e}\n\n"
                    "請把這段錯誤貼回給我，我會幫你修。"
                )
                continue

            reply_text(event.reply_token, build_success_reply(order_id, parsed))

    return "OK"
