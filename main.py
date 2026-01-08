import os
import json
import base64
import hmac
import hashlib
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Tuple, Set

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse

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

# Sheets
SHEET_ORDERS = os.getenv("SHEET_NAME", "orders").strip()  # A 表
SHEET_ITEMS_READABLE = os.getenv("SHEET_ITEMS_READABLE", "order_items_readable").strip()  # B 表
SHEET_SETTINGS = os.getenv("SHEET_SETTINGS", "settings").strip()  # settings

TZ = timezone(timedelta(hours=8))  # Asia/Taipei
LINE_API_BASE = "https://api.line.me/v2/bot/message"

PICKUP_ADDRESS = os.getenv("PICKUP_ADDRESS", "新竹縣竹北市隘口六街65號").strip()


# =========================
# App / Session
# =========================
app = FastAPI()
SESSIONS: Dict[str, Dict[str, Any]] = {}


def get_session(user_id: str) -> Dict[str, Any]:
    if user_id not in SESSIONS:
        SESSIONS[user_id] = reset_session_dict()
    return SESSIONS[user_id]


def reset_session_dict() -> Dict[str, Any]:
    return {
        "ordering": False,
        "state": "IDLE",

        "cart": [],
        "pending_item": None,
        "pending_flavor": None,

        "pickup_method": None,  # 店取 / 宅配
        "pickup_date": None,
        "pickup_time": None,
        "pickup_name": None,
        "pickup_phone": None,

        "delivery_date": None,
        "delivery_name": None,
        "delivery_phone": None,
        "delivery_address": None,

        # phone confirm
        "phone_temp": None,
        "phone_target": None,  # "PICKUP" or "DELIVERY"

        "edit_mode": None,
    }


def reset_session(sess: Dict[str, Any]):
    sess.clear()
    sess.update(reset_session_dict())


# =========================
# Menu / Data
# =========================
DACQUOISE_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]
TOAST_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

ITEMS = {
    "dacquoise": {"label": "達克瓦茲", "unit_price": 95, "has_flavor": True,  "flavors": DACQUOISE_FLAVORS, "min_qty": 2, "step": 1},
    "scone":     {"label": "原味司康", "unit_price": 65, "has_flavor": False, "flavors": [],               "min_qty": 1, "step": 1},
    "canele6":   {"label": "可麗露 6顆/盒", "unit_price": 490, "has_flavor": False, "flavors": [],          "min_qty": 1, "step": 1},
    "toast":     {"label": "伊思尼奶酥厚片", "unit_price": 85, "has_flavor": True, "flavors": TOAST_FLAVORS,"min_qty": 1, "step": 1},
}

BANK_TRANSFER_TEXT = (
    "付款方式：轉帳（對帳後依訂單號安排出貨/取貨）\n"
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
    "提醒：所有甜點需提前 3 天預訂。"
)


# =========================
# LINE API (no SDK)
# =========================
def line_headers() -> dict:
    return {
        "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def line_reply(reply_token: str, messages: List[dict]):
    if not CHANNEL_ACCESS_TOKEN:
        return
    payload = {"replyToken": reply_token, "messages": messages}
    r = requests.post(
        f"{LINE_API_BASE}/reply",
        headers=line_headers(),
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        timeout=12,
    )
    if r.status_code >= 300:
        print("[ERROR] reply failed:", r.status_code, r.text)


def msg_text(text: str, quick_items: Optional[List[dict]] = None) -> dict:
    m = {"type": "text", "text": text}
    if quick_items:
        m["quickReply"] = {"items": quick_items}
    return m


def quick_postback(label: str, data: str, display_text: Optional[str] = None) -> dict:
    action = {"type": "postback", "label": label, "data": data}
    if display_text:
        action["displayText"] = display_text
    return {"type": "action", "action": action}


def msg_flex(alt_text: str, contents: dict) -> dict:
    # altText/contents 不能空
    return {"type": "flex", "altText": alt_text or "訊息", "contents": contents or {"type": "bubble", "body": {"type": "box", "layout": "vertical", "contents": [{"type": "text", "text": "OK"}]}}}


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


def sheet_exists(service, sheet_name: str) -> bool:
    try:
        meta = service.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheets = meta.get("sheets", [])
        for s in sheets:
            title = (s.get("properties") or {}).get("title")
            if title == sheet_name:
                return True
        return False
    except Exception as e:
        print("[WARN] sheet_exists check failed:", e)
        return False


def append_row(sheet_name: str, row: List[Any]) -> bool:
    if not GSHEET_ID:
        print("[WARN] GSHEET_ID missing, skip append.")
        return False
    service = get_sheets_service()
    if not service:
        print("[WARN] Google Sheet env missing, skip append.")
        return False

    try:
        if not sheet_exists(service, sheet_name):
            print(f"[WARN] sheet '{sheet_name}' not found, skip append.")
            return False

        range_ = f"'{sheet_name}'!A1"
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
        print("[ERROR] append_row failed:", sheet_name, e)
        return False


def read_settings() -> Dict[str, str]:
    """Read settings sheet key/value into dict. If missing, return defaults."""
    defaults = {
        "closed_weekdays": "2",  # Tue
        "closed_dates": "",
        "min_days": "3",
        "max_days": "14",
    }
    if not GSHEET_ID:
        return defaults
    service = get_sheets_service()
    if not service:
        return defaults
    try:
        if not sheet_exists(service, SHEET_SETTINGS):
            return defaults
        rng = f"'{SHEET_SETTINGS}'!A:B"
        resp = service.spreadsheets().values().get(spreadsheetId=GSHEET_ID, range=rng).execute()
        values = resp.get("values", [])
        out = dict(defaults)
        for r in values:
            if len(r) >= 2:
                k = str(r[0]).strip()
                v = str(r[1]).strip()
                if k:
                    out[k] = v
        return out
    except Exception as e:
        print("[WARN] read_settings failed:", e)
        return defaults


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


def fmt_md_date(dt: datetime) -> str:
    wk = "一二三四五六日"[dt.weekday()]
    return f"{dt.month}/{dt.day}（{wk}）"


def parse_closed_weekdays(v: str) -> Set[int]:
    # v: "2" or "2,3"
    out = set()
    for p in (v or "").split(","):
        p = p.strip()
        if not p:
            continue
        try:
            n = int(p)
            if 1 <= n <= 7:
                out.add(n)
        except:
            pass
    return out


def parse_closed_dates(v: str) -> Set[str]:
    # "YYYY-MM-DD,YYYY-MM-DD"
    out = set()
    for p in (v or "").split(","):
        p = p.strip()
        if p:
            out.add(p)
    return out


def is_closed_date(ymd: str, closed_weekdays: Set[int], closed_dates: Set[str]) -> bool:
    if ymd in closed_dates:
        return True
    try:
        dt = datetime.strptime(ymd, "%Y-%m-%d")
        # weekday: Mon=0..Sun=6 => convert to 1..7
        w = dt.weekday() + 1
        return w in closed_weekdays
    except:
        return False


def build_date_buttons() -> List[Tuple[str, str]]:
    st = read_settings()
    min_days = int(st.get("min_days", "3") or "3")
    max_days = int(st.get("max_days", "14") or "14")

    closed_weekdays = parse_closed_weekdays(st.get("closed_weekdays", "2"))
    closed_dates = parse_closed_dates(st.get("closed_dates", ""))

    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)

    out: List[Tuple[str, str]] = []
    # 為了避免公休太多導致按鈕不夠，往後多掃一些天
    scan_limit = max_days + 30

    for i in range(min_days, scan_limit + 1):
        d = today + timedelta(days=i)
        ymd = d.strftime("%Y-%m-%d")
        if i <= max_days and not is_closed_date(ymd, closed_weekdays, closed_dates):
            out.append((fmt_md_date(d), ymd))
        if len(out) >= (max_days - min_days + 1):
            break

    # 如果被公休吃掉太多，至少回傳可選的
    return out


def find_cart_line_label(x: dict) -> str:
    name = x["label"]
    if x.get("flavor"):
        name += f"（{x['flavor']}）"
    qty = x["qty"]
    unit = x["unit_price"]
    sub = x["subtotal"]
    return f"{name} ×{qty}（{unit}/單位）＝{sub}"


def to_readable_items(cart: List[dict]) -> str:
    # 範例：達克瓦茲｜6顆｜原味；可麗露 6顆/盒｜1盒
    parts = []
    for x in cart:
        label = x.get("label", "")
        qty = int(x.get("qty", 0))
        flavor = (x.get("flavor") or "").strip()

        if x.get("item_key") == "canele6":
            s = f"{label}｜{qty}盒"
        else:
            # 預設用「顆」；司康/厚片其實也可用顆，先統一
            if flavor:
                s = f"{label}｜{qty}顆｜{flavor}"
            else:
                s = f"{label}｜{qty}顆"
        parts.append(s)
    return "；".join(parts)


# =========================
# Flex builders
# =========================
def flex_home_hint() -> dict:
    return {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "UooUoo 甜點訂購", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "點「甜點」只看菜單。\n要下單請點「我要下單」。", "wrap": True, "size": "sm", "color": "#666666"},
            ],
        },
    }


def flex_product_menu(ordering: bool) -> dict:
    def btn(label: str, data: str, enabled: bool = True) -> dict:
        return {
            "type": "button",
            "style": "primary" if enabled else "secondary",
            "action": {
                "type": "postback",
                "label": label,
                "data": data,
                "displayText": label,
            },
            "height": "sm",
        }

    disable = not ordering
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
                btn("達克瓦茲｜NT$95", "PB:ITEM:dacquoise", enabled=not disable),
                btn("原味司康｜NT$65", "PB:ITEM:scone", enabled=not disable),
                btn("可麗露 6顆/盒｜NT$490", "PB:ITEM:canele6", enabled=not disable),
                btn("伊思尼奶酥厚片｜NT$85", "PB:ITEM:toast", enabled=not disable),
                {"type": "separator", "margin": "lg"},
                {
                    "type": "button",
                    "style": "secondary",
                    "action": {"type": "postback", "label": "🧾 前往結帳", "data": "PB:CHECKOUT", "displayText": "前往結帳"},
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "action": {"type": "postback", "label": "🗑 清空重來", "data": "PB:RESET", "displayText": "清空重來"},
                },
            ],
        },
    }


def flex_pickup_method() -> dict:
    return {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "請選擇店取或宅配", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "（日期可直接按按鈕，不用手打）", "size": "sm", "color": "#666666"},
                {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🏪 店取", "data": "PB:PICKUP:店取", "displayText": "店取"}},
                {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🚚 冷凍宅配", "data": "PB:PICKUP:宅配", "displayText": "冷凍宅配"}},
            ],
        },
    }


def flex_checkout_summary(sess: dict) -> dict:
    cart = sess["cart"]
    lines = [find_cart_line_label(x) for x in cart]
    total = cart_total(cart)
    fee = shipping_fee(total) if sess.get("pickup_method") == "宅配" else 0
    grand = total + fee

    method = sess.get("pickup_method") or "（未選）"
    date = sess.get("pickup_date") if method == "店取" else sess.get("delivery_date")
    date = date or "（未選）"
    time = sess.get("pickup_time") or ("—" if method != "店取" else "（未選）")

    shown = lines[:10]
    if len(lines) > 10:
        shown.append(f"…等 {len(lines)} 項（請先刪減購物車）")

    list_text = "\n".join([f"• {s}" for s in shown]) if shown else "（購物車是空的）"

    bottom_text = f"小計：NT${total}"
    if method == "宅配":
        bottom_text += f"\n運費：NT${fee}\n應付：NT${grand}"

    return {
        "type": "bubble",
        "size": "mega",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "🧾 結帳內容", "weight": "bold", "size": "xl"},
                {"type": "text", "text": list_text, "wrap": True, "size": "sm"},
                {"type": "separator", "margin": "md"},
                {"type": "text", "text": f"取貨方式：{method}", "size": "sm", "color": "#666666"},
                {"type": "text", "text": f"日期：{date}", "size": "sm", "color": "#666666"},
                {"type": "text", "text": f"時段：{time}", "size": "sm", "color": "#666666"},
                {"type": "separator", "margin": "md"},
                {"type": "text", "text": bottom_text, "weight": "bold", "size": "lg"},
            ],
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🛠 修改品項", "data": "PB:EDIT:MENU", "displayText": "修改品項"}},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "➕ 繼續加購", "data": "PB:CONTINUE", "displayText": "繼續加購"}},
                {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "✅ 下一步", "data": "PB:NEXT", "displayText": "下一步"}},
            ],
        },
    }


# =========================
# Cart operations
# =========================
def recalc_cart(sess: dict):
    for x in sess["cart"]:
        x["subtotal"] = int(x["unit_price"]) * int(x["qty"])


def add_to_cart(user_id: str, item_key: str, flavor: Optional[str], qty: int):
    sess = get_session(user_id)
    meta = ITEMS[item_key]

    if meta["has_flavor"] and not flavor:
        raise ValueError("缺少口味")
    if qty < meta["min_qty"]:
        raise ValueError(f"數量至少 {meta['min_qty']}")

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


def can_dec_item(item_key: str, new_qty: int) -> bool:
    return new_qty >= ITEMS[item_key]["min_qty"]


def build_cart_item_choices(sess: dict, mode: str) -> List[dict]:
    items = []
    for idx, x in enumerate(sess["cart"]):
        label = x["label"]
        if x.get("flavor"):
            label += f"（{x['flavor']}）"
        label += f" ×{x['qty']}"
        items.append(quick_postback(label, f"PB:EDIT:{mode}:{idx}", display_text=label))
    return items


def build_qty_quick(min_qty: int, max_qty: int, prefix: str) -> List[dict]:
    items = []
    for i in range(min_qty, max_qty + 1):
        items.append(quick_postback(str(i), f"{prefix}{i}", display_text=str(i)))
    return items


# =========================
# Order write (A + B)
# =========================
def create_order_and_write_sheets(user_id: str) -> str:
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
    phone = ""

    if pickup_method == "宅配":
        delivery_date = sess.get("delivery_date") or ""
        dn = sess.get("delivery_name") or ""
        dp = sess.get("delivery_phone") or ""
        da = sess.get("delivery_address") or ""
        phone = dp
        note = f"希望到貨:{delivery_date} | 收件人:{dn} | 電話:{dp} | 地址:{da}"
        pickup_date = delivery_date
        pickup_time = ""

    if pickup_method == "店取":
        pn = sess.get("pickup_name") or ""
        pp = sess.get("pickup_phone") or ""
        phone = pp
        note = f"取件人:{pn} | 電話:{pp}"

    # A 表：orders（維持你原本欄位習慣）
    row_a = [
        now_str(),
        user_id,
        "",  # display_name（之後要抓 profile 可補）
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
    append_row(SHEET_ORDERS, row_a)

    # B 表：order_items_readable（白話文）
    readable = to_readable_items(cart)
    row_b = [
        now_str(),
        order_id,
        pickup_method,
        pickup_date,
        pickup_time,
        readable,
        total,
        "UNPAID",
        phone,
    ]
    append_row(SHEET_ITEMS_READABLE, row_b)

    return order_id


# =========================
# Signature verify
# =========================
def verify_line_signature(body: bytes, signature: str) -> bool:
    if not CHANNEL_SECRET:
        return False
    mac = hmac.new(CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature)


# =========================
# Routes
# =========================
@app.get("/")
def root():
    return {"ok": True, "service": "uoo-line-bot"}


@app.post("/callback")
async def callback(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")

    if not verify_line_signature(body, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(body.decode("utf-8"))
    events = payload.get("events", [])

    for ev in events:
        try:
            handle_event(ev)
        except Exception as e:
            print("[ERROR] handle_event:", e)

    return PlainTextResponse("OK")


# =========================
# Event handler
# =========================
def handle_event(ev: dict):
    etype = ev.get("type")
    user_id = (ev.get("source") or {}).get("userId", "")
    reply_token = ev.get("replyToken", "")

    if not user_id:
        return

    sess = get_session(user_id)

    # ---- message text ----
    if etype == "message" and (ev.get("message") or {}).get("type") == "text":
        text = (ev["message"].get("text") or "").strip()

        # Rich menu: 甜點 / 我要下單 / 取貨說明 / 付款說明
        if text in ["清空重來", "清空", "reset"]:
            reset_session(sess)
            line_reply(reply_token, [msg_text("已清空，重新開始。\n點「甜點」可看菜單；要下單請點「我要下單」。")])
            return

        if text == "甜點":
            # 只看菜單，不進下單
            line_reply(reply_token, [msg_flex("甜點菜單", flex_product_menu(ordering=False))])
            return

        if text == "我要下單":
            sess["ordering"] = True
            sess["state"] = "IDLE"
            line_reply(reply_token, [
                msg_text("好的，開始下單。\n請從菜單選擇商品加入購物車。"),
                msg_flex("甜點菜單", flex_product_menu(ordering=True)),
            ])
            return

        if text == "取貨說明":
            line_reply(reply_token, [msg_text(PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE)])
            return

        if text == "付款資訊" or text == "付款說明":
            line_reply(reply_token, [msg_text(BANK_TRANSFER_TEXT)])
            return

        if text.startswith("已轉帳"):
            line_reply(reply_token, [msg_text("收到，我們會核對帳款後依訂單號安排出貨/取貨。\n若需補充資訊也可以直接留言。")])
            return

        # 其他文字：依 state 接續流程
        handle_state_text(user_id, reply_token, text)
        return

    # ---- postback ----
    if etype == "postback":
        data = (ev.get("postback") or {}).get("data", "")
        handle_postback(user_id, reply_token, data)
        return


# =========================
# Postback flows
# =========================
def handle_postback(user_id: str, reply_token: str, data: str):
    sess = get_session(user_id)

    # RESET
    if data == "PB:RESET":
        reset_session(sess)
        line_reply(reply_token, [msg_text("已清空。\n點「甜點」可看菜單；要下單請點「我要下單」。")])
        return

    # CONTINUE
    if data == "PB:CONTINUE":
        line_reply(reply_token, [msg_flex("甜點菜單", flex_product_menu(ordering=sess["ordering"]))])
        return

    # CHECKOUT entry
    if data == "PB:CHECKOUT":
        if not sess["ordering"]:
            line_reply(reply_token, [msg_text("目前是看菜單模式。\n要下單請先點「我要下單」。")])
            return
        if not sess["cart"]:
            line_reply(reply_token, [msg_text("購物車是空的，請先選商品。"), msg_flex("甜點菜單", flex_product_menu(ordering=True))])
            return

        sess["state"] = "WAIT_PICKUP_METHOD"
        line_reply(reply_token, [msg_flex("取貨方式", flex_pickup_method())])
        return

    # ITEM
    if data.startswith("PB:ITEM:"):
        if not sess["ordering"]:
            line_reply(reply_token, [msg_text("目前是看菜單模式。\n要下單請先點「我要下單」。")])
            return
        item_key = data.split("PB:ITEM:", 1)[1].strip()
        if item_key not in ITEMS:
            line_reply(reply_token, [msg_text("品項不存在，請重新選擇。")])
            return

        sess["pending_item"] = item_key
        sess["pending_flavor"] = None

        meta = ITEMS[item_key]
        if meta["has_flavor"]:
            sess["state"] = "WAIT_FLAVOR"
            q = [quick_postback(f, f"PB:FLAVOR:{f}", display_text=f) for f in meta["flavors"]]
            line_reply(reply_token, [msg_text(f"你選了：{meta['label']}\n請選口味：", quick_items=q)])
            return
        else:
            sess["state"] = "WAIT_QTY"
            q = build_qty_quick(meta["min_qty"], 12, prefix="PB:QTY:")
            line_reply(reply_token, [msg_text(f"你選了：{meta['label']}\n請選數量：", quick_items=q)])
            return

    # FLAVOR
    if data.startswith("PB:FLAVOR:"):
        flavor = data.split("PB:FLAVOR:", 1)[1].strip()
        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            line_reply(reply_token, [msg_text("流程有點亂掉了，請點「我要下單」重新開始。")])
            return
        if flavor not in ITEMS[item_key]["flavors"]:
            line_reply(reply_token, [msg_text("口味不正確，請重新選。")])
            return

        sess["pending_flavor"] = flavor
        sess["state"] = "WAIT_QTY"
        q = build_qty_quick(ITEMS[item_key]["min_qty"], 12, prefix="PB:QTY:")
        line_reply(reply_token, [msg_text(f"口味：{flavor}\n請選數量：", quick_items=q)])
        return

    # QTY
    if data.startswith("PB:QTY:"):
        qty = int(data.split("PB:QTY:", 1)[1].strip())
        item_key = sess.get("pending_item")
        if not item_key or item_key not in ITEMS:
            line_reply(reply_token, [msg_text("流程有點亂掉了，請點「我要下單」重新開始。")])
            return

        flavor = sess.get("pending_flavor")
        try:
            add_to_cart(user_id, item_key, flavor, qty)
        except Exception as e:
            line_reply(reply_token, [msg_text(f"加入失敗：{e}")])
            return

        sess["pending_item"] = None
        sess["pending_flavor"] = None
        sess["state"] = "IDLE"
        recalc_cart(sess)

        line_reply(reply_token, [
            msg_text("✅ 已加入購物車"),
            msg_flex("結帳內容", flex_checkout_summary(sess)),
        ])
        return

    # PICKUP METHOD
    if data.startswith("PB:PICKUP:"):
        method = data.split("PB:PICKUP:", 1)[1].strip()
        sess["pickup_method"] = method

        date_buttons = build_date_buttons()
        if not date_buttons:
            line_reply(reply_token, [msg_text("目前可預訂日期不足（可能公休設定太多）。\n請稍後再試或聯繫我們。")])
            return

        quick_items = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]

        if method == "店取":
            sess["state"] = "WAIT_PICKUP_DATE"
            line_reply(reply_token, [msg_text("請選「店取日期」（依公休自動排除）：", quick_items=quick_items)])
            return

        if method == "宅配":
            sess["state"] = "WAIT_DELIVERY_DATE"
            line_reply(reply_token, [msg_text("請選「希望到貨日期」（僅作希望日，不保證準時到貨；公休日不出貨）：", quick_items=quick_items)])
            return

    # DATE
    if data.startswith("PB:DATE:"):
        ymd = data.split("PB:DATE:", 1)[1].strip()

        # double-check closed
        st = read_settings()
        cw = parse_closed_weekdays(st.get("closed_weekdays", "2"))
        cd = parse_closed_dates(st.get("closed_dates", ""))
        if is_closed_date(ymd, cw, cd):
            # 直接阻擋
            date_buttons = build_date_buttons()
            quick_items = [quick_postback(lbl, f"PB:DATE:{d}", display_text=lbl) for (lbl, d) in date_buttons]
            line_reply(reply_token, [msg_text(f"⚠️ {ymd} 為公休/不出貨日，請改選其他日期：", quick_items=quick_items)])
            return

        if sess["state"] == "WAIT_PICKUP_DATE":
            sess["pickup_date"] = ymd
            sess["state"] = "WAIT_PICKUP_TIME"
            q = [
                quick_postback("10:00-12:00", "PB:TIME:10:00-12:00", display_text="10:00-12:00"),
                quick_postback("12:00-14:00", "PB:TIME:12:00-14:00", display_text="12:00-14:00"),
                quick_postback("14:00-16:00", "PB:TIME:14:00-16:00", display_text="14:00-16:00"),
            ]
            line_reply(reply_token, [msg_text(f"✅ 已選店取日期：{ymd}\n請選店取時段：", quick_items=q)])
            return

        if sess["state"] == "WAIT_DELIVERY_DATE":
            sess["delivery_date"] = ymd
            sess["state"] = "WAIT_DELIVERY_NAME"
            line_reply(reply_token, [msg_text(f"✅ 已選希望到貨日期：{ymd}\n請輸入宅配收件人姓名：")])
            return

        line_reply(reply_token, [msg_text("日期已收到，但目前流程不在選日期階段。請點「前往結帳」重新操作。")])
        return

    # TIME
    if data.startswith("PB:TIME:") and sess["state"] == "WAIT_PICKUP_TIME":
        t = data.split("PB:TIME:", 1)[1].strip()
        sess["pickup_time"] = t
        sess["state"] = "WAIT_PICKUP_NAME"
        line_reply(reply_token, [
            msg_text(f"✅ 店取資訊已選好：\n日期：{sess.get('pickup_date')}\n時段：{t}\n地址：{PICKUP_ADDRESS}\n\n請輸入取件人姓名：")
        ])
        return

    # PHONE CONFIRM buttons
    if data == "PB:PHONE:OK":
        # confirm phone
        if sess.get("phone_target") == "PICKUP":
            sess["pickup_phone"] = sess.get("phone_temp")
            sess["phone_temp"] = None
            sess["phone_target"] = None
            sess["state"] = "IDLE"
            line_reply(reply_token, [msg_text("✅ 已確認店取電話"), msg_flex("結帳內容", flex_checkout_summary(sess))])
            return

        if sess.get("phone_target") == "DELIVERY":
            sess["delivery_phone"] = sess.get("phone_temp")
            sess["phone_temp"] = None
            sess["phone_target"] = None
            sess["state"] = "WAIT_DELIVERY_ADDRESS"
            line_reply(reply_token, [msg_text("✅ 已確認宅配電話\n請輸入宅配地址（完整地址）：")])
            return

        line_reply(reply_token, [msg_text("目前沒有需要確認的電話。")])
        return

    if data == "PB:PHONE:RETRY":
        target = sess.get("phone_target")
        sess["phone_temp"] = None
        if target == "PICKUP":
            sess["state"] = "WAIT_PICKUP_PHONE"
            line_reply(reply_token, [msg_text("好的，請重新輸入店取電話：")])
            return
        if target == "DELIVERY":
            sess["state"] = "WAIT_DELIVERY_PHONE"
            line_reply(reply_token, [msg_text("好的，請重新輸入宅配電話：")])
            return
        line_reply(reply_token, [msg_text("目前沒有需要重新輸入的電話。")])
        return

    # EDIT MENU
    if data == "PB:EDIT:MENU":
        if not sess["cart"]:
            line_reply(reply_token, [msg_text("購物車是空的，無法修改。")])
            return
        sess["state"] = "EDIT_MENU"
        q = [
            quick_postback("➕ 增加數量", "PB:EDITMODE:INC", display_text="增加數量"),
            quick_postback("➖ 減少數量", "PB:EDITMODE:DEC", display_text="減少數量"),
            quick_postback("🗑 移除品項", "PB:EDITMODE:DEL", display_text="移除品項"),
        ]
        line_reply(reply_token, [msg_text("請選要修改的方式：", quick_items=q)])
        return

    # EDITMODE
    if data.startswith("PB:EDITMODE:"):
        mode = data.split("PB:EDITMODE:", 1)[1].strip()
        sess["edit_mode"] = mode
        sess["state"] = "EDIT_PICK_ITEM"
        q = build_cart_item_choices(sess, mode)
        line_reply(reply_token, [msg_text("請選要修改的品項：", quick_items=q)])
        return

    # EDIT apply
    if data.startswith("PB:EDIT:") and data.count(":") == 3:
        parts = data.split(":")  # PB EDIT MODE IDX
        mode = parts[2].strip()
        idx = int(parts[3].strip())

        if idx < 0 or idx >= len(sess["cart"]):
            line_reply(reply_token, [msg_text("找不到該品項，請重新操作。")])
            return

        x = sess["cart"][idx]
        item_key = x["item_key"]

        if mode == "INC":
            x["qty"] += ITEMS[item_key]["step"]
        elif mode == "DEC":
            new_qty = x["qty"] - ITEMS[item_key]["step"]
            if not can_dec_item(item_key, new_qty):
                line_reply(reply_token, [msg_text(f"此品項最低數量為 {ITEMS[item_key]['min_qty']}，不能再減了。")])
                return
            x["qty"] = new_qty
        elif mode == "DEL":
            sess["cart"].pop(idx)
        else:
            line_reply(reply_token, [msg_text("未知的修改模式。")])
            return

        recalc_cart(sess)
        sess["state"] = "IDLE"
        sess["edit_mode"] = None

        if not sess["cart"]:
            line_reply(reply_token, [msg_text("✅ 已更新。購物車目前是空的。"), msg_flex("甜點菜單", flex_product_menu(ordering=True))])
            return

        line_reply(reply_token, [msg_text("✅ 已更新結帳內容"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

    # NEXT
    if data == "PB:NEXT":
        if not sess["cart"]:
            line_reply(reply_token, [msg_text("購物車是空的，請先選商品。")])
            return

        if not sess.get("pickup_method"):
            sess["state"] = "WAIT_PICKUP_METHOD"
            line_reply(reply_token, [msg_flex("取貨方式", flex_pickup_method())])
            return

        if sess["pickup_method"] == "店取":
            if not sess.get("pickup_date"):
                sess["state"] = "WAIT_PICKUP_DATE"
                date_buttons = build_date_buttons()
                qi = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]
                line_reply(reply_token, [msg_text("請選店取日期：", quick_items=qi)])
                return
            if not sess.get("pickup_time"):
                sess["state"] = "WAIT_PICKUP_TIME"
                q = [
                    quick_postback("10:00-12:00", "PB:TIME:10:00-12:00", display_text="10:00-12:00"),
                    quick_postback("12:00-14:00", "PB:TIME:12:00-14:00", display_text="12:00-14:00"),
                    quick_postback("14:00-16:00", "PB:TIME:14:00-16:00", display_text="14:00-16:00"),
                ]
                line_reply(reply_token, [msg_text("請選店取時段：", quick_items=q)])
                return
            if not sess.get("pickup_name"):
                sess["state"] = "WAIT_PICKUP_NAME"
                line_reply(reply_token, [msg_text("請輸入取件人姓名：")])
                return
            if not sess.get("pickup_phone"):
                sess["state"] = "WAIT_PICKUP_PHONE"
                line_reply(reply_token, [msg_text("請輸入店取聯絡電話（必填）：")])
                return

        if sess["pickup_method"] == "宅配":
            if not sess.get("delivery_date"):
                sess["state"] = "WAIT_DELIVERY_DATE"
                date_buttons = build_date_buttons()
                qi = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]
                line_reply(reply_token, [msg_text("請選希望到貨日期：", quick_items=qi)])
                return
            if not sess.get("delivery_name"):
                sess["state"] = "WAIT_DELIVERY_NAME"
                line_reply(reply_token, [msg_text("請輸入宅配收件人姓名：")])
                return
            if not sess.get("delivery_phone"):
                sess["state"] = "WAIT_DELIVERY_PHONE"
                line_reply(reply_token, [msg_text("請輸入宅配電話（必填）：")])
                return
            if not sess.get("delivery_address"):
                sess["state"] = "WAIT_DELIVERY_ADDRESS"
                line_reply(reply_token, [msg_text("請輸入宅配地址（完整地址）：")])
                return

        # all ok -> create order
        order_id = create_order_and_write_sheets(user_id)

        total = cart_total(sess["cart"])
        fee = shipping_fee(total) if sess["pickup_method"] == "宅配" else 0
        grand = total + fee
        summary_lines = "\n".join([f"• {find_cart_line_label(x)}" for x in sess["cart"]])

        # 穩定版確認文案（含電話填錯補救）
        if sess["pickup_method"] == "店取":
            msg = (
                "✅ 訂單建立成功（待轉帳確認）\n"
                f"訂單編號：{order_id}\n\n"
                "【品項】\n"
                f"{summary_lines}\n\n"
                "【店取資訊】\n"
                f"日期：{sess['pickup_date']}\n"
                f"時段：{sess['pickup_time']}\n"
                f"取件人：{sess['pickup_name']}\n"
                f"電話：{sess['pickup_phone']}\n"
                f"地址：{PICKUP_ADDRESS}\n\n"
                f"小計：NT${total}\n\n"
                f"{BANK_TRANSFER_TEXT}\n\n"
                "若電話填錯：請直接回覆\n"
                f"「修改電話 {order_id} 正確電話」我們會以此更新聯絡資訊。"
            )
        else:
            msg = (
                "✅ 訂單建立成功（待轉帳確認）\n"
                f"訂單編號：{order_id}\n\n"
                "【品項】\n"
                f"{summary_lines}\n\n"
                "【宅配資訊】\n"
                f"希望到貨：{sess['delivery_date']}（僅作希望日，不保證準時）\n"
                f"收件人：{sess['delivery_name']}\n"
                f"電話：{sess['delivery_phone']}\n"
                f"地址：{sess['delivery_address']}\n\n"
                f"小計：NT${total}\n"
                f"運費：NT${fee}\n"
                f"應付：NT${grand}\n\n"
                f"{DELIVERY_NOTICE}\n\n"
                f"{BANK_TRANSFER_TEXT}\n\n"
                "若電話填錯：請直接回覆\n"
                f"「修改電話 {order_id} 正確電話」我們會以此更新聯絡資訊。"
            )

        # clear session
        reset_session(sess)
        line_reply(reply_token, [msg_text(msg)])
        return

    line_reply(reply_token, [msg_text("已收到操作，但流程未對上。若要下單請點「我要下單」。")])


# =========================
# State text handlers
# =========================
def handle_state_text(user_id: str, reply_token: str, text: str):
    sess = get_session(user_id)

    # 若還沒開始下單
    if not sess["ordering"]:
        line_reply(reply_token, [msg_flex("提示", flex_home_hint())])
        return

    # 店取姓名
    if sess["state"] == "WAIT_PICKUP_NAME":
        sess["pickup_name"] = text.strip()
        sess["state"] = "WAIT_PICKUP_PHONE"
        line_reply(reply_token, [msg_text("✅ 已收到取件人姓名\n請輸入店取聯絡電話（必填）：")])
        return

    # 店取電話 -> 二次確認
    if sess["state"] == "WAIT_PICKUP_PHONE":
        p = text.strip()
        sess["phone_temp"] = p
        sess["phone_target"] = "PICKUP"
        sess["state"] = "WAIT_PICKUP_PHONE_CONFIRM"
        q = [
            quick_postback("✅ 電話正確", "PB:PHONE:OK", display_text="電話正確"),
            quick_postback("✏️ 重新輸入", "PB:PHONE:RETRY", display_text="重新輸入"),
        ]
        line_reply(reply_token, [msg_text(f"請確認店取電話：{p}", quick_items=q)])
        return

    # 宅配姓名
    if sess["state"] == "WAIT_DELIVERY_NAME":
        sess["delivery_name"] = text.strip()
        sess["state"] = "WAIT_DELIVERY_PHONE"
        line_reply(reply_token, [msg_text("請輸入宅配電話（必填）：")])
        return

    # 宅配電話 -> 二次確認
    if sess["state"] == "WAIT_DELIVERY_PHONE":
        p = text.strip()
        sess["phone_temp"] = p
        sess["phone_target"] = "DELIVERY"
        sess["state"] = "WAIT_DELIVERY_PHONE_CONFIRM"
        q = [
            quick_postback("✅ 電話正確", "PB:PHONE:OK", display_text="電話正確"),
            quick_postback("✏️ 重新輸入", "PB:PHONE:RETRY", display_text="重新輸入"),
        ]
        line_reply(reply_token, [msg_text(f"請確認宅配電話：{p}", quick_items=q)])
        return

    # 宅配地址
    if sess["state"] == "WAIT_DELIVERY_ADDRESS":
        sess["delivery_address"] = text.strip()
        sess["state"] = "IDLE"
        line_reply(reply_token, [msg_text("✅ 已收到宅配資訊"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

    # 其他狀態
    line_reply(reply_token, [msg_text("我有收到你的訊息，但目前建議用按鈕操作。\n要下單請點「我要下單」，要看菜單請點「甜點」。")])
