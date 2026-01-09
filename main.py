import os
import json
import base64
import hmac
import hashlib
import random
import string
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Any, Optional, List, Tuple

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

# A/B/C 表（你說 A=orders）
SHEET_A_NAME = os.getenv("SHEET_NAME", "orders").strip()  # A表（orders）
SHEET_B_NAME = os.getenv("SHEET_B_NAME", "order_items_readable").strip()  # B表
SHEET_C_NAME = os.getenv("SHEET_C_NAME", "cashflow").strip()  # C表
SHEET_SETTINGS_NAME = os.getenv("SHEET_SETTINGS_NAME", "settings").strip()  # settings（可無）

# 管理員 ID（逗號分隔）
ADMIN_USER_IDS = [x.strip() for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()]

TZ = timezone(timedelta(hours=8))  # Asia/Taipei

LINE_API_BASE = "https://api.line.me/v2/bot/message"

PICKUP_ADDRESS = os.getenv("PICKUP_ADDRESS", "新竹縣竹北市隘口六街65號").strip()

BANK_TRANSFER_TEXT = os.getenv(
    "BANK_TRANSFER_TEXT",
    "付款方式：轉帳（對帳後依訂單號安排出貨/取貨）\n"
    "台灣銀行 004\n"
    "帳號：248-001-03430-6\n\n"
    "轉帳後請回傳：\n"
    "「已轉帳 訂單編號 末五碼12345」"
).strip()

DELIVERY_NOTICE = os.getenv(
    "DELIVERY_NOTICE",
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
).strip()

PICKUP_NOTICE = os.getenv(
    "PICKUP_NOTICE",
    f"店取地址：\n{PICKUP_ADDRESS}\n\n提醒：所有甜點需提前3天預訂。"
).strip()

# 日期規則
MIN_DAYS = int(os.getenv("MIN_DAYS", "3").strip() or "3")
MAX_DAYS = int(os.getenv("MAX_DAYS", "14").strip() or "14")

# 公休日（ENV 可先用，settings sheet 可覆蓋）
# CLOSED_WEEKDAYS: 週二=2（你習慣）可多個，例如 "2,3"
ENV_CLOSED_WEEKDAYS = os.getenv("CLOSED_WEEKDAYS", "2").strip()
# CLOSED_DATES: 例如 "2026-01-13,2026-01-14"
ENV_CLOSED_DATES = os.getenv("CLOSED_DATES", "").strip()

# 店取時段
PICKUP_SLOTS = ["10:00-12:00", "12:00-14:00", "14:00-16:00"]


# =========================
# App
# =========================
app = FastAPI()

# =========================
# In-memory session store
# =========================
SESSIONS: Dict[str, Dict[str, Any]] = {}


def get_session(user_id: str) -> Dict[str, Any]:
    if user_id not in SESSIONS:
        SESSIONS[user_id] = {
            "ordering": False,
            "state": "IDLE",

            "cart": [],
            "pending_item": None,
            "pending_flavor": None,

            "pickup_method": None,
            "pickup_date": None,
            "pickup_time": None,
            "pickup_name": None,
            "pickup_phone": None,
            "pickup_phone_ok": False,

            "delivery_date": None,
            "delivery_name": None,
            "delivery_phone": None,
            "delivery_phone_ok": False,
            "delivery_address": None,

            "edit_mode": None,
        }
    return SESSIONS[user_id]


# =========================
# Menu / Items
# =========================
DACQUOISE_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]
TOAST_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

ITEMS = {
    "dacquoise": {"label": "達克瓦茲", "unit_price": 95, "has_flavor": True,  "flavors": DACQUOISE_FLAVORS, "min_qty": 1, "step": 1},
    "scone":     {"label": "原味司康", "unit_price": 65, "has_flavor": False, "flavors": [],               "min_qty": 1, "step": 1},
    "canele6":   {"label": "可麗露 6顆/盒", "unit_price": 490, "has_flavor": False, "flavors": [],        "min_qty": 1, "step": 1},
    "toast":     {"label": "伊思尼奶酥厚片", "unit_price": 85, "has_flavor": True, "flavors": TOAST_FLAVORS,"min_qty": 1, "step": 1},
}


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
        timeout=15,
    )
    if r.status_code >= 300:
        print("[ERROR] reply failed:", r.status_code, r.text)


def line_push(user_id: str, messages: List[dict]):
    if not CHANNEL_ACCESS_TOKEN:
        return
    payload = {"to": user_id, "messages": messages}
    r = requests.post(
        f"{LINE_API_BASE}/push",
        headers=line_headers(),
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        timeout=15,
    )
    if r.status_code >= 300:
        print("[ERROR] push failed:", r.status_code, r.text)


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
    # LINE 會檢查 altText/contents 不可空
    if not alt_text:
        alt_text = "訊息"
    if not contents:
        contents = {"type": "bubble", "body": {"type": "box", "layout": "vertical", "contents": [{"type": "text", "text": "…"}]}}
    return {"type": "flex", "altText": alt_text, "contents": contents}


# =========================
# Google Sheets (googleapiclient)
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


def sheet_list_titles() -> List[str]:
    """用來檢查 A/B/C/settings 分頁是否真的存在（避免 C 表不動其實是 tab 名稱不對）。"""
    service = get_sheets_service()
    if not service or not GSHEET_ID:
        return []
    try:
        meta = service.spreadsheets().get(spreadsheetId=GSHEET_ID).execute()
        sheets = meta.get("sheets", []) or []
        return [((s.get("properties") or {}).get("title") or "").strip() for s in sheets]
    except Exception as e:
        print("[WARN] sheet_list_titles failed:", e)
        return []


def sheet_append(sheet_name: str, row: List[Any]) -> bool:
    if not GSHEET_ID:
        print("[WARN] GSHEET_ID missing, skip append.")
        return False
    service = get_sheets_service()
    if not service:
        print("[WARN] Google Sheet env missing, skip append.")
        return False
    try:
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
        print(f"[ERROR] append to sheet='{sheet_name}' failed:", e)
        return False


def sheet_read_range(sheet_name: str, a1: str) -> List[List[str]]:
    service = get_sheets_service()
    if not service or not GSHEET_ID:
        return []
    try:
        r = service.spreadsheets().values().get(
            spreadsheetId=GSHEET_ID,
            range=f"'{sheet_name}'!{a1}"
        ).execute()
        return r.get("values", []) or []
    except Exception as e:
        print(f"[WARN] read range failed sheet='{sheet_name}' range='{a1}':", e)
        return []


@app.on_event("startup")
def startup_check():
    # 開機就把分頁存在性印出來（你一看 log 就知道 C 表不動是不是名稱不對）
    titles = sheet_list_titles()
    if not titles:
        print("[INFO] Startup: cannot list sheet titles (maybe missing GSHEET_ID or service account).")
        return

    need = [SHEET_A_NAME, SHEET_B_NAME, SHEET_C_NAME, SHEET_SETTINGS_NAME]
    missing = [n for n in need if n and n not in titles]
    print("[INFO] Google Sheet tabs:", titles)
    if missing:
        print("[ERROR] Missing sheet tabs:", missing)
        print("[ERROR] -> 請去 Google Sheet 把分頁名稱改成一致，或用 env 改 SHEET_*_NAME")
    else:
        print("[INFO] All required sheet tabs exist.")


# =========================
# Settings: 公休 / 不出貨日
# =========================
def parse_int_list(s: str) -> List[int]:
    out = []
    for x in (s or "").split(","):
        x = x.strip()
        if not x:
            continue
        try:
            out.append(int(x))
        except:
            pass
    return out


def parse_date_set(s: str) -> set:
    out = set()
    for x in (s or "").split(","):
        x = x.strip()
        if not x:
            continue
        out.add(x)
    return out


def load_settings() -> Dict[str, Any]:
    """
    修正點（對應你上一個對話的公休日可選問題）：
    - settings 分頁「可能沒有 header」，舊版會 rows[1:] 直接跳過第一列，導致 closed_weekdays 沒讀到
    - 這版會自動判斷第一列是不是 key/value header，不是就全讀
    """
    settings = {
        "closed_weekdays": parse_int_list(ENV_CLOSED_WEEKDAYS),  # 你慣用 週二=2
        "closed_dates": parse_date_set(ENV_CLOSED_DATES),
        "min_days": MIN_DAYS,
        "max_days": MAX_DAYS,
    }

    try:
        rows = sheet_read_range(SHEET_SETTINGS_NAME, "A1:B200")
        if not rows:
            return settings

        first_k = (rows[0][0] or "").strip().lower() if len(rows[0]) >= 1 else ""
        first_v = (rows[0][1] or "").strip().lower() if len(rows[0]) >= 2 else ""
        has_header = (first_k in ("key", "k") and first_v in ("value", "v"))

        data_rows = rows[1:] if has_header else rows

        for r in data_rows:
            if len(r) < 2:
                continue
            k = (r[0] or "").strip()
            v = (r[1] or "").strip()
            if not k:
                continue

            if k == "closed_weekdays":
                settings["closed_weekdays"] = parse_int_list(v)
            elif k == "closed_dates":
                settings["closed_dates"] = parse_date_set(v)
            elif k == "min_days":
                try:
                    settings["min_days"] = int(v)
                except:
                    pass
            elif k == "max_days":
                try:
                    settings["max_days"] = int(v)
                except:
                    pass

    except Exception as e:
        print("[INFO] settings sheet not loaded, use ENV:", e)

    return settings


def weekday_user_to_py(wd: int) -> int:
    # 你習慣：週二=2 -> python weekday (Mon=0)
    if 1 <= wd <= 7:
        return wd - 1
    return wd


def is_closed(d: date, settings: Dict[str, Any]) -> bool:
    ymd = d.strftime("%Y-%m-%d")
    if ymd in settings["closed_dates"]:
        return True
    for wd in settings["closed_weekdays"]:
        if d.weekday() == weekday_user_to_py(wd):
            return True
    return False


def fmt_md_date(dt: datetime) -> str:
    wk = "一二三四五六日"[dt.weekday()]
    return f"{dt.month}/{dt.day}（{wk}）"


def build_available_date_buttons(settings: Dict[str, Any]) -> List[Tuple[str, str]]:
    # 只回傳可選日期（min~max 且排除公休/不出貨日）
    today = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    out = []
    for i in range(settings["min_days"], settings["max_days"] + 1):
        d = today + timedelta(days=i)
        if not is_closed(d.date(), settings):
            out.append((fmt_md_date(d), d.strftime("%Y-%m-%d")))
    return out


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


def recalc_cart(sess: dict):
    for x in sess["cart"]:
        x["subtotal"] = int(x["unit_price"]) * int(x["qty"])


def find_cart_line_label(x: dict) -> str:
    name = x["label"]
    if x.get("flavor"):
        name += f"（{x['flavor']}）"
    qty = x["qty"]
    unit = x["unit_price"]
    sub = x["subtotal"]
    return f"{name} ×{qty}（{unit}/單位）＝{sub}"


def cart_readable_text(cart: List[dict]) -> str:
    parts = []
    for x in cart:
        label = x["label"]
        qty = x["qty"]
        flavor = (x.get("flavor") or "").strip()
        if flavor:
            parts.append(f"{label}｜{qty}｜{flavor}")
        else:
            parts.append(f"{label}｜{qty}")
    return "；".join(parts)


def is_phone_digits(s: str) -> bool:
    s = (s or "").strip()
    return s.isdigit() and 8 <= len(s) <= 10


# =========================
# Flex builders
# =========================
def flex_home_hint() -> dict:
    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "UooUoo 甜點訂購", "weight": "bold", "size": "xl"},
            {"type": "text", "text": "• 點「甜點」只看菜單\n• 點「我要下單」才會開始輸入資料與下訂",
             "wrap": True, "size": "sm", "color": "#666666"},
        ]}
    }


def flex_menu_view_only() -> dict:
    rows = []
    for _, meta in ITEMS.items():
        rows.append({
            "type": "box", "layout": "horizontal", "contents": [
                {"type": "text", "text": meta["label"], "flex": 7, "wrap": True},
                {"type": "text", "text": f"NT${meta['unit_price']}", "flex": 3, "align": "end", "color": "#666666"},
            ]
        })
    return {
        "type": "bubble",
        "size": "mega",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "甜點菜單", "weight": "bold", "size": "xl"},
            {"type": "text", "text": "（點「我要下單」才會開始下訂流程）", "size": "sm", "color": "#666666", "wrap": True},
            {"type": "separator"},
            *rows,
        ]}
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
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "請選擇店取或宅配", "weight": "bold", "size": "xl"},
            {"type": "text", "text": "（日期會排除公休/不出貨日）", "size": "sm", "color": "#666666"},
            {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🏪 店取", "data": "PB:PICKUP:店取", "displayText": "店取"}},
            {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🚚 冷凍宅配", "data": "PB:PICKUP:宅配", "displayText": "冷凍宅配"}},
        ]}
    }


def flex_phone_confirm(phone: str, kind: str) -> dict:
    ok_data = f"PB:PHONE_OK:{kind}"
    retry_data = f"PB:PHONE_RETRY:{kind}"
    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "電話二次確認", "weight": "bold", "size": "xl"},
            {"type": "text", "text": f"你填的電話：{phone}", "size": "md", "wrap": True},
            {"type": "text", "text": "請確認電話正確，避免通知不到你。", "size": "sm", "color": "#666666", "wrap": True},
        ]},
        "footer": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": [
            {"type": "button", "style": "primary", "action": {"type": "postback", "label": "✅ 正確", "data": ok_data, "displayText": "電話正確"}},
            {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "✏️ 重新輸入", "data": retry_data, "displayText": "重新輸入電話"}},
        ]}
    }


def flex_checkout_summary(sess: dict) -> dict:
    cart = sess["cart"]
    lines = [find_cart_line_label(x) for x in cart]
    total = cart_total(cart)

    method = sess.get("pickup_method") or "（未選）"

    if method == "宅配":
        fee = shipping_fee(total)
        grand = total + fee
        date_show = sess.get("delivery_date") or "（未選）"
        time_show = "—"
        bottom_text = f"小計：NT${total}\n運費：NT${fee}\n應付：NT${grand}"
    elif method == "店取":
        date_show = sess.get("pickup_date") or "（未選）"
        time_show = sess.get("pickup_time") or "（未選）"
        bottom_text = f"小計：NT${total}"
    else:
        date_show = "（未選）"
        time_show = "（未選）"
        bottom_text = f"小計：NT${total}"

    shown = lines[:10]
    if len(lines) > 10:
        shown.append(f"…等 {len(lines)} 項（請先刪減購物車）")

    list_text = "\n".join([f"• {s}" for s in shown]) if shown else "（購物車是空的）"

    return {
        "type": "bubble",
        "size": "mega",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "🧾 結帳內容", "weight": "bold", "size": "xl"},
            {"type": "text", "text": list_text, "wrap": True, "size": "sm"},
            {"type": "separator", "margin": "md"},
            {"type": "text", "text": f"取貨方式：{method}", "size": "sm", "color": "#666666"},
            {"type": "text", "text": f"日期：{date_show}", "size": "sm", "color": "#666666"},
            {"type": "text", "text": f"時段：{time_show}", "size": "sm", "color": "#666666"},
            {"type": "separator", "margin": "md"},
            {"type": "text", "text": bottom_text, "weight": "bold", "size": "lg"},
        ]},
        "footer": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": [
            {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🛠 修改品項", "data": "PB:EDIT:MENU", "displayText": "修改品項"}},
            {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "➕ 繼續加購", "data": "PB:CONTINUE", "displayText": "繼續加購"}},
            {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "✅ 下一步", "data": "PB:NEXT", "displayText": "下一步"}},
        ]}
    }


def flex_admin_notify(order_id: str, method: str) -> dict:
    if method == "店取":
        btn_label = "📣 已做好，通知客人取貨"
        data = f"ADMIN:READY:{order_id}"
        hint = "按下後會推播「已可取貨」給客人，並寫入 C 表 status=READY"
    else:
        btn_label = "🚚 已出貨，通知客人"
        data = f"ADMIN:SHIPPED:{order_id}"
        hint = "按下後會推播「已出貨」給客人，並寫入 C 表 status=SHIPPED"
    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "商家通知按鈕", "weight": "bold", "size": "xl"},
            {"type": "text", "text": hint, "size": "sm", "color": "#666666", "wrap": True},
            {"type": "text", "text": f"訂單：{order_id}", "size": "md", "wrap": True},
        ]},
        "footer": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": [
            {"type": "button", "style": "primary", "action": {"type": "postback", "label": btn_label, "data": data, "displayText": btn_label}},
        ]}
    }


# =========================
# Cart operations
# =========================
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
    min_qty = ITEMS[item_key]["min_qty"]
    return new_qty >= min_qty


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
# Order write: A/B/C
# =========================
def write_order_A(user_id: str, order_id: str, sess: dict) -> bool:
    cart = sess["cart"]
    total = cart_total(cart)

    pickup_method = sess.get("pickup_method") or ""
    pickup_date = sess.get("pickup_date") or ""
    pickup_time = sess.get("pickup_time") or ""

    note = ""
    if pickup_method == "宅配":
        delivery_date = sess.get("delivery_date") or ""
        dn = sess.get("delivery_name") or ""
        dp = sess.get("delivery_phone") or ""
        da = sess.get("delivery_address") or ""
        note = f"期望到貨:{delivery_date} | 收件人:{dn} | 電話:{dp} | 地址:{da}"
        pickup_date = delivery_date
        pickup_time = ""

    if pickup_method == "店取":
        pn = sess.get("pickup_name") or ""
        pp = sess.get("pickup_phone") or ""
        note = f"取件人:{pn} | 電話:{pp}"

    rowA = [
        now_str(),
        user_id,
        "",  # display_name
        order_id,
        json.dumps({"cart": cart}, ensure_ascii=False),
        pickup_method,
        pickup_date,
        pickup_time,
        note,
        total,
        "UNPAID",
        cart_readable_text(cart),  # transaction_note 白話
    ]
    return sheet_append(SHEET_A_NAME, rowA)


def write_order_B(order_id: str, sess: dict) -> bool:
    ok_all = True
    created_at = now_str()
    pickup_method = sess.get("pickup_method") or ""
    pickup_date = sess.get("pickup_date") or ""
    pickup_time = sess.get("pickup_time") or ""

    if pickup_method == "宅配":
        pickup_date = sess.get("delivery_date") or ""
        pickup_time = ""

    phone = sess.get("pickup_phone") if pickup_method == "店取" else sess.get("delivery_phone")

    for it in sess["cart"]:
        item_name = it["label"]
        if (it.get("flavor") or "").strip():
            item_name = f"{item_name}｜{it['flavor']}"
        rowB = [
            created_at,
            order_id,
            item_name,
            it["qty"],
            it["unit_price"],
            it["subtotal"],
            pickup_method,
            pickup_date,
            pickup_time,
            "UNPAID",
            phone or "",
        ]
        ok = sheet_append(SHEET_B_NAME, rowB)
        ok_all = ok_all and ok
    return ok_all


def write_order_C(order_id: str, sess: dict) -> bool:
    created_at = now_str()
    pickup_method = sess.get("pickup_method") or ""
    amount = cart_total(sess["cart"])
    fee = shipping_fee(amount) if pickup_method == "宅配" else 0
    grand = amount + fee

    note = ""
    if pickup_method == "店取":
        note = f"店取 {sess.get('pickup_date','')} {sess.get('pickup_time','')} | {sess.get('pickup_name','')} | {sess.get('pickup_phone','')}"
    else:
        note = f"宅配 期望到貨:{sess.get('delivery_date','')} | {sess.get('delivery_name','')} | {sess.get('delivery_phone','')}"

    rowC = [
        created_at,
        order_id,
        "ORDER",
        pickup_method,
        amount,
        fee,
        grand,
        "ORDER",
        note,
    ]
    return sheet_append(SHEET_C_NAME, rowC)


def write_status_C(order_id: str, status: str, note: str) -> bool:
    row = [now_str(), order_id, "STATUS", "", "", "", "", status, note]
    return sheet_append(SHEET_C_NAME, row)


def find_user_id_by_order_id(order_id: str) -> Optional[str]:
    rows = sheet_read_range(SHEET_A_NAME, "A1:L5000")
    if not rows or len(rows) < 2:
        return None
    for r in rows[1:]:
        if len(r) >= 4 and (r[3] or "").strip() == order_id:
            return (r[1] or "").strip()
    return None


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

    if etype == "message" and (ev.get("message") or {}).get("type") == "text":
        text = (ev["message"].get("text") or "").strip()

        if text == "甜點":
            line_reply(reply_token, [msg_flex("甜點菜單", flex_menu_view_only())])
            return

        if text == "我要下單":
            sess["ordering"] = True
            sess["state"] = "IDLE"
            line_reply(reply_token, [
                msg_text("好的，開始下單。\n請從菜單選擇商品加入購物車。"),
                msg_flex("甜點菜單", flex_product_menu(ordering=True)),
            ])
            return

        if text in ["清空重來", "清空", "reset"]:
            reset_session(sess)
            line_reply(reply_token, [msg_text("已清空，重新開始。\n請點「我要下單」開始，或點「甜點」先看菜單。")])
            return

        if text == "取貨說明":
            line_reply(reply_token, [msg_text(PICKUP_NOTICE + "\n\n" + DELIVERY_NOTICE)])
            return

        if text == "付款說明":
            line_reply(reply_token, [msg_text(BANK_TRANSFER_TEXT)])
            return

        if text.startswith("已轉帳"):
            line_reply(reply_token, [msg_text("收到，我們會核對帳款後依訂單號安排出貨/取貨。\n若需補充資訊也可以直接留言。")])
            return

        handle_state_text(user_id, reply_token, text)
        return

    if etype == "postback":
        data = (ev.get("postback") or {}).get("data", "")
        handle_postback(user_id, reply_token, data)
        return


def reset_session(sess: dict):
    sess["ordering"] = False
    sess["state"] = "IDLE"
    sess["cart"] = []
    sess["pending_item"] = None
    sess["pending_flavor"] = None

    sess["pickup_method"] = None
    sess["pickup_date"] = None
    sess["pickup_time"] = None
    sess["pickup_name"] = None
    sess["pickup_phone"] = None
    sess["pickup_phone_ok"] = False

    sess["delivery_date"] = None
    sess["delivery_name"] = None
    sess["delivery_phone"] = None
    sess["delivery_phone_ok"] = False
    sess["delivery_address"] = None

    sess["edit_mode"] = None


# =========================
# Postback flows
# =========================
def handle_postback(user_id: str, reply_token: str, data: str):
    sess = get_session(user_id)

    # ---- 管理員通知按鈕 ----
    if data.startswith("ADMIN:"):
        if ADMIN_USER_IDS and user_id not in ADMIN_USER_IDS:
            line_reply(reply_token, [msg_text("此功能僅限商家管理員使用。")])
            return

        parts = data.split(":", 2)
        if len(parts) != 3:
            line_reply(reply_token, [msg_text("管理員指令格式錯誤。")])
            return
        _, act, order_id = parts

        target_user = find_user_id_by_order_id(order_id)
        if not target_user:
            line_reply(reply_token, [msg_text(f"找不到訂單對應客人：{order_id}")])
            return

        if act == "READY":
            line_push(target_user, [msg_text(f"你的訂單已完成，可以來取貨了。\n訂單編號：{order_id}\n如需更改取貨時間請回覆訊息。")])
            write_status_C(order_id, "READY", "店取已做好通知")
            line_reply(reply_token, [msg_text("已通知客人（READY），並寫入 C 表。")])
            return

        if act == "SHIPPED":
            line_push(target_user, [msg_text(f"你的訂單已出貨。\n訂單編號：{order_id}\n提醒：運送可能因天候/物流量延遲。")])
            write_status_C(order_id, "SHIPPED", "宅配已出貨通知")
            line_reply(reply_token, [msg_text("已通知客人（SHIPPED），並寫入 C 表。")])
            return

        line_reply(reply_token, [msg_text("未知的管理員動作。")])
        return

    if data == "PB:RESET":
        reset_session(sess)
        line_reply(reply_token, [msg_text("已清空。\n請點「我要下單」開始，或點「甜點」先看菜單。")])
        return

    if data == "PB:CONTINUE":
        if not sess["ordering"]:
            line_reply(reply_token, [msg_text("請先點「我要下單」開始下單流程。")])
            return
        line_reply(reply_token, [msg_flex("甜點菜單", flex_product_menu(ordering=True))])
        return

    if data == "PB:CHECKOUT":
        if not sess["ordering"]:
            line_reply(reply_token, [msg_text("請先點「我要下單」開始下單流程。")])
            return
        if not sess["cart"]:
            line_reply(reply_token, [msg_text("購物車是空的，請先選商品。"), msg_flex("甜點菜單", flex_product_menu(ordering=True))])
            return

        sess["state"] = "WAIT_PICKUP_METHOD"
        line_reply(reply_token, [msg_flex("取貨方式", flex_pickup_method())])
        return

    if data.startswith("PB:ITEM:"):
        if not sess["ordering"]:
            line_reply(reply_token, [msg_text("想下單請先點「我要下單」。\n你也可以點「甜點」先看菜單。")])
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

    if data.startswith("PB:PICKUP:"):
        method = data.split("PB:PICKUP:", 1)[1].strip()
        sess["pickup_method"] = method

        settings = load_settings()
        date_buttons = build_available_date_buttons(settings)
        if not date_buttons:
            line_reply(reply_token, [msg_text("近期可選日期不足（可能都遇到公休/不出貨日）。請調整公休日設定後再試。")])
            return

        quick_items = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]

        if method == "店取":
            sess["state"] = "WAIT_PICKUP_DATE"
            line_reply(reply_token, [msg_text("請選「店取日期」（3～14天內，已排除公休）：", quick_items=quick_items)])
            return

        if method == "宅配":
            sess["state"] = "WAIT_DELIVERY_DATE"
            line_reply(reply_token, [msg_text("請選「期望到貨日」（3～14天內；不保證準時，僅作期望日；已排除公休）：", quick_items=quick_items)])
            return

    # DATE：這裡再二次防呆（避免客人點到舊 quickreply）
    if data.startswith("PB:DATE:"):
        ymd = data.split("PB:DATE:", 1)[1].strip()
        settings = load_settings()
        try:
            d_obj = datetime.strptime(ymd, "%Y-%m-%d").date()
            if is_closed(d_obj, settings):
                # 重新提供可選日期（避免又回到可選公休）
                date_buttons = build_available_date_buttons(settings)
                q = [quick_postback(lbl, f"PB:DATE:{d2}", display_text=lbl) for (lbl, d2) in date_buttons]
                line_reply(reply_token, [msg_text("此日期為公休/不出貨日，請重新選擇：", quick_items=q)])
                return
        except:
            line_reply(reply_token, [msg_text("日期格式錯誤，請重新選擇。")])
            return

        if sess["state"] == "WAIT_PICKUP_DATE":
            sess["pickup_date"] = ymd
            sess["state"] = "WAIT_PICKUP_TIME"
            q = [quick_postback(s, f"PB:TIME:{s}", display_text=s) for s in PICKUP_SLOTS]
            line_reply(reply_token, [msg_text(f"✅ 已選店取日期：{ymd}\n請選店取時段：", quick_items=q)])
            return

        if sess["state"] == "WAIT_DELIVERY_DATE":
            sess["delivery_date"] = ymd
            sess["state"] = "WAIT_DELIVERY_NAME"
            line_reply(reply_token, [msg_text(f"✅ 已選期望到貨日：{ymd}\n請輸入宅配收件人姓名：")])
            return

        line_reply(reply_token, [msg_text("日期已收到，但目前流程不在選日期階段。請點「前往結帳」重新操作。")])
        return

    if data.startswith("PB:TIME:") and sess["state"] == "WAIT_PICKUP_TIME":
        t = data.split("PB:TIME:", 1)[1].strip()
        sess["pickup_time"] = t
        sess["state"] = "WAIT_PICKUP_NAME"
        line_reply(reply_token, [msg_text(
            f"✅ 店取資訊已選好：\n日期：{sess.get('pickup_date')}\n時段：{t}\n地址：{PICKUP_ADDRESS}\n\n請輸入取件人姓名："
        )])
        return

    if data.startswith("PB:PHONE_OK:"):
        kind = data.split("PB:PHONE_OK:", 1)[1].strip()
        if kind == "PICKUP":
            sess["pickup_phone_ok"] = True
            sess["state"] = "IDLE"
            line_reply(reply_token, [msg_text("✅ 電話已確認"), msg_flex("結帳內容", flex_checkout_summary(sess))])
            return
        if kind == "DELIVERY":
            sess["delivery_phone_ok"] = True
            sess["state"] = "IDLE"
            line_reply(reply_token, [msg_text("✅ 電話已確認"), msg_flex("結帳內容", flex_checkout_summary(sess))])
            return

    if data.startswith("PB:PHONE_RETRY:"):
        kind = data.split("PB:PHONE_RETRY:", 1)[1].strip()
        if kind == "PICKUP":
            sess["pickup_phone"] = None
            sess["pickup_phone_ok"] = False
            sess["state"] = "WAIT_PICKUP_PHONE"
            line_reply(reply_token, [msg_text("請重新輸入店取電話（純數字）：")])
            return
        if kind == "DELIVERY":
            sess["delivery_phone"] = None
            sess["delivery_phone_ok"] = False
            sess["state"] = "WAIT_DELIVERY_PHONE"
            line_reply(reply_token, [msg_text("請重新輸入宅配電話（純數字）：")])
            return

    if data == "PB:EDIT:MENU":
        if not sess["cart"]:
            line_reply(reply_token, [msg_text("購物車是空的，無法修改。")])
            return
        sess["state"] = "EDIT_MENU"
        q = [
            quick_postback("➕ 增加數量", "PB:EDITMODE:INC", display_text="增加數量"),
            quick_postback("➖ 減少數量", "PB:EDITMODE:DEC", display_text="減少數量"),
            quick_postback("🗑 移除品項", "PB:EDITMODE:DEL", display_text="移除品項"),
            quick_postback("🍵 修改口味", "PB:EDITMODE:FLAVOR", display_text="修改口味"),
        ]
        line_reply(reply_token, [msg_text("請選要修改的方式：", quick_items=q)])
        return

    if data.startswith("PB:EDITMODE:"):
        mode = data.split("PB:EDITMODE:", 1)[1].strip()
        sess["edit_mode"] = mode
        sess["state"] = "EDIT_PICK_ITEM"
        q = build_cart_item_choices(sess, mode)
        line_reply(reply_token, [msg_text("請選要修改的品項：", quick_items=q)])
        return

    if data.startswith("PB:EDIT:"):
        parts = data.split(":")
        if len(parts) != 4:
            line_reply(reply_token, [msg_text("修改指令格式錯誤，請重新操作。")])
            return
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
        elif mode == "FLAVOR":
            if not ITEMS[item_key]["has_flavor"]:
                line_reply(reply_token, [msg_text("此品項沒有口味可修改。")])
                return
            sess["state"] = "WAIT_EDIT_FLAVOR"
            sess["pending_item"] = item_key
            sess["pending_flavor"] = idx  # 暫存 idx
            q = [quick_postback(f, f"PB:SETFLAVOR:{f}", display_text=f) for f in ITEMS[item_key]["flavors"]]
            line_reply(reply_token, [msg_text("請選新口味：", quick_items=q)])
            return
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

    if data.startswith("PB:SETFLAVOR:") and sess.get("state") == "WAIT_EDIT_FLAVOR":
        new_flavor = data.split("PB:SETFLAVOR:", 1)[1].strip()
        idx = sess.get("pending_flavor")
        if idx is None or not isinstance(idx, int) or idx < 0 or idx >= len(sess["cart"]):
            line_reply(reply_token, [msg_text("修改口味失敗，請重新操作。")])
            return
        sess["cart"][idx]["flavor"] = new_flavor
        sess["state"] = "IDLE"
        sess["pending_item"] = None
        sess["pending_flavor"] = None
        recalc_cart(sess)
        line_reply(reply_token, [msg_text("✅ 口味已更新"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

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
                settings = load_settings()
                date_buttons = build_available_date_buttons(settings)
                q = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]
                line_reply(reply_token, [msg_text("請選店取日期：", quick_items=q)])
                return
            if not sess.get("pickup_time"):
                sess["state"] = "WAIT_PICKUP_TIME"
                q = [quick_postback(s, f"PB:TIME:{s}", display_text=s) for s in PICKUP_SLOTS]
                line_reply(reply_token, [msg_text("請選店取時段：", quick_items=q)])
                return
            if not sess.get("pickup_name"):
                sess["state"] = "WAIT_PICKUP_NAME"
                line_reply(reply_token, [msg_text("請輸入取件人姓名：")])
                return
            if not sess.get("pickup_phone"):
                sess["state"] = "WAIT_PICKUP_PHONE"
                line_reply(reply_token, [msg_text("請輸入店取電話（純數字）：")])
                return
            if not sess.get("pickup_phone_ok"):
                line_reply(reply_token, [msg_flex("電話確認", flex_phone_confirm(sess["pickup_phone"], "PICKUP"))])
                return

        if sess["pickup_method"] == "宅配":
            if not sess.get("delivery_date"):
                sess["state"] = "WAIT_DELIVERY_DATE"
                settings = load_settings()
                date_buttons = build_available_date_buttons(settings)
                q = [quick_postback(lbl, f"PB:DATE:{ymd}", display_text=lbl) for (lbl, ymd) in date_buttons]
                line_reply(reply_token, [msg_text("請選期望到貨日：", quick_items=q)])
                return
            if not sess.get("delivery_name"):
                sess["state"] = "WAIT_DELIVERY_NAME"
                line_reply(reply_token, [msg_text("請輸入宅配收件人姓名：")])
                return
            if not sess.get("delivery_phone"):
                sess["state"] = "WAIT_DELIVERY_PHONE"
                line_reply(reply_token, [msg_text("請輸入宅配電話（純數字）：")])
                return
            if not sess.get("delivery_phone_ok"):
                line_reply(reply_token, [msg_flex("電話確認", flex_phone_confirm(sess["delivery_phone"], "DELIVERY"))])
                return
            if not sess.get("delivery_address"):
                sess["state"] = "WAIT_DELIVERY_ADDRESS"
                line_reply(reply_token, [msg_text("請輸入宅配地址（完整地址）：")])
                return

        order_id = gen_order_id()

        okA = write_order_A(user_id, order_id, sess)
        okB = write_order_B(order_id, sess)
        okC = write_order_C(order_id, sess)

        if not (okA and okB and okC):
            line_reply(reply_token, [msg_text(
                "訂單已建立，但表單寫入可能有錯誤。\n"
                "請看後台 log：多半是分頁名稱不一致（尤其 C 表 cashflow），或 service account 權限不足。"
            )])

        total = cart_total(sess["cart"])
        fee = shipping_fee(total) if sess["pickup_method"] == "宅配" else 0
        grand = total + fee
        summary_lines = "\n".join([f"• {find_cart_line_label(x)}" for x in sess["cart"]])

        if sess["pickup_method"] == "店取":
            msg = (
                "✅ 訂單已建立（待轉帳）\n"
                f"訂單編號：{order_id}\n\n"
                f"{summary_lines}\n\n"
                "【店取資訊】\n"
                f"日期：{sess['pickup_date']}\n"
                f"時段：{sess['pickup_time']}\n"
                f"取件人：{sess['pickup_name']}\n"
                f"電話：{sess['pickup_phone']}\n"
                f"地址：{PICKUP_ADDRESS}\n\n"
                f"小計：NT${total}\n\n"
                + BANK_TRANSFER_TEXT
            )
        else:
            msg = (
                "✅ 訂單已建立（待轉帳）\n"
                f"訂單編號：{order_id}\n\n"
                f"{summary_lines}\n\n"
                "【宅配資訊】\n"
                f"期望到貨日：{sess['delivery_date']}（不保證準時）\n"
                f"收件人：{sess['delivery_name']}\n"
                f"電話：{sess['delivery_phone']}\n"
                f"地址：{sess['delivery_address']}\n\n"
                f"小計：NT${total}\n運費：NT${fee}\n應付：NT${grand}\n\n"
                + DELIVERY_NOTICE
                + "\n\n"
                + BANK_TRANSFER_TEXT
            )

        line_reply(reply_token, [msg_text(msg)])

        if ADMIN_USER_IDS:
            admin_card = msg_flex("商家通知按鈕", flex_admin_notify(order_id, sess["pickup_method"]))
            for admin_uid in ADMIN_USER_IDS:
                line_push(admin_uid, [admin_card])

        reset_session(sess)
        return

    line_reply(reply_token, [msg_text("已收到操作，但流程未對上。請點「我要下單」重新開始。")])


# =========================
# State text handlers
# =========================
def handle_state_text(user_id: str, reply_token: str, text: str):
    sess = get_session(user_id)

    if not sess["ordering"]:
        line_reply(reply_token, [msg_flex("提示", flex_home_hint())])
        return

    if sess["state"] == "WAIT_PICKUP_NAME":
        sess["pickup_name"] = text.strip()
        sess["state"] = "WAIT_PICKUP_PHONE"
        line_reply(reply_token, [msg_text("請輸入店取電話（純數字）：")])
        return

    if sess["state"] == "WAIT_PICKUP_PHONE":
        if not is_phone_digits(text):
            line_reply(reply_token, [msg_text("電話格式看起來不對，請輸入純數字（例如 09xxxxxxxx）。")])
            return
        sess["pickup_phone"] = text.strip()
        sess["pickup_phone_ok"] = False
        sess["state"] = "IDLE"
        line_reply(reply_token, [
            msg_text("已收到店取電話，請二次確認："),
            msg_flex("電話確認", flex_phone_confirm(sess["pickup_phone"], "PICKUP"))
        ])
        return

    if sess["state"] == "WAIT_DELIVERY_NAME":
        sess["delivery_name"] = text.strip()
        sess["state"] = "WAIT_DELIVERY_PHONE"
        line_reply(reply_token, [msg_text("請輸入宅配電話（純數字）：")])
        return

    if sess["state"] == "WAIT_DELIVERY_PHONE":
        if not is_phone_digits(text):
            line_reply(reply_token, [msg_text("電話格式看起來不對，請輸入純數字（例如 09xxxxxxxx）。")])
            return
        sess["delivery_phone"] = text.strip()
        sess["delivery_phone_ok"] = False
        sess["state"] = "IDLE"
        line_reply(reply_token, [
            msg_text("已收到宅配電話，請二次確認："),
            msg_flex("電話確認", flex_phone_confirm(sess["delivery_phone"], "DELIVERY"))
        ])
        return

    if sess["state"] == "WAIT_DELIVERY_ADDRESS":
        sess["delivery_address"] = text.strip()
        sess["state"] = "IDLE"
        line_reply(reply_token, [msg_text("✅ 已收到宅配地址"), msg_flex("結帳內容", flex_checkout_summary(sess))])
        return

    line_reply(reply_token, [msg_text("我有收到你的訊息，但目前建議用按鈕操作。\n要看菜單請點「甜點」，要下單請點「我要下單」。")])
