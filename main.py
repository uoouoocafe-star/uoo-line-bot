import os
import json
import base64
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse

from linebot.v3.webhook import WebhookHandler
from linebot.v3.webhooks import MessageEvent, TextMessageContent

from linebot.v3.messaging import (
    ApiClient,
    Configuration,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
    RichMenuRequest,
    RichMenuSize,
    RichMenuArea,
    RichMenuBounds,
    URIAction,
    MessageAction,
)
from linebot.v3.exceptions import InvalidSignatureError

from google.oauth2 import service_account
from googleapiclient.discovery import build


# =========================
# Timezone / Utilities
# =========================
TZ_TAIPEI = timezone(timedelta(hours=8))


def now_tpe() -> datetime:
    return datetime.now(TZ_TAIPEI)


def gen_order_id() -> str:
    # ex: UOO-20260107-103012-4821
    ts = now_tpe().strftime("%Y%m%d-%H%M%S")
    suffix = random.randint(1000, 9999)
    return f"UOO-{ts}-{suffix}"


def safe_env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if (v is not None and str(v).strip() != "") else default


def require_env(name: str) -> str:
    v = safe_env(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


# =========================
# Product / Policy
# =========================
DACQ_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

MENU = {
    "dacquoise": {
        "name": "達克瓦茲",
        "price": 95,
        "min_qty": 2,
        "no_mix_flavor": True,  # 口味不可混
        "flavors": DACQ_FLAVORS,
    },
    "scone": {"name": "原味司康", "price": 65, "min_qty": 1},
    "canele": {"name": "原味可麗露", "price": 90, "min_qty": 1},
    "toast": {
        "name": "伊思尼奶酥厚片",
        "price": 85,
        "min_qty": 1,
        "flavors": DACQ_FLAVORS,
    },
}

POLICY_TEXT = """📌 全部甜點皆為「前三天預訂製作」
📦 取貨方式：
▪ 店取：新竹縣竹北市隘口六街65號
▪ 宅配：冷凍宅配（大榮貨運）運費 180 元／滿 2500 免運

🚚 宅配提醒
・保持電話暢通，避免退件
・收到後立即開箱確認並盡快冷藏/冷凍
・若嚴重損壞（如糊爛、不成形）請拍照（含原箱）並當日聯繫
・未處理完前請保留原樣（勿丟棄/食用）

⚠️ 風險認知
・運送過程輕微位移、裝飾掉落通常不在理賠範圍
・遇天災物流可能延遲或暫停，無法保證準時送達
"""


def menu_text() -> str:
    return (
        "🍰 UooUoo 甜點預訂\n\n"
        "1️⃣ 達克瓦茲 95/顆（口味不可混｜2 顆起）\n"
        f"口味：{ '、'.join(DACQ_FLAVORS) }\n"
        "2️⃣ 原味司康 65/顆\n"
        "3️⃣ 原味可麗露 90/顆（限冷凍保存）\n"
        "4️⃣ 伊思尼奶酥厚片 85/片\n"
        f"口味：{ '、'.join(DACQ_FLAVORS) }\n\n"
        "✏️ 下單格式（直接複製貼上填數量）：\n"
        "達克瓦茲 原味 x2\n"
        "司康 x3\n"
        "可麗露 x2\n"
        "奶酥厚片 焙茶 x4\n"
        "取貨方式：店取/宅配\n"
        "取貨日期：YYYY-MM-DD\n"
        "取貨時段：例如 14:00-16:00\n"
        "備註：＿＿＿（可空）\n\n"
        + POLICY_TEXT
    )


# =========================
# Google Sheets
# =========================
def load_service_account_info() -> Dict[str, Any]:
    """
    支援兩種 env：
    - GOOGLE_SERVICE_ACCOUNT_B64 : base64 的整份 JSON
    - GOOGLE_SERVICE_ACCOUNT_JSON: 直接貼 JSON（較容易換行出錯，不建議）
    """
    b64 = safe_env("GOOGLE_SERVICE_ACCOUNT_B64")
    raw_json = safe_env("GOOGLE_SERVICE_ACCOUNT_JSON")

    if b64:
        try:
            decoded = base64.b64decode(b64).decode("utf-8")
            return json.loads(decoded)
        except Exception as e:
            raise RuntimeError(f"Invalid GOOGLE_SERVICE_ACCOUNT_B64: {e}")

    if raw_json:
        try:
            return json.loads(raw_json)
        except Exception as e:
            raise RuntimeError(f"Invalid GOOGLE_SERVICE_ACCOUNT_JSON: {e}")

    raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_B64 or GOOGLE_SERVICE_ACCOUNT_JSON")


def sheets_service():
    info = load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def append_order_row(row: List[Any]) -> None:
    spreadsheet_id = require_env("GSHEET_ID")
    sheet_name = safe_env("GSHEET_SHEET_NAME", "sheet1")
    rng = f"{sheet_name}!A:L"
    svc = sheets_service()
    body = {"values": [row]}
    svc.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


def find_order_row_index(order_id: str, max_rows: int = 2000) -> Optional[int]:
    """
    回傳「資料列 index（1-based）」：例如第 2 列代表 row_index=2
    假設 headers 在第 1 列，order_id 在 D 欄（第 4 欄）。
    """
    spreadsheet_id = require_env("GSHEET_ID")
    sheet_name = safe_env("GSHEET_SHEET_NAME", "sheet1")
    rng = f"{sheet_name}!A1:L{max_rows}"
    svc = sheets_service()
    resp = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = resp.get("values", [])
    if not values:
        return None

    # 找 D 欄（index 3）
    for i, row in enumerate(values, start=1):
        if len(row) >= 4 and row[3] == order_id:
            return i
    return None


def update_order_cells(order_row_index: int, updates: Dict[str, Any]) -> None:
    """
    依欄位名稱更新。你目前 sheet 欄位順序：
    A created_at
    B user_id
    C display_name
    D order_id
    E items_json
    F pickup_method
    G pickup_date
    H pickup_time
    I note
    J amount
    K pay_status
    L linepay_transaction_id（可留空）
    """
    col_map = {
        "note": "I",
        "amount": "J",
        "pay_status": "K",
    }
    spreadsheet_id = require_env("GSHEET_ID")
    sheet_name = safe_env("GSHEET_SHEET_NAME", "sheet1")
    svc = sheets_service()

    data = []
    for k, v in updates.items():
        if k not in col_map:
            continue
        a1 = f"{sheet_name}!{col_map[k]}{order_row_index}"
        data.append({"range": a1, "values": [[v]]})

    if not data:
        return

    body = {"valueInputOption": "RAW", "data": data}
    svc.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


# =========================
# LINE Messaging
# =========================
CHANNEL_ACCESS_TOKEN = require_env("CHANNEL_ACCESS_TOKEN")
CHANNEL_SECRET = require_env("CHANNEL_SECRET")

line_config = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

app = FastAPI()


def reply_text(reply_token: str, text: str) -> None:
    with ApiClient(line_config) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[TextMessage(text=text)],
            )
        )


# =========================
# Order Parsing
# =========================
def parse_qty(line: str) -> Optional[int]:
    # 支援 x2 / X2 / 2顆 / 2片 / 2
    m = re.search(r"[xX]\s*(\d+)", line)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*(顆|片)?", line)
    if m:
        return int(m.group(1))
    return None


def parse_pickup_method(text: str) -> Optional[str]:
    if "店取" in text:
        return "店取"
    if "宅配" in text:
        return "宅配"
    return None


def parse_date(text: str) -> Optional[str]:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    return m.group(1) if m else None


def parse_time_range(text: str) -> Optional[str]:
    # e.g. 14:00-16:00
    m = re.search(r"(\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2})", text)
    if m:
        return m.group(1).replace(" ", "")
    return None


def validate_preorder_date(date_str: str) -> Tuple[bool, str]:
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=TZ_TAIPEI)
    except Exception:
        return False, "取貨日期格式請用 YYYY-MM-DD，例如 2026-01-10"

    min_dt = (now_tpe() + timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
    if dt < min_dt:
        return False, "全部甜點需「前三天預訂」。請選擇今天起算第 3 天（含）之後的日期。"
    return True, ""


def parse_items(lines: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    回傳 (items, errors)
    items: [{key, name, flavor, qty, unit_price, subtotal}]
    """
    items = []
    errors = []

    # 1) 達克瓦茲：必須寫口味，且不可混
    dacq_lines = [ln for ln in lines if "達克瓦茲" in ln]
    if dacq_lines:
        # 若使用者分多行寫不同口味，視為「口味混了」→ 直接拒絕
        flavors_found = []
        total_qty = 0
        for ln in dacq_lines:
            flavor = None
            for f in DACQ_FLAVORS:
                if f in ln:
                    flavor = f
                    break
            if not flavor:
                errors.append("達克瓦茲請指定口味（原味/蜜香紅茶/抹茶/焙茶/可可）。")
                continue
            qty = parse_qty(ln) or 0
            total_qty += qty
            flavors_found.append(flavor)

        uniq = sorted(set(flavors_found))
        if len(uniq) > 1:
            errors.append("達克瓦茲口味不可混：請同一筆訂單只選 1 種口味。")
        if total_qty and total_qty < MENU["dacquoise"]["min_qty"]:
            errors.append("達克瓦茲每項最低購買數量為 2 顆起。")

        if (not errors) and total_qty > 0:
            unit = MENU["dacquoise"]["price"]
            items.append(
                {
                    "key": "dacquoise",
                    "name": MENU["dacquoise"]["name"],
                    "flavor": uniq[0],
                    "qty": total_qty,
                    "unit_price": unit,
                    "subtotal": total_qty * unit,
                }
            )

    # 2) 司康
    for ln in lines:
        if "司康" in ln:
            qty = parse_qty(ln) or 0
            if qty <= 0:
                errors.append("司康請填數量，例如：司康 x2")
            else:
                unit = MENU["scone"]["price"]
                items.append(
                    {
                        "key": "scone",
                        "name": MENU["scone"]["name"],
                        "flavor": None,
                        "qty": qty,
                        "unit_price": unit,
                        "subtotal": qty * unit,
                    }
                )
            break

    # 3) 可麗露
    for ln in lines:
        if "可麗露" in ln:
            qty = parse_qty(ln) or 0
            if qty <= 0:
                errors.append("可麗露請填數量，例如：可麗露 x2")
            else:
                unit = MENU["canele"]["price"]
                items.append(
                    {
                        "key": "canele",
                        "name": MENU["canele"]["name"],
                        "flavor": None,
                        "qty": qty,
                        "unit_price": unit,
                        "subtotal": qty * unit,
                    }
                )
            break

    # 4) 奶酥厚片（要口味）
    for ln in lines:
        if ("奶酥" in ln) or ("厚片" in ln):
            flavor = None
            for f in DACQ_FLAVORS:
                if f in ln:
                    flavor = f
                    break
            if not flavor:
                errors.append("奶酥厚片請指定口味（原味/蜜香紅茶/抹茶/焙茶/可可）。")
                continue
            qty = parse_qty(ln) or 0
            if qty <= 0:
                errors.append("奶酥厚片請填數量，例如：奶酥厚片 焙茶 x3")
                continue
            unit = MENU["toast"]["price"]
            items.append(
                {
                    "key": "toast",
                    "name": MENU["toast"]["name"],
                    "flavor": flavor,
                    "qty": qty,
                    "unit_price": unit,
                    "subtotal": qty * unit,
                }
            )
            break

    # 合併同品項（司康/可麗露/厚片可能只會出現一次；保險起見）
    merged: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = {}
    for it in items:
        k = (it["key"], it.get("flavor"))
        if k not in merged:
            merged[k] = dict(it)
        else:
            merged[k]["qty"] += it["qty"]
            merged[k]["subtotal"] += it["subtotal"]
    items = list(merged.values())

    # 至少要有一個品項
    if not items:
        errors.append("我沒有讀到你要買的品項。你可以輸入「甜點」查看菜單與下單格式。")

    return items, errors


def calc_shipping(pickup_method: str, amount: int) -> int:
    if pickup_method == "店取":
        return 0
    # 宅配：180 / 滿 2500 免運
    return 0 if amount >= 2500 else 180


def summarize_items(items: List[Dict[str, Any]]) -> str:
    lines = []
    for it in items:
        flavor = f"（{it['flavor']}）" if it.get("flavor") else ""
        lines.append(f"- {it['name']}{flavor} x{it['qty']} = {it['subtotal']} 元")
    return "\n".join(lines)


# =========================
# Payment Instructions (Transfer)
# =========================
def transfer_instructions(order_id: str, total: int) -> str:
    # 你已經有轉帳帳號了；這裡只做模板，不硬寫入金流 ID / QR
    bank_name = safe_env("BANK_NAME", "台灣銀行（004）")
    bank_account = safe_env("BANK_ACCOUNT", "（請在 Render Env 設定 BANK_ACCOUNT）")
    pay_deadline_hours = safe_env("PAY_DEADLINE_HOURS", "24")

    return (
        f"✅ 已建立訂單\n"
        f"訂單編號：{order_id}\n"
        f"應付金額：{total} 元\n\n"
        f"🏦 付款方式：銀行轉帳\n"
        f"- 銀行：{bank_name}\n"
        f"- 帳號：{bank_account}\n"
        f"- 請於 {pay_deadline_hours} 小時內完成轉帳\n\n"
        f"📩 轉帳後請回傳：\n"
        f"「已轉帳 {order_id} 末五碼12345」\n\n"
        f"（我們核帳後會依訂單號碼陸續排單出貨/通知取貨）"
    )


# =========================
# In-memory State (for guided ordering)
# Render free instance 可能重啟，但你目前主流程是「一次貼完整下單格式」為主
# =========================
USER_STATE: Dict[str, Dict[str, Any]] = {}


# =========================
# Webhook /callback
# =========================
@app.get("/", response_class=PlainTextResponse)
async def health():
    return "ok"


@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    body_text = body.decode("utf-8")

    try:
        handler.handle(body_text, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    except Exception as e:
        # 避免 webhook 失敗造成 LINE 重送
        return JSONResponse({"ok": False, "error": str(e)}, status_code=200)

    return JSONResponse({"ok": True}, status_code=200)


# =========================
# Admin endpoints (B + C)
# 你可以用瀏覽器或 curl 打，建議加 ADMIN_TOKEN
# =========================
def check_admin(request: Request):
    admin_token = safe_env("ADMIN_TOKEN")
    if not admin_token:
        return  # 若你沒設 ADMIN_TOKEN，就不擋（不建議）
    token = request.headers.get("X-Admin-Token") or request.query_params.get("token")
    if token != admin_token:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.post("/admin/mark_paid")
async def admin_mark_paid(request: Request):
    check_admin(request)
    payload = await request.json()
    order_id = payload.get("order_id")
    note = payload.get("note", "")
    if not order_id:
        raise HTTPException(status_code=400, detail="order_id required")

    row_idx = find_order_row_index(order_id)
    if not row_idx:
        raise HTTPException(status_code=404, detail="order_id not found")

    update_order_cells(row_idx, {"pay_status": "paid", "note": note})
    return {"ok": True, "order_id": order_id}


@app.post("/admin/mark_shipped")
async def admin_mark_shipped(request: Request):
    check_admin(request)
    payload = await request.json()
    order_id = payload.get("order_id")
    note = payload.get("note", "")
    if not order_id:
        raise HTTPException(status_code=400, detail="order_id required")

    row_idx = find_order_row_index(order_id)
    if not row_idx:
        raise HTTPException(status_code=404, detail="order_id not found")

    update_order_cells(row_idx, {"pay_status": "shipped", "note": note})
    return {"ok": True, "order_id": order_id}


# =========================
# C: Rich Menu / Flex Menu scaffolding
# 你之後可以：
# - 在 LINE Official Account Manager 建好 Rich Menu / 或用 API 建
# - 把 RICH_MENU_ID 放進 env
# - 呼叫 /admin/richmenu/apply_default 給所有使用者（或新使用者）
# =========================
@app.post("/admin/richmenu/apply_default")
async def admin_apply_richmenu_default(request: Request):
    check_admin(request)
    rich_menu_id = safe_env("RICH_MENU_ID")
    if not rich_menu_id:
        raise HTTPException(status_code=400, detail="Missing env: RICH_MENU_ID")

    # 這個 API 是「把 rich menu 設成 default」：套用到所有使用者
    with ApiClient(line_config) as api_client:
        api = MessagingApi(api_client)
        api.set_default_rich_menu(rich_menu_id)

    return {"ok": True, "rich_menu_id": rich_menu_id}


# =========================
# LINE Message Handler
# =========================
@handler.add(MessageEvent, message=TextMessageContent)
def on_text(event: MessageEvent):
    user_id = event.source.user_id if event.source else None
    text = (event.message.text or "").strip()

    # 1) 快捷指令
    if text in ["甜點", "菜單", "menu", "Menu"]:
        reply_text(event.reply_token, menu_text())
        return

    # 2) 付款回報：已轉帳 UOO-... 末五 👉 自動標 paid
    # 格式：已轉帳 {order_id} 末五碼12345
    if text.startswith("已轉帳") or text.startswith("已付款"):
        order_id = None
        m = re.search(r"(UOO-\d{8}-\d{6}-\d{4})", text)
        if m:
            order_id = m.group(1)

        tail5 = None
        m2 = re.search(r"末五碼\s*(\d{5})", text)
        if m2:
            tail5 = m2.group(1)

        if not order_id:
            reply_text(event.reply_token, "我沒看到訂單編號。請用：已轉帳 UOO-xxxx 末五碼12345")
            return

        row_idx = find_order_row_index(order_id)
        if not row_idx:
            reply_text(event.reply_token, "我找不到這筆訂單編號，請確認是否輸入正確。")
            return

        note = f"客回報末五碼:{tail5}" if tail5 else "客回報已付款"
        update_order_cells(row_idx, {"pay_status": "paid", "note": note})
        reply_text(event.reply_token, f"收到，我們已記錄付款回報 ✅\n訂單：{order_id}\n核帳後會依序排單出貨/通知取貨。")
        return

    # 3) 下單：允許使用者一次貼完整格式（最穩）
    # 我們用「包含取貨方式/日期」來判斷是下單訊息
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    has_method = any(("取貨方式" in ln) for ln in lines) or ("店取" in text) or ("宅配" in text)
    has_date = any(("取貨日期" in ln) for ln in lines) or bool(parse_date(text))

    if has_method or has_date or any(("達克瓦茲" in ln) or ("司康" in ln) or ("可麗露" in ln) or ("奶酥" in ln) for ln in lines):
        try:
            # 取貨資訊
            pickup_method = None
            for ln in lines:
                if "取貨方式" in ln:
                    pickup_method = parse_pickup_method(ln)
            pickup_method = pickup_method or parse_pickup_method(text)

            pickup_date = None
            for ln in lines:
                if "取貨日期" in ln:
                    pickup_date = parse_date(ln)
            pickup_date = pickup_date or parse_date(text)

            pickup_time = None
            for ln in lines:
                if "取貨時段" in ln or "取貨時間" in ln:
                    pickup_time = parse_time_range(ln)
            pickup_time = pickup_time or parse_time_range(text)

            note = ""
            for ln in lines:
                if ln.startswith("備註"):
                    note = ln.split("：", 1)[-1].strip() if "：" in ln else ln.replace("備註", "").strip()

            # items
            items, errors = parse_items(lines)

            if not pickup_method:
                errors.append("請補上取貨方式：店取 或 宅配（例如：取貨方式：宅配）")
            if not pickup_date:
                errors.append("請補上取貨日期（YYYY-MM-DD）例如：取貨日期：2026-01-10")
            if pickup_date:
                ok, msg = validate_preorder_date(pickup_date)
                if not ok:
                    errors.append(msg)

            if errors:
                reply_text(
                    event.reply_token,
                    "❗ 下單資訊需要補齊/修正：\n" + "\n".join([f"- {e}" for e in errors]) + "\n\n你可以輸入「甜點」看菜單與格式。",
                )
                return

            # amount
            subtotal = sum(int(it["subtotal"]) for it in items)
            shipping = calc_shipping(pickup_method, subtotal)
            total = subtotal + shipping

            order_id = gen_order_id()
            created_at = now_tpe().strftime("%Y-%m-%d %H:%M:%S")

            # display_name：為了穩定先不去 call profile（避免額外權限/錯誤）
            display_name = "LINE客人"

            row = [
                created_at,              # A created_at
                user_id or "",           # B user_id
                display_name,            # C display_name
                order_id,                # D order_id
                json.dumps(items, ensure_ascii=False),  # E items_json
                pickup_method,           # F pickup_method
                pickup_date,             # G pickup_date
                pickup_time or "",       # H pickup_time
                note or "",              # I note
                total,                   # J amount
                "pending",               # K pay_status
                "",                      # L linepay_transaction_id (不用)
            ]

            append_order_row(row)

            summary = (
                "🧾 訂單內容\n"
                + summarize_items(items)
                + "\n"
                + (f"\n📦 宅配運費：{shipping} 元" if pickup_method == "宅配" else "\n📦 店取：運費 0 元")
                + f"\n💰 小計：{subtotal} 元\n💰 總計：{total} 元\n\n"
            )

            reply = summary + transfer_instructions(order_id, total)
            reply_text(event.reply_token, reply)
            return

        except Exception as e:
            # 避免 webhook 失敗導致 LINE 重送
            reply_text(event.reply_token, f"系統剛剛忙碌了一下（已收到訊息）。\n請再傳一次下單內容或輸入「甜點」。\n\n錯誤：{e}")
            return

    # 4) 其他：給引導
    reply_text(
        event.reply_token,
        "你可以輸入：\n- 「甜點」看菜單與下單格式\n- 直接貼上下單格式（包含取貨方式/日期）即可建立訂單\n- 轉帳後回傳：「已轉帳 訂單編號 末五碼12345」",
    )
