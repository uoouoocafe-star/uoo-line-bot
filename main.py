
import os
import json
import base64
import uuid
from datetime import datetime, timezone, timedelta, date
from urllib.parse import parse_qs

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse

from linebot.v3.webhook import WebhookParser
from linebot.v3.webhooks import MessageEvent, TextMessageContent, PostbackEvent
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
    FlexMessage,
    FlexContainer,
)

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# -----------------------------
# ENV (Render Environment Variables)
# -----------------------------
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "").strip()
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "").strip()

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GSHEET_TAB = os.getenv("GSHEET_TAB", "orders").strip()  # Google sheet 分頁名稱
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "").strip()

# Business rules
LEAD_DAYS = 3
DELIVERY_FEE = 180
FREE_SHIP_THRESHOLD = 2500

TZ_TAIPEI = timezone(timedelta(hours=8))

app = FastAPI()


# -----------------------------
# In-memory user sessions (Render 重啟會清空；先做到能收單即可)
# -----------------------------
USER_SESSIONS = {}  # user_id -> dict


def session_get(user_id: str) -> dict:
    s = USER_SESSIONS.get(user_id)
    if not s:
        s = {"state": "idle", "cart": [], "pickup_method": "", "pickup_date": "", "note": ""}
        USER_SESSIONS[user_id] = s
    return s


def session_reset(user_id: str):
    USER_SESSIONS[user_id] = {"state": "idle", "cart": [], "pickup_method": "", "pickup_date": "", "note": ""}


# -----------------------------
# Product catalog
# -----------------------------
DACQ_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]
TOAST_FLAVORS = ["原味", "蜜香紅茶", "日式抹茶", "日式焙茶", "法芙娜可可"]

PRODUCTS = {
    "dacq": {"name": "達克瓦茲", "unit_price": 95},
    "scone": {"name": "原味司康", "unit_price": 65},
    "canele": {"name": "原味可麗露", "unit_price": 90},
    "toast": {"name": "伊思尼奶酥厚片", "unit_price": 85},
}


# -----------------------------
# Utils
# -----------------------------
def taipei_today() -> date:
    return datetime.now(TZ_TAIPEI).date()


def min_pickup_date_str() -> str:
    d = taipei_today() + timedelta(days=LEAD_DAYS)
    return d.strftime("%Y-%m-%d")


def allowed_pickup_dates(days_ahead: int = 21):
    start = taipei_today() + timedelta(days=LEAD_DAYS)
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days_ahead)]


def money(n: int) -> str:
    return f"{n:,}"


def calc_subtotal(cart: list) -> int:
    return sum(int(item["subtotal"]) for item in cart)


def calc_shipping(subtotal: int, pickup_method: str) -> int:
    if pickup_method != "delivery":
        return 0
    return 0 if subtotal >= FREE_SHIP_THRESHOLD else DELIVERY_FEE


def build_order_summary_text(s: dict) -> str:
    lines = ["🧾 訂單摘要"]
    if not s["cart"]:
        lines.append("（目前購物車是空的）")
    else:
        for item in s["cart"]:
            if item.get("flavor"):
                lines.append(f"- {item['name']}（{item['flavor']}）x {item['qty']} ＝ {money(item['subtotal'])}")
            else:
                lines.append(f"- {item['name']} x {item['qty']} ＝ {money(item['subtotal'])}")

    subtotal = calc_subtotal(s["cart"])
    shipping = calc_shipping(subtotal, s.get("pickup_method", ""))
    total = subtotal + shipping

    lines.append(f"\n小計：{money(subtotal)}")
    if s.get("pickup_method") == "delivery":
        lines.append(f"運費：{money(shipping)}（滿{money(FREE_SHIP_THRESHOLD)}免運）")
    lines.append(f"合計：{money(total)}")

    if s.get("pickup_method"):
        pm = "宅配（大榮）" if s["pickup_method"] == "delivery" else "店取"
        lines.append(f"\n取貨方式：{pm}")

    if s.get("pickup_date"):
        lines.append(f"取貨/出貨日期：{s['pickup_date']}（至少提前{LEAD_DAYS}天）")

    lines.append("\n回覆『取消』可清空本次下單。")
    return "\n".join(lines)


# -----------------------------
# LINE / Google Sheets setup
# -----------------------------
def _require_env():
    missing = []
    if not CHANNEL_ACCESS_TOKEN:
        missing.append("CHANNEL_ACCESS_TOKEN")
    if not CHANNEL_SECRET:
        missing.append("CHANNEL_SECRET")
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env: {', '.join(missing)}")


parser = WebhookParser(CHANNEL_SECRET) if CHANNEL_SECRET else None


def _line_api() -> MessagingApi:
    config = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
    api_client = ApiClient(config)
    return MessagingApi(api_client)


def _get_sheets_service():
    if not GOOGLE_SERVICE_ACCOUNT_B64:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_B64")
    if not GSHEET_ID:
        raise RuntimeError("Missing GSHEET_ID")

    sa_json_bytes = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64)
    sa_info = json.loads(sa_json_bytes.decode("utf-8"))

    creds = Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def append_order_row(
    *,
    user_id: str,
    display_name: str,
    order_payload: dict,
    pickup_method: str,
    pickup_date: str,
    amount: int,
    pay_status: str = "pending",
):
    """
    created_at, user_id, display_name, order_id, items_json,
    pickup_method, pickup_date, pickup_time, note, amount,
    pay_status, linepay_transaction_id
    """
    created_at = datetime.now(TZ_TAIPEI).strftime("%Y-%m-%d %H:%M:%S")
    order_id = str(uuid.uuid4())[:8]

    row = [
        created_at,
        user_id,
        display_name,
        order_id,
        json.dumps(order_payload, ensure_ascii=False),
        pickup_method,            # pickup_method
        pickup_date,              # pickup_date
        "",                       # pickup_time (先保留)
        "",                       # note
        str(amount),              # amount
        pay_status,               # pay_status
        "",                       # linepay_transaction_id
    ]

    service = _get_sheets_service()
    range_name = f"{GSHEET_TAB}!A:L"
    body = {"values": [row]}

    resp = (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=GSHEET_ID,
            range=range_name,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        )
        .execute()
    )
    updates = resp.get("updates", {})
    print(f"[OK] append_order_row success. updatedRange={updates.get('updatedRange')} rows={updates.get('updatedRows')}")


# -----------------------------
# Flex builders
# -----------------------------
def flex_bubble(title: str, subtitle: str, buttons: list, hero_url: str = "") -> FlexMessage:
    hero = None
    if hero_url:
        hero = {
            "type": "image",
            "url": hero_url,
            "size": "full",
            "aspectRatio": "20:13",
            "aspectMode": "cover",
        }

    bubble = {
        "type": "bubble",
        **({"hero": hero} if hero else {}),
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": title, "weight": "bold", "size": "xl", "wrap": True},
                {"type": "text", "text": subtitle, "size": "sm", "color": "#666666", "wrap": True},
                {
                    "type": "box",
                    "layout": "vertical",
                    "spacing": "sm",
                    "margin": "lg",
                    "contents": buttons,
                },
            ],
        },
    }

    return FlexMessage(alt_text=title, contents=FlexContainer.from_dict(bubble))


def build_main_menu_flex() -> FlexMessage:
    buttons = [
        {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🍞 店內菜單", "data": "action=menu_instore"}},
        {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🍰 甜點訂單", "data": "action=order_start"}},
        {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "📅 下單規則 / 取貨方式", "data": "action=rules"}},
        {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "🧾 查看本次訂單", "data": "action=order_summary"}},
    ]
    return flex_bubble(
        title="UooUoo 點餐中心",
        subtitle="請選擇你要的功能",
        buttons=buttons,
        hero_url="https://images.unsplash.com/photo-1511920170033-f8396924c348?w=1200",
    )


def build_product_picker_flex() -> FlexMessage:
    buttons = [
        {"type": "button", "style": "primary", "action": {"type": "postback", "label": "達克瓦茲（95/顆）", "data": "action=pick_product&pid=dacq"}},
        {"type": "button", "style": "primary", "action": {"type": "postback", "label": "原味司康（65/顆）", "data": "action=pick_product&pid=scone"}},
        {"type": "button", "style": "primary", "action": {"type": "postback", "label": "原味可麗露（90/顆）", "data": "action=pick_product&pid=canele"}},
        {"type": "button", "style": "primary", "action": {"type": "postback", "label": "伊思尼奶酥厚片（85/片）", "data": "action=pick_product&pid=toast"}},
        {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "查看本次訂單", "data": "action=order_summary"}},
    ]
    return flex_bubble(
        title="🍰 甜點訂單",
        subtitle=f"全部甜點需提前{LEAD_DAYS}天預訂（最早可選：{min_pickup_date_str()}）",
        buttons=buttons,
    )


def build_flavor_picker_flex(pid: str) -> FlexMessage:
    if pid == "dacq":
        flavors = DACQ_FLAVORS
        title = "選擇達克瓦茲口味（不可混）"
        subtitle = "一次下單只能選一種口味"
    else:
        flavors = TOAST_FLAVORS
        title = "選擇奶酥厚片口味"
        subtitle = "請選擇一種口味"

    buttons = []
    for f in flavors:
        buttons.append(
            {"type": "button", "style": "primary", "action": {"type": "postback", "label": f, "data": f"action=pick_flavor&pid={pid}&flavor={f}"}}
        )
    buttons.append({"type": "button", "style": "secondary", "action": {"type": "postback", "label": "返回甜點列表", "data": "action=order_start"}})

    return flex_bubble(title=title, subtitle=subtitle, buttons=buttons)


def build_qty_picker_flex(pid: str, flavor: str = "") -> FlexMessage:
    p = PRODUCTS[pid]
    name = p["name"]
    unit = p["unit_price"]

    buttons = []
    if pid == "dacq":
        # min 2, even only
        options = [2, 4, 6, 8, 10, 12]
    else:
        options = [1, 2, 3, 4, 5, 6, 8, 10]

    for q in options:
        label = f"{name} x {q}（{money(unit*q)}）"
        data = f"action=pick_qty&pid={pid}&qty={q}"
        if flavor:
            data += f"&flavor={flavor}"
        buttons.append({"type": "button", "style": "primary", "action": {"type": "postback", "label": label, "data": data}})

    buttons.append({"type": "button", "style": "secondary", "action": {"type": "postback", "label": "返回甜點列表", "data": "action=order_start"}})

    title = "選擇數量"
    subtitle = "達克瓦茲最低2顆且只能偶數；其餘甜點可自由選擇"
    return flex_bubble(title=title, subtitle=subtitle, buttons=buttons)


def build_pickup_method_flex() -> FlexMessage:
    buttons = [
        {"type": "button", "style": "primary", "action": {"type": "postback", "label": "🏠 店取", "data": "action=pickup_method&method=pickup"}},
        {"type": "button", "style": "primary", "action": {"type": "postback", "label": f"📦 宅配（大榮 {money(DELIVERY_FEE)}；滿{money(FREE_SHIP_THRESHOLD)}免運）", "data": "action=pickup_method&method=delivery"}},
        {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "查看本次訂單", "data": "action=order_summary"}},
    ]
    return flex_bubble(title="選擇取貨方式", subtitle="宅配運費規則會自動計算", buttons=buttons)


def build_pickup_date_flex() -> FlexMessage:
    dates = allowed_pickup_dates(14)
    buttons = []
    for d in dates:
        buttons.append({"type": "button", "style": "primary", "action": {"type": "postback", "label": d, "data": f"action=pickup_date&date={d}"}})
    buttons.append({"type": "button", "style": "secondary", "action": {"type": "postback", "label": "返回取貨方式", "data": "action=pickup_method_back"}})

    return flex_bubble(
        title="選擇取貨/出貨日期",
        subtitle=f"依規則：全部甜點需至少提前{LEAD_DAYS}天（最早：{min_pickup_date_str()}）",
        buttons=buttons,
    )


def build_confirm_flex(summary_text: str) -> FlexMessage:
    # 用 Flex 顯示摘要 + 兩顆按鈕
    buttons = [
        {"type": "button", "style": "primary", "action": {"type": "postback", "label": "✅ 確認下單（寫入表單）", "data": "action=confirm_order"}},
        {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "➕ 繼續加購", "data": "action=order_start"}},
        {"type": "button", "style": "secondary", "action": {"type": "postback", "label": "🧾 再看一次摘要", "data": "action=order_summary"}},
    ]

    bubble = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "請確認訂單", "weight": "bold", "size": "xl"},
                {"type": "text", "text": summary_text, "wrap": True, "size": "sm", "color": "#333333"},
            ],
        },
        "footer": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": buttons},
    }

    return FlexMessage(alt_text="請確認訂單", contents=FlexContainer.from_dict(bubble))


# -----------------------------
# Reply helper
# -----------------------------
def reply_messages(reply_token: str, messages):
    api = _line_api()
    api.reply_message(ReplyMessageRequest(reply_token=reply_token, messages=messages))


# -----------------------------
# Postback handler
# -----------------------------
def handle_postback(reply_token: str, user_id: str, display_name: str, postback_data: str):
    s = session_get(user_id)
    qs = parse_qs(postback_data)
    action = (qs.get("action", [""])[0] or "").strip()

    # Main menu actions
    if action == "menu_instore":
        reply_messages(reply_token, [TextMessage(text="🍞 店內菜單：\n（你可以先回我：你要用『菜單圖片』還是『Google Drive 連結』，我幫你接到按鈕裡）")])
        return

    if action == "rules":
        msg = (
            f"📌 下單規則\n"
            f"- 全部甜點需至少提前{LEAD_DAYS}天預訂（最早日期：{min_pickup_date_str()}）\n"
            f"- 宅配：大榮貨運 運費{money(DELIVERY_FEE)}；滿{money(FREE_SHIP_THRESHOLD)}免運\n"
            f"- 達克瓦茲：口味不可混、最低2顆且只能偶數（2/4/6...）\n"
            f"\n回覆『menu』可開啟主選單。"
        )
        reply_messages(reply_token, [TextMessage(text=msg)])
        return

    if action == "order_summary":
        reply_messages(reply_token, [TextMessage(text=build_order_summary_text(s))])
        return

    # Start order flow
    if action == "order_start":
        # 不清空 cart，讓使用者可以加購
        s["state"] = "picking_product"
        reply_messages(reply_token, [build_product_picker_flex()])
        return

    # Pick product
    if action == "pick_product":
        pid = (qs.get("pid", [""])[0] or "").strip()
        if pid not in PRODUCTS:
            reply_messages(reply_token, [TextMessage(text="找不到這個品項，請回覆 menu 重新開始。")])
            return

        s["state"] = "picking_detail"
        s["current_pid"] = pid
        s.pop("current_flavor", None)

        if pid == "dacq":
            reply_messages(reply_token, [build_flavor_picker_flex("dacq")])
            return
        if pid == "toast":
            reply_messages(reply_token, [build_flavor_picker_flex("toast")])
            return

        # scone / canele: directly qty
        reply_messages(reply_token, [build_qty_picker_flex(pid)])
        return

    # Pick flavor
    if action == "pick_flavor":
        pid = (qs.get("pid", [""])[0] or "").strip()
        flavor = (qs.get("flavor", [""])[0] or "").strip()

        if pid == "dacq" and flavor not in DACQ_FLAVORS:
            reply_messages(reply_token, [TextMessage(text="口味不在清單內，請重新選擇。")])
            return
        if pid == "toast" and flavor not in TOAST_FLAVORS:
            reply_messages(reply_token, [TextMessage(text="口味不在清單內，請重新選擇。")])
            return

        s["current_pid"] = pid
        s["current_flavor"] = flavor

        reply_messages(reply_token, [build_qty_picker_flex(pid, flavor=flavor)])
        return

    # Pick qty -> add to cart
    if action == "pick_qty":
        pid = (qs.get("pid", [""])[0] or "").strip()
        qty_str = (qs.get("qty", ["0"])[0] or "0").strip()
        flavor = (qs.get("flavor", [""])[0] or "").strip()

        try:
            qty = int(qty_str)
        except ValueError:
            qty = 0

        if pid not in PRODUCTS or qty <= 0:
            reply_messages(reply_token, [TextMessage(text="數量或品項不正確，請重新選擇。")])
            return

        # enforce dacq rule: min2 even
        if pid == "dacq":
            if qty < 2 or qty % 2 != 0:
                reply_messages(reply_token, [TextMessage(text="達克瓦茲最低2顆且只能偶數（2/4/6...），請重新選擇。")])
                return
            if not flavor:
                reply_messages(reply_token, [TextMessage(text="達克瓦茲需要先選口味（不可混）。")])
                return

        # toast needs flavor
        if pid == "toast" and not flavor:
            reply_messages(reply_token, [TextMessage(text="奶酥厚片需要先選口味。")])
            return

        p = PRODUCTS[pid]
        subtotal = p["unit_price"] * qty

        s["cart"].append(
            {
                "pid": pid,
                "name": p["name"],
                "unit_price": p["unit_price"],
                "qty": qty,
                "flavor": flavor,
                "subtotal": subtotal,
            }
        )

        s["state"] = "picked_item"

        reply_messages(
            reply_token,
            [
                TextMessage(
                    text=f"✅ 已加入：{p['name']}{'（'+flavor+'）' if flavor else ''} x {qty}\n\n接下來請選取貨方式。"
                ),
                build_pickup_method_flex(),
            ],
        )
        return

    # pickup method
    if action == "pickup_method":
        method = (qs.get("method", [""])[0] or "").strip()
        if method not in ["pickup", "delivery"]:
            reply_messages(reply_token, [TextMessage(text="取貨方式不正確，請重新選擇。")])
            return

        s["pickup_method"] = method
        s["state"] = "picking_date"
        reply_messages(reply_token, [build_pickup_date_flex()])
        return

    if action == "pickup_method_back":
        reply_messages(reply_token, [build_pickup_method_flex()])
        return

    # pickup date
    if action == "pickup_date":
        d = (qs.get("date", [""])[0] or "").strip()
        if d not in allowed_pickup_dates(60):
            reply_messages(reply_token, [TextMessage(text=f"日期不符合規則（需至少提前{LEAD_DAYS}天），請重新選擇。")])
            return

        s["pickup_date"] = d
        s["state"] = "confirming"

        summary = build_order_summary_text(s)
        reply_messages(reply_token, [build_confirm_flex(summary)])
        return

    # confirm
    if action == "confirm_order":
        if not s["cart"]:
            reply_messages(reply_token, [TextMessage(text="你的購物車是空的，請先選擇甜點。")])
            return
        if not s.get("pickup_method") or not s.get("pickup_date"):
            reply_messages(reply_token, [TextMessage(text="請先完成取貨方式與日期選擇。回覆『menu』重新開始。")])
            return

        subtotal = calc_subtotal(s["cart"])
        shipping = calc_shipping(subtotal, s["pickup_method"])
        total = subtotal + shipping

        order_payload = {
            "items": s["cart"],
            "rules": {
                "lead_days": LEAD_DAYS,
                "delivery_fee": DELIVERY_FEE,
                "free_ship_threshold": FREE_SHIP_THRESHOLD,
                "dacquoise_no_mix": True,
            },
            "pickup_method": s["pickup_method"],
            "pickup_date": s["pickup_date"],
            "subtotal": subtotal,
            "shipping": shipping,
            "total": total,
        }

        try:
            append_order_row(
                user_id=user_id,
                display_name=display_name,
                order_payload=order_payload,
                pickup_method=s["pickup_method"],
                pickup_date=s["pickup_date"],
                amount=total,
                pay_status="pending",
            )
            pm = "宅配（大榮）" if s["pickup_method"] == "delivery" else "店取"
            reply_messages(
                reply_token,
                [
                    TextMessage(
                        text=(
                            "✅ 已成立訂單（已寫入表單）\n\n"
                            f"{build_order_summary_text(s)}\n\n"
                            f"下一步：我可以幫你接 LINE Pay，付款成功後自動把 pay_status 變成 paid。\n"
                            f"目前取貨方式：{pm}"
                        )
                    )
                ],
            )
        except Exception as e:
            reply_messages(reply_token, [TextMessage(text=f"訂單寫入失敗：{e}\n請稍後再試或回覆『客服』。")])
            return

        # 成功後清空 session（避免重複送）
        session_reset(user_id)
        return

    # unknown
    reply_messages(reply_token, [TextMessage(text="我沒有讀到正確指令，回覆『menu』開啟選單。")])


# -----------------------------
# Webhook endpoint
# -----------------------------
@app.get("/", response_class=PlainTextResponse)
def root():
    return "OK"


@app.post("/callback")
async def callback(request: Request):
    _require_env()
    if not parser:
        raise HTTPException(status_code=500, detail="CHANNEL_SECRET not set")

    signature = request.headers.get("X-Line-Signature", "")
    body_bytes = await request.body()
    body_text = body_bytes.decode("utf-8")

    try:
        events = parser.parse(body_text, signature)
    except Exception as e:
        print("[ERROR] parse webhook failed:", repr(e))
        raise HTTPException(status_code=400, detail="Bad Request")

    for event in events:
        # Text message
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessageContent):
            user_id = getattr(event.source, "user_id", "") or ""
            text = (event.message.text or "").strip()
            reply_token = event.reply_token

            # display name (best-effort)
            display_name = ""
            try:
                api = _line_api()
                profile = api.get_profile(user_id)
                display_name = getattr(profile, "display_name", "") or ""
            except Exception as e:
                print("[WARN] get_profile failed:", repr(e))

            # cancel
            if text in ["取消", "清空", "重來", "reset"]:
                session_reset(user_id)
                reply_messages(reply_token, [TextMessage(text="已清空本次下單。回覆『menu』重新開始。")])
                continue

            # main menu
            if text.lower() in ["menu", "主選單", "選單", "開始", "點餐"]:
                reply_messages(reply_token, [build_main_menu_flex()])
                continue

            # support
            if text == "客服":
                reply_messages(reply_token, [TextMessage(text="好的，請直接留言你的需求（我這邊會協助處理）。")])
                continue

            # default
            reply_messages(reply_token, [TextMessage(text="收到～回覆『menu』可開啟點餐選單；回覆『取消』可清空本次下單。")])

        # Postback
        elif isinstance(event, PostbackEvent):
            user_id = getattr(event.source, "user_id", "") or ""
            reply_token = event.reply_token
            postback_data = event.postback.data if event.postback else ""

            # display name (best-effort)
            display_name = ""
            try:
                api = _line_api()
                profile = api.get_profile(user_id)
                display_name = getattr(profile, "display_name", "") or ""
            except Exception as e:
                print("[WARN] get_profile failed:", repr(e))

            handle_postback(reply_token, user_id, display_name, postback_data)

    return "OK"
