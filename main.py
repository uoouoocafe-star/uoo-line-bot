import os
import json
import base64
import uuid
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qs

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse

# LINE Bot SDK v3
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

# Google Sheets API (googleapiclient)
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# -----------------------------
# ENV (Render Environment Variables)
# -----------------------------
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "").strip()
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "").strip()

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GSHEET_TAB = os.getenv("GSHEET_TAB", "orders").strip()  # 你的工作表分頁名稱，建議 orders
GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "").strip()

# 你若還留著 GOOGLE_SERVICE_ACCOUNT_JSON，建議刪掉，避免格式錯誤造成混亂
# GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()


# -----------------------------
# App
# -----------------------------
app = FastAPI()


@app.get("/", response_class=PlainTextResponse)
def health():
    return "OK"


def _require_env():
    missing = []
    if not CHANNEL_ACCESS_TOKEN:
        missing.append("CHANNEL_ACCESS_TOKEN")
    if not CHANNEL_SECRET:
        missing.append("CHANNEL_SECRET")
    if missing:
        raise HTTPException(status_code=500, detail=f"Missing env: {', '.join(missing)}")


# -----------------------------
# LINE setup
# -----------------------------
parser = WebhookParser(CHANNEL_SECRET) if CHANNEL_SECRET else None


def _line_api() -> MessagingApi:
    config = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
    api_client = ApiClient(config)
    return MessagingApi(api_client)


# -----------------------------
# Google Sheets helpers
# -----------------------------
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
    # cache_discovery=False 避免某些環境快取問題
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def append_order_row(
    *,
    user_id: str,
    display_name: str,
    items_json: dict,
    pickup_method: str = "",
    pickup_date: str = "",
    pickup_time: str = "",
    note: str = "",
    amount: str = "",
    pay_status: str = "unpaid",
    linepay_transaction_id: str = "",
):
    """
    依你的欄位順序寫入一列：
    created_at, user_id, display_name, order_id, items_json,
    pickup_method, pickup_date, pickup_time, note, amount,
    pay_status, linepay_transaction_id
    """
    tz = timezone(timedelta(hours=8))
    created_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    order_id = str(uuid.uuid4())[:8]

    row = [
        created_at,
        user_id,
        display_name,
        order_id,
        json.dumps(items_json, ensure_ascii=False),
        pickup_method,
        pickup_date,
        pickup_time,
        note,
        amount,
        pay_status,
        linepay_transaction_id,
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
    print(
        f"[OK] append_order_row success. updatedRange={updates.get('updatedRange')} rows={updates.get('updatedRows')}"
    )


# -----------------------------
# Flex Menu
# -----------------------------
def build_main_menu_flex() -> FlexMessage:
    # 你之後可以換成自己的品牌圖 URL（建議 https）
    hero_image_url = "https://images.unsplash.com/photo-1511920170033-f8396924c348?w=1200"

    flex_json = {
        "type": "bubble",
        "hero": {
            "type": "image",
            "url": hero_image_url,
            "size": "full",
            "aspectRatio": "20:13",
            "aspectMode": "cover",
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "UooUoo 點餐中心", "weight": "bold", "size": "xl"},
                {"type": "text", "text": "請選擇你要的功能", "size": "sm", "color": "#666666", "wrap": True},
                {
                    "type": "box",
                    "layout": "vertical",
                    "spacing": "sm",
                    "margin": "lg",
                    "contents": [
                        {
                            "type": "button",
                            "style": "primary",
                            "action": {"type": "postback", "label": "🍞 店內菜單", "data": "action=menu_instore"},
                        },
                        {
                            "type": "button",
                            "style": "primary",
                            "action": {"type": "postback", "label": "🍰 甜點訂單", "data": "action=order_dessert"},
                        },
                        {
                            "type": "button",
                            "style": "secondary",
                            "action": {
                                "type": "postback",
                                "label": "📅 下單日程 / 取貨方式",
                                "data": "action=schedule_pickup",
                            },
                        },
                        {
                            "type": "button",
                            "style": "secondary",
                            "action": {"type": "postback", "label": "💳 LINE Pay 結帳", "data": "action=linepay_checkout"},
                        },
                    ],
                },
            ],
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": [
                {"type": "text", "text": "需要真人協助：回覆『客服』", "size": "xs", "color": "#999999", "wrap": True}
            ],
        },
    }

    return FlexMessage(
        alt_text="UooUoo 點餐中心",
        contents=FlexContainer.from_dict(flex_json),
    )


def reply_messages(reply_token: str, messages):
    api = _line_api()
    api.reply_message(
        ReplyMessageRequest(
            reply_token=reply_token,
            messages=messages,
        )
    )


def handle_postback(reply_token: str, user_id: str, display_name: str, postback_data: str):
    qs = parse_qs(postback_data)
    action = (qs.get("action", [""])[0] or "").strip()

    # 先回覆（A階段先用文字回覆，B階段再改成流程）
    if action == "menu_instore":
        msg = TextMessage(text="🍞 店內菜單：\n（下一步我幫你接：菜單圖 / 連結 / 分類）")
    elif action == "order_dessert":
        msg = TextMessage(text="🍰 甜點訂單：\n已進入下單入口（下一步我們做 B：選品項/數量/取貨）。")
    elif action == "schedule_pickup":
        msg = TextMessage(text="📅 下單日程/取貨方式：\n目前先提供：自取 / 宅配（下一步做成可點選）。")
    elif action == "linepay_checkout":
        msg = TextMessage(text="💳 LINE Pay：\n等 B 訂單流程完成後再串付款，會最穩。")
    else:
        msg = TextMessage(text="我沒有讀到 action，請回覆『menu』重新開啟選單。")

    # 同步寫入 sheet（紀錄使用者點了什麼）
    try:
        append_order_row(
            user_id=user_id,
            display_name=display_name,
            items_json={"event": "postback", "action": action, "raw": postback_data},
            note="postback",
            pay_status="unpaid",
        )
    except Exception as e:
        print("[WARN] sheet append (postback) failed:", repr(e))

    reply_messages(reply_token, [msg])


# -----------------------------
# Webhook endpoint
# -----------------------------
@app.post("/callback")
async def callback(request: Request):
    _require_env()
    if not parser:
        raise HTTPException(status_code=500, detail="CHANNEL_SECRET not set")

    signature = request.headers.get("X-Line-Signature", "")
    body_bytes = await request.body()
    body_text = body_bytes.decode("utf-8")

    print("=== callback hit ===")
    print("signature:", signature[:10] + "..." if signature else "(missing)")
    # print("raw body:", body_text)  # 若太長可註解

    try:
        events = parser.parse(body_text, signature)
    except Exception as e:
        print("[ERROR] parse webhook failed:", repr(e))
        raise HTTPException(status_code=400, detail="Bad Request")

    for event in events:
        # 文字訊息
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessageContent):
            user_id = getattr(event.source, "user_id", "") or ""
            text = (event.message.text or "").strip()
            reply_token = event.reply_token

            # 嘗試抓 display name（抓不到也不影響）
            display_name = ""
            try:
                api = _line_api()
                profile = api.get_profile(user_id)
                display_name = getattr(profile, "display_name", "") or ""
            except Exception as e:
                print("[WARN] get_profile failed:", repr(e))

            # 打 menu / 主選單 → 回 Flex
            lowered = text.lower()
            if lowered in ["menu", "主選單", "選單", "開始", "點餐"]:
                flex = build_main_menu_flex()
                reply_messages(reply_token, [flex])

                # 記錄：使用者開啟主選單
                try:
                    append_order_row(
                        user_id=user_id,
                        display_name=display_name,
                        items_json={"event": "text", "text": text, "intent": "open_menu"},
                        note="open_menu",
                        pay_status="unpaid",
                    )
                except Exception as e:
                    print("[WARN] sheet append (open_menu) failed:", repr(e))

                continue

            # 客服
            if text == "客服":
                reply_messages(reply_token, [TextMessage(text="好的，我已通知客服（你也可以直接留言需求）。")])
                try:
                    append_order_row(
                        user_id=user_id,
                        display_name=display_name,
                        items_json={"event": "text", "text": text, "intent": "support"},
                        note="support",
                        pay_status="unpaid",
                    )
                except Exception as e:
                    print("[WARN] sheet append (support) failed:", repr(e))
                continue

            # 其他訊息：先回覆並寫入 sheet（維持你現在成功的寫入能力）
            reply_messages(reply_token, [TextMessage(text=f"收到：{text}\n回覆『menu』可開啟點餐選單。")])

            try:
                append_order_row(
                    user_id=user_id,
                    display_name=display_name,
                    items_json={"event": "text", "text": text},
                    note=text,
                    pay_status="unpaid",
                )
            except Exception as e:
                print("[ERROR] append_order_row failed:", repr(e))

        # postback（點 Flex 按鈕）
        elif isinstance(event, PostbackEvent):
            user_id = getattr(event.source, "user_id", "") or ""
            reply_token = event.reply_token
            postback_data = event.postback.data if event.postback else ""

            display_name = ""
            try:
                api = _line_api()
                profile = api.get_profile(user_id)
                display_name = getattr(profile, "display_name", "") or ""
            except Exception as e:
                print("[WARN] get_profile failed:", repr(e))

            handle_postback(reply_token, user_id, display_name, postback_data)

    return "OK"
