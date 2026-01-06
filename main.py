import base64
import os
import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    ApiClient,
    Configuration,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

import gspread
from google.oauth2.service_account import Credentials

app = FastAPI()

# ---- ENV ----
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "")
GSHEET_ID = os.getenv("GSHEET_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    # 讓服務仍可啟動，但 webhook 會失敗；方便看 logs
    print("WARNING: Missing CHANNEL_ACCESS_TOKEN or CHANNEL_SECRET")

handler = WebhookHandler(CHANNEL_SECRET)

TZ_TAIPEI = timezone(timedelta(hours=8))


def _get_gspread_client():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")
    info = _get_service_account_info()
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def append_order_row(
    *,
    user_id: str,
    display_name: str,
    text: str,
):
    if not GSHEET_ID:
        raise RuntimeError("Missing GSHEET_ID")

    gc = _get_gspread_client()
    sh = gc.open_by_key(GSHEET_ID)
    ws = sh.sheet1

    created_at = datetime.now(TZ_TAIPEI).isoformat(timespec="seconds")
    order_id = str(uuid.uuid4())

    # 先用最簡單的 items_json：把使用者輸入存成一個 item
    items_json = json.dumps(
        [{"name": "text_message", "qty": 1, "note": text}],
        ensure_ascii=False,
    )

    row = [
        created_at,                 # created_at
        user_id,                    # user_id
        display_name,               # display_name
        order_id,                   # order_id
        items_json,                 # items_json
        "",                         # pickup_method
        "",                         # pickup_date
        "",                         # pickup_time
        "",                         # note
        0,                          # amount
        "UNPAID",                   # pay_status
        "",                         # linepay_transaction_id
    ]

    ws.append_row(row, value_input_option="RAW")


def get_display_name(user_id: str) -> str:
    # 取 profile 可能會因權限/設定失敗，所以做容錯
    try:
        configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
        with ApiClient(configuration) as api_client:
            messaging_api = MessagingApi(api_client)
            profile = messaging_api.get_profile(user_id)
            return getattr(profile, "display_name", "") or ""
    except Exception as e:
        print(f"[WARN] get_profile failed: {e}")
        return ""


@app.get("/")
def health():
    return {"ok": True}


from fastapi import Request

@app.post("/callback")
async def callback(request: Request):
    body = await request.body()
    body_text = body.decode("utf-8")
    print("=== callback hit ===")
    print("raw body:", body_text)
    ...

async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    body_text = body.decode("utf-8")

    try:
        handler.handle(body_text, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    return "OK"


@handler.add(MessageEvent, message=TextMessageContent)
def handle_text_message(event: MessageEvent):
    user_id = event.source.user_id if event.source else ""
    text = event.message.text if event.message else ""

    display_name = get_display_name(user_id) if user_id else ""

GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "")

def _get_service_account_info():
    if GOOGLE_SERVICE_ACCOUNT_B64:
        raw = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64).decode("utf-8")
        return json.loads(raw)
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        return json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_SERVICE_ACCOUNT_B64")
    
    # ---- Write to Google Sheet ----
    try:
        append_order_row(user_id=user_id, display_name=display_name, text=text)
        sheet_status = "已寫入訂單表"
    except Exception as e:
        print(f"[ERROR] append_order_row failed: {e}")
        sheet_status = f"寫入失敗：{e}"

    # ---- Reply ----
    reply_text = f"收到：{text}\n{sheet_status}"
    configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)

    with ApiClient(configuration) as api_client:
        messaging_api = MessagingApi(api_client)
        messaging_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply_text)],
            )
        )
