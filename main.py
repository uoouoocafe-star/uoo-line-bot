import os
import json
import base64
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse

# LINE Bot SDK v3
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
)

# Google Sheets
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ---------------------------
# Environment Variables
# ---------------------------
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "").strip()
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "").strip()

GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Sheet1").strip()  # 你的工作表分頁名稱


# ---------------------------
# App
# ---------------------------
app = FastAPI()


@app.get("/", response_class=PlainTextResponse)
def health():
    return "OK"


# ---------------------------
# LINE setup
# ---------------------------
if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    # 服務仍可啟動，但 webhook 會回 500，log 會提示你缺 env
    print("[WARN] LINE env missing: CHANNEL_ACCESS_TOKEN or CHANNEL_SECRET")

handler = WebhookHandler(CHANNEL_SECRET)

line_config = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
line_api_client = ApiClient(line_config)
messaging_api = MessagingApi(line_api_client)


# ---------------------------
# Google Sheets helper
# ---------------------------
def _get_sheets_service():
    if not GOOGLE_SERVICE_ACCOUNT_B64:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_B64")
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("Missing GOOGLE_SHEET_ID")

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
    note: str,
    items_json: Optional[dict] = None,
    pickup_method: str = "",
    pickup_date: str = "",
    pickup_time: str = "",
    amount: str = "",
    pay_status: str = "UNPAID",
    linepay_transaction_id: str = "",
):
    """
    依你 sheet 欄位順序 append：
    created_at, user_id, display_name, order_id, items_json,
    pickup_method, pickup_date, pickup_time, note, amount,
    pay_status, linepay_transaction_id
    """
    tz = timezone(timedelta(hours=8))  # GMT+8
    created_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    order_id = str(uuid.uuid4())[:8]

    values = [[
        created_at,
        user_id,
        display_name,
        order_id,
        json.dumps(items_json or {}, ensure_ascii=False),
        pickup_method,
        pickup_date,
        pickup_time,
        note,
        amount,
        pay_status,
        linepay_transaction_id,
    ]]

    try:
        service = _get_sheets_service()
        service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=f"{GOOGLE_SHEET_NAME}!A:Z",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()
        return True
    except HttpError as e:
        # Google API 回傳的錯誤會在這裡
        print("[ERROR] append_order_row HttpError:", str(e))
        return False
    except Exception as e:
        print("[ERROR] append_order_row failed:", str(e))
        return False


# ---------------------------
# LINE message handler
# ---------------------------
@handler.add(MessageEvent, message=TextMessageContent)
def handle_text_message(event: MessageEvent):
    user_id = event.source.user_id if event.source else ""
    reply_token = event.reply_token
    text = event.message.text if event.message else ""

    # 取 display name（可能失敗，失敗就用空字串）
    display_name = ""
    try:
        profile = messaging_api.get_profile(user_id)
        display_name = getattr(profile, "display_name", "") or ""
    except Exception as e:
        print("[WARN] get_profile failed:", str(e))

    # 先把訊息寫入 Google Sheet（當作訂單/需求紀錄的第一版）
    wrote = True
    if GOOGLE_SERVICE_ACCOUNT_B64 and GOOGLE_SHEET_ID:
        wrote = append_order_row(
            user_id=user_id,
            display_name=display_name,
            note=text,
            items_json={"raw_text": text},
        )
    else:
        print("[WARN] Google Sheet env missing, skip append.")

    # 回覆訊息
    if wrote:
        reply_text = f"收到：{text}\n（已記錄）"
    else:
        reply_text = f"收到：{text}\n（但寫入表單失敗，請稍後再試）"

    try:
        messaging_api.reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[TextMessage(text=reply_text)],
            )
        )
    except Exception as e:
        print("[ERROR] reply_message failed:", str(e))


# ---------------------------
# Webhook endpoint
# ---------------------------
@app.post("/callback")
async def callback(request: Request):
    if not CHANNEL_SECRET or not CHANNEL_ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="LINE env missing")

    signature = request.headers.get("x-line-signature", "")
    body_bytes = await request.body()
    body_text = body_bytes.decode("utf-8")

    # Debug：看到 webhook 真的有打進來
    print("=== callback hit ===")
    print("signature:", signature[:8] + "..." if signature else "(none)")
    print("raw body:", body_text[:200] + ("..." if len(body_text) > 200 else ""))

    try:
        handler.handle(body_text, signature)
    except InvalidSignatureError:
        # LINE 會認為你沒有正確驗簽（通常是 Channel Secret 不對）
        raise HTTPException(status_code=400, detail="Invalid signature")
    except Exception as e:
        print("[ERROR] handler exception:", str(e))
        raise HTTPException(status_code=500, detail="Handler error")

    return "OK"
