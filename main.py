import os
import json
import base64
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException

from linebot.v3.webhook import WebhookHandler
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
)

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# -----------------------------
# Env
# -----------------------------
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "").strip()
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "").strip()

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GSHEET_TAB = os.getenv("GSHEET_TAB", "orders").strip()

GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "").strip()
# 強烈建議不要用 GOOGLE_SERVICE_ACCOUNT_JSON（避免格式錯誤）
# 如果你真的要用，也必須是完整 JSON 字串且單行/正確 escaping
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

app = FastAPI()

if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    print("[WARN] LINE env missing: CHANNEL_ACCESS_TOKEN or CHANNEL_SECRET is empty.")

handler = WebhookHandler(CHANNEL_SECRET)

line_config = Configuration(access_token=CHANNEL_ACCESS_TOKEN)

# -----------------------------
# Google Sheets helpers
# -----------------------------
def _load_service_account_info() -> dict:
    """
    Prefer GOOGLE_SERVICE_ACCOUNT_B64.
    If GOOGLE_SERVICE_ACCOUNT_JSON exists, it must be valid JSON string.
    """
    if GOOGLE_SERVICE_ACCOUNT_B64:
        try:
            raw = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64).decode("utf-8")
            return json.loads(raw)
        except Exception as e:
            raise RuntimeError(f"Invalid GOOGLE_SERVICE_ACCOUNT_B64: {e}")

    if GOOGLE_SERVICE_ACCOUNT_JSON:
        try:
            return json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        except Exception as e:
            raise RuntimeError(f"Invalid GOOGLE_SERVICE_ACCOUNT_JSON: {e}")

    raise RuntimeError("Missing Google service account env (GOOGLE_SERVICE_ACCOUNT_B64 / JSON).")


def append_order_row(row: list[str]) -> None:
    if not GSHEET_ID:
        raise RuntimeError("Missing GSHEET_ID env.")
    info = _load_service_account_info()

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    service = build("sheets", "v4", credentials=creds)

    # A欄開始寫入
    range_name = f"{GSHEET_TAB}!A:A"
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
# Routes
# -----------------------------
@app.get("/")
def health():
    return {"ok": True}


@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body = (await request.body()).decode("utf-8")

    # 可保留/可刪：debug 用
    print("=== callback hit ===")
    print("signature:", signature[:10] + "..." if signature else "(missing)")
    # print("raw body:", body)  # 太長可先註解

    try:
        handler.handle(body, signature)
    except Exception as e:
        print("[ERROR] LINE handler error:", repr(e))
        raise HTTPException(status_code=400, detail="Invalid signature or handler error")

    return "OK"


# -----------------------------
# LINE Event handler
# -----------------------------
@handler.add(MessageEvent, message=TextMessageContent)
def on_message(event: MessageEvent):
    user_id = event.source.user_id if event.source else ""
    text = event.message.text if event.message else ""
    display_name = ""  # 若要抓 displayName 需額外用 Profile API

    order_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    # 你目前 sheet 欄位（照你截圖）
    # created_at, user_id, display_name, order_id, items_json, pickup_method, pickup_date, pickup_time, note, amount, pay_status, linepay_transaction_id
    items_json = json.dumps({"text": text}, ensure_ascii=False)

    row = [
        created_at,
        user_id,
        display_name,
        order_id,
        items_json,
        "",  # pickup_method
        "",  # pickup_date
        "",  # pickup_time
        "",  # note
        "",  # amount
        "unpaid",  # pay_status
        "",  # linepay_transaction_id
    ]

    # 先寫 Google Sheet（失敗也要讓 LINE 還能回覆）
    try:
        append_order_row(row)
    except Exception as e:
        print("[ERROR] append_order_row failed:", repr(e))

    # 回覆 LINE
    try:
        with ApiClient(line_config) as api_client:
            api = MessagingApi(api_client)
            api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text=f"收到：{text}")],
                )
            )
    except Exception as e:
        print("[ERROR] reply_message failed:", repr(e))
