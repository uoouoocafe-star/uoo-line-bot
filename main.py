import os, json, datetime
import requests
from fastapi import FastAPI, Request, Header, HTTPException
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

app = FastAPI()

# =============================
# ENV
# =============================
LINE_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
LINE_SECRET = os.getenv("LINE_CHANNEL_SECRET")

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
C_SHEET = "C"

GCAL_ID = os.getenv("GCAL_CALENDAR_ID")
TZ = "Asia/Taipei"

# =============================
# MENU（寫死，最穩）
# =============================
MENU = {
    "DACQ": {"name": "達克瓦茲", "price": 95},
    "SCONE": {"name": "原味司康", "price": 65},
    "CANELE": {"name": "可麗露6入盒", "price": 490},
    "TOAST": {"name": "奶酥厚片", "price": 85},
}

# =============================
# GOOGLE
# =============================
def get_google():
    info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/calendar"
        ]
    )
    sheets = build("sheets", "v4", credentials=creds)
    cal = build("calendar", "v3", credentials=creds)
    return sheets, cal

# =============================
# LINE HELPERS
# =============================
def line_reply(token, text):
    requests.post(
        "https://api.line.me/v2/bot/message/reply",
        headers={
            "Authorization": f"Bearer {LINE_TOKEN}",
            "Content-Type": "application/json"
        },
        json={
            "replyToken": token,
            "messages": [{"type": "text", "text": text}]
        }
    )

# =============================
# MAIN LOGIC
# =============================
@app.post("/callback")
async def callback(req: Request, x_line_signature: str = Header(None)):
    body = await req.json()
    for e in body.get("events", []):
        if e["type"] == "message":
            text = e["message"]["text"]
            token = e["replyToken"]
            user = e["source"]["userId"]

            if text == "甜點":
                msg = "\n".join([
                    "🍪 目前甜點",
                    "1️⃣ 達克瓦茲 95/顆",
                    "2️⃣ 原味司康 65/顆",
                    "3️⃣ 可麗露 6入 490",
                    "4️⃣ 奶酥厚片 85/片",
                    "",
                    "請直接回覆：",
                    "例如：達克瓦茲 4"
                ])
                line_reply(token, msg)

            elif "達克瓦茲" in text:
                qty = int("".join(filter(str.isdigit, text)))
                order_id = create_order(user, "達克瓦茲", qty, 95)
                line_reply(token, f"✅ 已建立訂單 {order_id}")

    return {"ok": True}

# =============================
# ORDER + SHEET + CALENDAR
# =============================
def create_order(user, item, qty, price):
    sheets, cal = get_google()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    order_id = f"UOO-{datetime.datetime.now().strftime('%H%M%S')}"
    total = qty * price

    # 👉 C 表
    sheets.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{C_SHEET}!A1",
        valueInputOption="RAW",
        body={"values": [[now, user, order_id, item, qty, total]]}
    ).execute()

    # 👉 Calendar
    cal.events().insert(
        calendarId=GCAL_ID,
        body={
            "summary": f"UooUoo 訂單 {order_id}",
            "description": f"{item} x{qty}",
            "start": {"date": now.split(" ")[0]},
            "end": {"date": now.split(" ")[0]}
        }
    ).execute()

    return order_id
