from fastapi import FastAPI, Request, HTTPException
from linebot.v3 import WebhookHandler
from linebot.v3.messaging import (
    Configuration,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
import os

# ========= FastAPI =========
app = FastAPI()

# ========= LINE env =========
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN")
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET")

if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    raise RuntimeError("Missing LINE channel env vars")

# ========= LINE SDK =========
configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
messaging_api = MessagingApi(configuration)
handler = WebhookHandler(CHANNEL_SECRET)

# ========= Health check =========
@app.get("/")
def root():
    return {"ok": True}

# ========= LINE webhook =========
@app.post("/callback")
async def callback(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature")

    try:
        handler.handle(body.decode("utf-8"), signature)
    except Exception as e:
        print("LINE handler error:", repr(e))
        raise HTTPException(status_code=400, detail=str(e))

    return "OK"

# ========= Message handler =========
@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    text = event.message.text.strip()

    if text == "菜單":
        reply = "🍞 UooUoo Cafe 菜單\n\n☕ 咖啡\n🍰 甜點\n🥐 早午餐"
    else:
        reply = f"你說的是：{text}"

    messaging_api.reply_message(
        ReplyMessageRequest(
            reply_token=event.reply_token,
            messages=[TextMessage(text=reply)],
        )
    )
