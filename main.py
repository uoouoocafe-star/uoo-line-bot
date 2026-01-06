from fastapi import FastAPI, Request, HTTPException
from linebot.v3 import WebhookHandler
from linebot.v3.messaging import MessagingApi, Configuration
from linebot.v3.webhooks import MessageEvent, TextMessageContent
import os

# ✅ 一定要有這行（Render 要找的）
app = FastAPI()

# LINE 環境變數
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN")
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET")

if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    raise RuntimeError("Missing LINE channel environment variables")

configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)
messaging_api = MessagingApi(configuration)


# 健康檢查（Render / Browser 用）
@app.get("/")
def root():
    return {"ok": True}


# LINE webhook
@app.post("/callback")
async def callback(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature")

    try:
        handler.handle(body.decode("utf-8"), signature)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    return "OK"


# 收到文字訊息時的回覆
@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    text = event.message.text.strip()

    if text == "菜單":
        reply = "🍞 UooUoo Cafe 菜單\n\n☕ 咖啡\n🍰 甜點\n🥐 早午餐"
    else:
        reply = f"你說的是：{text}"

    messaging_api.reply_message(
        reply_token=event.reply_token,
        messages=[{"type": "text", "text": reply}],
    )
