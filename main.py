import os
from fastapi import FastAPI, Request, HTTPException

from linebot.v3.webhook import WebhookParser
from linebot.v3.webhooks import MessageEvent, TextMessageContent

from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
)

app = FastAPI()

CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "")

if not CHANNEL_SECRET or not CHANNEL_ACCESS_TOKEN:
    raise RuntimeError("Missing LINE env vars: CHANNEL_SECRET / CHANNEL_ACCESS_TOKEN")

parser = WebhookParser(CHANNEL_SECRET)
config = Configuration(access_token=CHANNEL_ACCESS_TOKEN)


@app.get("/")
def root():
    return {"ok": True}


@app.post("/callback")
async def callback(request: Request):
    body = await request.body()
    body_text = body.decode("utf-8")
    signature = request.headers.get("X-Line-Signature")

    if not signature:
        raise HTTPException(status_code=400, detail="Missing X-Line-Signature")

    try:
        events = parser.parse(body_text, signature)
    except Exception as e:
        print("LINE parse error:", repr(e))
        raise HTTPException(status_code=400, detail=str(e))

    for event in events:
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessageContent):
            text = (event.message.text or "").strip()

            if text == "菜單":
                reply = "🍞 UooUoo Cafe 菜單\n\n☕ 咖啡\n🍰 甜點\n🥐 早午餐"
            else:
                reply = f"你說的是：{text}"

            # ✅ v3 正確回覆方式：ApiClient(config) -> MessagingApi(api_client)
            with ApiClient(config) as api_client:
                api = MessagingApi(api_client)
                api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[TextMessage(text=reply)],
                    )
                )

    return "OK"
