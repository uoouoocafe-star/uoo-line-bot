import os
from fastapi import FastAPI, Request, HTTPException
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent,
    TextMessage,
    TextSendMessage,
    FlexSendMessage,
)

# ========= 基本設定 =========
CHANNEL_ACCESS_TOKEN = os.getenv("CHANNEL_ACCESS_TOKEN", "").strip()
CHANNEL_SECRET = os.getenv("CHANNEL_SECRET", "").strip()

if not CHANNEL_ACCESS_TOKEN or not CHANNEL_SECRET:
    raise RuntimeError("LINE token / secret 沒有設定")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(CHANNEL_SECRET)

app = FastAPI()


# ========= 健康檢查（Render 需要） =========
@app.get("/")
def health_check():
    return {"status": "ok"}


# ========= LINE Webhook =========
@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    body_text = body.decode("utf-8")

    try:
        events = parser.parse(body_text, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    for event in events:
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
            handle_text(event)

    return "OK"


# ========= 文字處理 =========
def handle_text(event: MessageEvent):
    text = event.message.text.strip()

    if text in ["開始", "hi", "Hi", "hello", "Hello"]:
        send_home(event.reply_token)
        return

    if "甜點" in text:
        send_group_buy(event.reply_token)
        return

    if "彌月" in text:
        send_baby_box(event.reply_token)
        return

    # 預設回覆
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text="🤍 歡迎來到 UooUoo，請輸入「開始」")
    )


# ========= 首頁 =========
def send_home(reply_token: str):
    flex = FlexSendMessage(
        alt_text="UooUoo 甜點工作室",
        contents={
            "type": "bubble",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "contents": [
                    {
                        "type": "text",
                        "text": "UooUoo 甜點工作室 🤍",
                        "weight": "bold",
                        "size": "lg"
                    },
                    {
                        "type": "text",
                        "text": "請選擇你想看的項目 👇",
                        "size": "sm",
                        "color": "#666666"
                    }
                ]
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "contents": [
                    {
                        "type": "button",
                        "style": "primary",
                        "color": "#F5C6CB",
                        "action": {
                            "type": "message",
                            "label": "🍪 日常甜點團購",
                            "text": "甜點"
                        }
                    },
                    {
                        "type": "button",
                        reminder=True if False else "secondary",
                        "action": {
                            "type": "message",
                            "label": "🎁 彌月禮盒",
                            "text": "彌月"
                        }
                    }
                ]
            }
        }
    )

    line_bot_api.reply_message(reply_token, flex)


# ========= 日常甜點團購 =========
def send_group_buy(reply_token: str):
    text = (
        "🍪【日常甜點團購】\n\n"
        "我們以達克瓦茲為主，\n"
        "司康與奶酥為輔，\n"
        "少量製作、不定期開團。\n\n"
        "📦 目前品項請關注公告\n"
        "📅 下次開團時間將另行通知 🤍"
    )

    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(text=text)
    )


# ========= 彌月禮盒 =========
def send_baby_box(reply_token: str):
    text = (
        "🎁 彌月禮盒\n\n"
        "恭喜你，正在準備迎接新生命 🤍\n\n"
        "UooUoo 的彌月禮盒，\n"
        "是為「會被好好吃完」而做的甜點。\n\n"
        "若你正在比較彌月禮盒，\n"
        "我們很建議先試吃再決定。"
    )

    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(text=text)
    )
