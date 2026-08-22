#/nsetradingbot/Code/zerodha/callback.py
from fastapi import FastAPI, Request

app = FastAPI()


@app.get("/")
def home():
    return {
        "status": "Zerodha callback server is running"
    }


@app.get("/zerodha/callback")
async def zerodha_callback(request: Request):
    params = dict(request.query_params)

    return {
        "status": "callback_received",
        "parameters": params
    }