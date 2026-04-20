import os, json, httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

DROPBOX_APP_KEY      = os.environ.get("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET   = os.environ.get("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.environ.get("DROPBOX_REFRESH_TOKEN")
MANUALS_PATH = "/400000_CC/shikonshosai/manuals.json"

async def get_dropbox_token():
    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://api.dropbox.com/oauth2/token",
            data={"grant_type": "refresh_token", "refresh_token": DROPBOX_REFRESH_TOKEN},
            auth=(DROPBOX_APP_KEY, DROPBOX_APP_SECRET)
        )
        return r.json()["access_token"]

async def dropbox_get(path: str):
    token = await get_dropbox_token()
    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://api.dropboxapi.com/2/files/download",
            headers={
                "Authorization": f"Bearer {token}",
                "Dropbox-API-Arg": json.dumps({"path": path})
            }
        )
        if r.status_code == 200:
            return r.json()
        return None

async def dropbox_save(path: str, data: dict):
    token = await get_dropbox_token()
    content = json.dumps(data, ensure_ascii=False, indent=2).encode()
    async with httpx.AsyncClient() as client:
        await client.post(
            "https://api.dropboxapi.com/2/files/upload",
            headers={
                "Authorization": f"Bearer {token}",
                "Dropbox-API-Arg": json.dumps({
                    "path": path,
                    "mode": "overwrite",
                    "autorename": False
                }),
                "Content-Type": "application/octet-stream"
            },
            content=content
        )

@app.get("/api/manuals")
async def get_manuals():
    data = await dropbox_get(MANUALS_PATH)
    if data is None:
        return {"categories": []}
    return data

@app.post("/api/manuals")
async def save_manuals(request: Request):
    data = await request.json()
    await dropbox_save(MANUALS_PATH, data)
    return {"ok": True}

app.mount("/", StaticFiles(directory="static", html=True), name="static")
