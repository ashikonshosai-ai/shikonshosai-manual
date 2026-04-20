import os, json, httpx
import dropbox
from dropbox.exceptions import ApiError as DropboxApiError
from fastapi import FastAPI, Request, UploadFile, File, Form, Query
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

DROPBOX_APP_KEY      = os.environ.get("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET   = os.environ.get("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.environ.get("DROPBOX_REFRESH_TOKEN")
MANUALS_PATH  = "/400000_CC/shikonshosai/manuals.json"
NOTICES_PATH  = "/400000_CC/shikonshosai/notices.json"
IMAGES_BASE   = "/400000_CC/shikonshosai/manual_images"

def _get_dropbox_client():
    return dropbox.Dropbox(
        oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
        app_key=DROPBOX_APP_KEY,
        app_secret=DROPBOX_APP_SECRET,
    )

async def dropbox_get(path: str):
    try:
        dbx = _get_dropbox_client()
        _, res = dbx.files_download(path)
        return json.loads(res.content)
    except DropboxApiError as e:
        if e.error.is_path() and e.error.get_path().is_not_found():
            return None
        raise

async def dropbox_save(path: str, data: dict):
    dbx = _get_dropbox_client()
    content = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    dbx.files_upload(content, path, mode=dropbox.files.WriteMode.overwrite, mute=True)

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

@app.get("/api/notices")
async def get_notices():
    data = await dropbox_get(NOTICES_PATH)
    return data if data else {"notices": []}

@app.post("/api/notices")
async def save_notices(request: Request):
    data = await request.json()
    await dropbox_save(NOTICES_PATH, data)
    return {"ok": True}

@app.post("/api/manuals/upload_image")
async def upload_image(manual_id: str = Form(...), file: UploadFile = File(...)):
    dbx = _get_dropbox_client()
    content = await file.read()
    path = f"{IMAGES_BASE}/{manual_id}/{file.filename}"
    dbx.files_upload(content, path, mode=dropbox.files.WriteMode.overwrite, mute=True)
    link = dbx.files_get_temporary_link(path)
    return {"path": path, "url": link.link}

@app.get("/api/manuals/image_url")
async def image_url(path: str = Query(...)):
    dbx = _get_dropbox_client()
    link = dbx.files_get_temporary_link(path)
    return {"url": link.link}

@app.delete("/api/manuals/delete_image")
async def delete_image(request: Request):
    body = await request.json()
    dbx = _get_dropbox_client()
    dbx.files_delete_v2(body["path"])
    return {"ok": True}

app.mount("/", StaticFiles(directory="static", html=True), name="static")
