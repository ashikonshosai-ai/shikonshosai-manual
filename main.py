import os, json, httpx
from datetime import date
import dropbox
from dropbox.exceptions import ApiError as DropboxApiError
from fastapi import FastAPI, Request, UploadFile, File, Form, Query, Header, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

DROPBOX_APP_KEY      = os.environ.get("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET   = os.environ.get("DROPBOX_APP_SECRET")
DROPBOX_REFRESH_TOKEN = os.environ.get("DROPBOX_REFRESH_TOKEN")
MANUALS_PATH  = "/400000_CC/shikonshosai/manuals.json"
NOTICES_PATH  = "/400000_CC/shikonshosai/notices.json"
QA_PATH       = "/400000_CC/shikonshosai/qa.json"
USERS_PATH    = "/400000_CC/shikonshosai/users.json"
IMAGES_BASE   = "/400000_CC/shikonshosai/manual_images"
REPORTS_BASE  = "/外注先共有/400000_CC/shikonshosai/reports"

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

@app.get("/api/qa")
async def get_qa():
    data = await dropbox_get(QA_PATH)
    return data if data else {"questions": []}

@app.post("/api/qa")
async def save_qa(request: Request):
    data = await request.json()
    await dropbox_save(QA_PATH, data)
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

@app.get("/api/reports/all/{year_month}")
async def get_all_reports(year_month: str, user_id: str = Header(None)):
    users_data = await dropbox_get(USERS_PATH)
    if not users_data:
        raise HTTPException(status_code=500)
    current_user = next((u for u in users_data.get("users", []) if u.get("id") == user_id), None)
    if not current_user:
        raise HTTPException(status_code=401)
    role = current_user.get("role", "staff")
    if role == "admin":
        target_ids = [u["id"] for u in users_data["users"]]
    elif role == "leader":
        my_group = current_user.get("group", "")
        target_ids = [u["id"] for u in users_data["users"] if u.get("group") == my_group]
    else:
        raise HTTPException(status_code=403)
    results = {}
    for uid in target_ids:
        path = f"{REPORTS_BASE}/{uid}_{year_month}.json"
        data = await dropbox_get(path)
        if data:
            user = next((u for u in users_data["users"] if u["id"] == uid), None)
            results[uid] = {
                "user_name": user["name"] if user else uid,
                "entries": data.get("entries", [])
            }
    return results

@app.get("/api/reports/{user_id}/{year_month}")
async def get_report(user_id: str, year_month: str):
    path = f"{REPORTS_BASE}/{user_id}_{year_month}.json"
    data = await dropbox_get(path)
    return data if data else {"entries": []}

@app.post("/api/reports/{user_id}/{year_month}")
async def save_report(user_id: str, year_month: str, request: Request):
    data = await request.json()
    path = f"{REPORTS_BASE}/{user_id}_{year_month}.json"
    await dropbox_save(path, data)
    return {"ok": True}

@app.on_event("startup")
async def startup_event():
    data = await dropbox_get(USERS_PATH)
    if data is None:
        await dropbox_save(USERS_PATH, {
            "password": "shikonshosai",
            "users": [{"id": "u1", "name": "勝野弘志", "email": "hkcpa416@gmail.com", "role": "admin", "group": "", "photo": ""}]
        })
    else:
        updated = False
        for u in data.get("users", []):
            if "group" not in u:
                u["group"] = ""
                updated = True
        if updated:
            await dropbox_save(USERS_PATH, data)

@app.get("/api/users")
async def get_users():
    data = await dropbox_get(USERS_PATH)
    if data is None:
        return {"users": []}
    return {"users": data.get("users", [])}

@app.post("/api/users")
async def save_users(request: Request):
    data = await request.json()
    await dropbox_save(USERS_PATH, data)
    return {"ok": True}

@app.post("/api/auth/login")
async def login(request: Request):
    body = await request.json()
    email = body.get("email", "").strip()
    password = body.get("password", "")
    data = await dropbox_get(USERS_PATH)
    if data is None:
        return JSONResponse({"error": "ユーザーが見つかりません"}, status_code=401)
    user = next((u for u in data.get("users", []) if u.get("email") == email), None)
    if not user:
        return JSONResponse({"error": "ユーザーが見つかりません"}, status_code=401)
    individual_pw = user.get("individual_password", "")
    if individual_pw:
        if password != individual_pw:
            return JSONResponse({"error": "パスワードが違います"}, status_code=401)
    else:
        if password != data.get("password", ""):
            return JSONResponse({"error": "パスワードが違います"}, status_code=401)
    password_changed = bool(individual_pw)
    for u in data.get("users", []):
        if u.get("email") == email:
            u["last_login"] = date.today().isoformat()
            break
    await dropbox_save(USERS_PATH, data)
    return {**{k: v for k, v in user.items() if k != "individual_password"}, "password_changed": password_changed, "last_login": date.today().isoformat()}

@app.post("/api/auth/change_password")
async def change_password(request: Request):
    body = await request.json()
    email = body.get("email", "").strip()
    new_password = body.get("new_password", "")
    data = await dropbox_get(USERS_PATH)
    if data is None:
        return JSONResponse({"error": "ユーザーが見つかりません"}, status_code=404)
    for user in data.get("users", []):
        if user.get("email") == email:
            user["individual_password"] = new_password
            break
    else:
        return JSONResponse({"error": "ユーザーが見つかりません"}, status_code=404)
    await dropbox_save(USERS_PATH, data)
    return {"ok": True}

@app.post("/api/auth/ping")
async def auth_ping(request: Request):
    body = await request.json()
    user_id = body.get("user_id")
    if not user_id:
        raise HTTPException(status_code=400)
    users_data = await dropbox_get(USERS_PATH)
    if not users_data:
        raise HTTPException(status_code=500)
    today = date.today().isoformat()
    for u in users_data.get("users", []):
        if u.get("id") == user_id:
            u["last_login"] = today
            break
    await dropbox_save(USERS_PATH, users_data)
    return {"ok": True}

app.mount("/", StaticFiles(directory="static", html=True), name="static")
