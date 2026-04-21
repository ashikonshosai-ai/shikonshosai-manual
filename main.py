import os, json, httpx, asyncio, time, io
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from datetime import date
import dropbox
from dropbox.exceptions import ApiError as DropboxApiError
from fastapi import FastAPI, Request, UploadFile, File, Form, Query, Header, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
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

_cache: dict = {}
_CACHE_TTL = 60

def _cache_get(key):
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < _CACHE_TTL:
            return data
    return None

def _cache_set(key, data):
    _cache[key] = (data, time.time())

def _cache_delete(key):
    _cache.pop(key, None)

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
    cached = _cache_get("manuals")
    if cached is not None:
        return cached
    data = await dropbox_get(MANUALS_PATH) or {"categories": []}
    _cache_set("manuals", data)
    return data

@app.post("/api/manuals")
async def save_manuals(request: Request):
    data = await request.json()
    await dropbox_save(MANUALS_PATH, data)
    _cache_delete("manuals")
    return {"ok": True}

@app.get("/api/notices")
async def get_notices():
    cached = _cache_get("notices")
    if cached is not None:
        return cached
    data = await dropbox_get(NOTICES_PATH) or {"notices": []}
    _cache_set("notices", data)
    return data

@app.post("/api/notices")
async def save_notices(request: Request):
    data = await request.json()
    await dropbox_save(NOTICES_PATH, data)
    _cache_delete("notices")
    return {"ok": True}

@app.get("/api/qa")
async def get_qa():
    cached = _cache_get("qa")
    if cached is not None:
        return cached
    data = await dropbox_get(QA_PATH) or {"questions": []}
    _cache_set("qa", data)
    return data

@app.post("/api/qa")
async def save_qa(request: Request):
    data = await request.json()
    await dropbox_save(QA_PATH, data)
    _cache_delete("qa")
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
        target_users = users_data["users"]
    elif role == "leader":
        my_group = current_user.get("group", "")
        target_users = [u for u in users_data["users"] if u.get("group") == my_group]
    else:
        raise HTTPException(status_code=403)

    async def fetch_report(user):
        path = f"{REPORTS_BASE}/{user['id']}_{year_month}.json"
        data = await dropbox_get(path)
        return user["id"], user.get("name", user["id"]), (data or {}).get("entries", [])

    results_list = await asyncio.gather(*[fetch_report(u) for u in target_users])
    results = {uid: {"user_name": uname, "entries": entries} for uid, uname, entries in results_list if entries}
    return results

@app.get("/api/reports/excel/{year_month}")
async def download_reports_excel(
    year_month: str,
    user_id: str = Query(None)
):
    if not user_id:
        raise HTTPException(status_code=401)
    users_data = await dropbox_get(USERS_PATH)
    if not users_data:
        raise HTTPException(status_code=500)
    current_user = next((u for u in users_data.get("users", []) if u.get("id") == user_id), None)
    if not current_user or current_user.get("role", "staff") == "staff":
        raise HTTPException(status_code=403)

    if current_user["role"] == "admin":
        target_users = users_data["users"]
    else:
        my_group = current_user.get("group", "")
        target_users = [u for u in users_data["users"] if u.get("group") == my_group]

    async def fetch_report(user):
        path = f"{REPORTS_BASE}/{user['id']}_{year_month}.json"
        try:
            data = await dropbox_get(path)
            return user.get("name", user["id"]), (data or {}).get("entries", [])
        except Exception:
            return user.get("name", user["id"]), []

    results = await asyncio.gather(*[fetch_report(u) for u in target_users])

    # 会社×スタッフのクロス集計
    companies: dict = {}
    for uname, entries in results:
        for e in entries:
            company = e.get("company_name") or "（会社名なし）"
            if company not in companies:
                companies[company] = {}
            companies[company][uname] = companies[company].get(uname, 0) + e.get("hours", 0)

    staff_names = [r[0] for r in results]
    sorted_companies = sorted(
        companies.items(),
        key=lambda kv: sum(kv[1].values()),
        reverse=True
    )

    wb = Workbook()
    ws = wb.active
    ws.title = f"{year_month}集計"

    header_fill = PatternFill(fill_type="solid", fgColor="2563EB")
    header_font = Font(bold=True, color="FFFFFF")
    center = Alignment(horizontal="center")

    header = ["会社名"] + staff_names + ["合計"]
    for col, val in enumerate(header, 1):
        cell = ws.cell(row=1, column=col, value=val)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center

    for row_idx, (company, staff_hours) in enumerate(sorted_companies, 2):
        ws.cell(row=row_idx, column=1, value=company)
        row_total = 0.0
        for col_idx, sname in enumerate(staff_names, 2):
            h = round(staff_hours.get(sname, 0) * 100) / 100
            if h > 0:
                ws.cell(row=row_idx, column=col_idx, value=h)
                row_total += h
        ws.cell(row=row_idx, column=len(staff_names) + 2, value=round(row_total * 100) / 100)

    total_row = len(sorted_companies) + 2
    ws.cell(row=total_row, column=1, value="合計").font = Font(bold=True)
    grand_total = 0.0
    for col_idx, sname in enumerate(staff_names, 2):
        col_total = round(sum(v.get(sname, 0) for v in companies.values()) * 100) / 100
        if col_total > 0:
            ws.cell(row=total_row, column=col_idx, value=col_total).font = Font(bold=True)
        grand_total += col_total
    ws.cell(row=total_row, column=len(staff_names) + 2, value=round(grand_total * 100) / 100).font = Font(bold=True)

    ws.column_dimensions["A"].width = 25
    from openpyxl.utils import get_column_letter
    for i in range(2, len(staff_names) + 3):
        ws.column_dimensions[get_column_letter(i)].width = 12

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    from urllib.parse import quote
    filename = quote(f"グループ集計_{year_month}.xlsx")
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{filename}"}
    )

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

@app.post("/api/users/profile")
async def update_profile(request: Request):
    body = await request.json()
    user_id = body.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401)
    allowed_fields = [
        "personal_email", "phone", "postal_code", "address",
        "bank_name", "bank_branch", "bank_type", "bank_number",
        "bank_holder", "invoice_number", "hourly_rate"
    ]
    users_data = await dropbox_get(USERS_PATH)
    if not users_data:
        raise HTTPException(status_code=500)
    for user in users_data.get("users", []):
        if user.get("id") == user_id:
            for field in allowed_fields:
                if field in body:
                    user[field] = body[field]
            break
    await dropbox_save(USERS_PATH, users_data)
    _cache_delete("users")
    return {"ok": True}

@app.get("/api/users")
async def get_users():
    cached = _cache_get("users")
    if cached is not None:
        return cached
    data = await dropbox_get(USERS_PATH)
    if data is None:
        return {"users": []}
    result = {"users": [{k: v for k, v in u.items() if k != "individual_password"} for u in data.get("users", [])]}
    _cache_set("users", result)
    return result

@app.post("/api/users")
async def save_users(request: Request):
    data = await request.json()
    await dropbox_save(USERS_PATH, data)
    _cache_delete("users")
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
    _cache_delete("users")
    return {"ok": True}

@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    body = await request.json()
    user_id = body.get("user_id")
    if not user_id:
        raise HTTPException(status_code=400)
    users_data = await dropbox_get(USERS_PATH)
    if not users_data:
        raise HTTPException(status_code=500)
    for u in users_data.get("users", []):
        if u.get("id") == user_id:
            u["last_login"] = ""
            break
    await dropbox_save(USERS_PATH, users_data)
    _cache_delete("users")
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
    _cache_delete("users")
    return {"ok": True}

app.mount("/", StaticFiles(directory="static", html=True), name="static")
