import httpx, json, os

app_key      = os.environ.get("DROPBOX_APP_KEY")
app_secret   = os.environ.get("DROPBOX_APP_SECRET")
refresh_token = os.environ.get("DROPBOX_REFRESH_TOKEN")

print(f"APP_KEY exists: {bool(app_key)}")
print(f"APP_SECRET exists: {bool(app_secret)}")
print(f"REFRESH_TOKEN exists: {bool(refresh_token)}")

r = httpx.post(
    "https://api.dropbox.com/oauth2/token",
    data={"grant_type": "refresh_token", "refresh_token": refresh_token},
    auth=(app_key, app_secret)
)
print(f"Token status: {r.status_code}")
print(f"Token response: {r.text[:200]}")

if r.status_code == 200:
    token = r.json()["access_token"]
    for path in ["/400000_CC/shikonshosai/manuals.json", "/shikonshosai/manuals.json"]:
        r2 = httpx.post(
            "https://api.dropboxapi.com/2/files/download",
            headers={
                "Authorization": f"Bearer {token}",
                "Dropbox-API-Arg": json.dumps({"path": path})
            }
        )
        print(f"Download [{path}] status: {r2.status_code}")
        if r2.status_code == 200:
            print(f"  内容: {r2.text[:200]}")
            break
