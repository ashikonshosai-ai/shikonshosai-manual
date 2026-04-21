import dropbox, json, os

dbx = dropbox.Dropbox(
    oauth2_refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
    app_key=os.environ["DROPBOX_APP_KEY"],
    app_secret=os.environ["DROPBOX_APP_SECRET"],
)

_, res = dbx.files_download("/400000_CC/shikonshosai/manuals.json")
data = json.loads(res.content)

if any(c["id"] == "cat_links" for c in data["categories"]):
    print("既に追加済みです")
    exit()

data["categories"].append({
    "id": "cat_links",
    "name": "リンク集",
    "manuals": [
        {
            "id": "m_links_1",
            "title": "管理表・スプレッドシート",
            "content": (
                "【外部委託マニュアル】\nhttps://docs.google.com/spreadsheets/d/1HgjDy8zXJ6fZddQLXhgKga_UAs7j5MN5x3B0g2wfyNM/edit\n\n"
                "【申告管理表】\nhttps://docs.google.com/spreadsheets/d/1zb1RDQj55fjP1d6WBD1LzPFCFo8GxDcOC3MLa_Wz4Gs/edit\n\n"
                "【担当表】\nhttps://docs.google.com/spreadsheets/d/1vv24K5BgpbMxTvh0C00-9yGTYUry2TfS/edit\n\n"
                "【業務担当一覧表】\nhttps://docs.google.com/spreadsheets/d/1pJiql9riRdL5469KmsWUMcO_wOwT7QsqVz2E07iyak0/edit\n\n"
                "【給与計算人数管理表】\nhttps://docs.google.com/spreadsheets/d/1XLecFVw5VtWZG5vV3lEt-aaQc-xQGXAS/edit\n\n"
                "【月次進捗管理表】\nhttps://docs.google.com/spreadsheets/d/1hP17TiDrOga74AN37rXq81ezT2NHrykm1BUoSyhe24A/edit\n\n"
                "【納期特例管理表】\nhttps://docs.google.com/spreadsheets/d/1CeNdAQ6E6bs8FfSQIZVOlJoZ-rO9EzBN4WK-6hxXi8c/edit\n\n"
                "【年末調整管理表】\nhttps://docs.google.com/spreadsheets/d/1M8aGXAlEB-wUj6kdd-PAEJA8494QLjZo/edit\n\n"
                "【確定申告管理表】\nhttps://docs.google.com/spreadsheets/d/1FPkdaOCMT7NK4cSdqvUXovHlRn2HJRnp/\n\n"
                "【個別面談調整】\nhttps://docs.google.com/spreadsheets/d/1QelZMbVDzVDSsK5cs-O5Iy60Bcy4VNez9aK3i-4g10I/edit\n\n"
                "【アシスタント会議資料】\nhttps://docs.google.com/spreadsheets/d/16MvG0_q6Lk0byVY88BlzybvtYISIbQXi/edit\n\n"
                "【質問回答集・インボイス編】\nhttps://docs.google.com/spreadsheets/d/1mRzRl-eEWeRpmzG7VteqCz8P5KrangkpNh2D8U7CowE/edit"
            ),
            "images": [], "video_url": "", "updated_at": "2026-04-21", "updated_by": "インポート"
        },
        {
            "id": "m_links_2",
            "title": "動画マニュアル・研修",
            "content": (
                "【動画マニュアル（Vimeo）】\nhttps://vimeo.com/user/47128708/folder/14827149\n\n"
                "【freee研修会動画（YouTube）】\nhttps://www.youtube.com/watch?v=wDBfta143fA"
            ),
            "images": [], "video_url": "", "updated_at": "2026-04-21", "updated_by": "インポート"
        },
        {
            "id": "m_links_3",
            "title": "ミーティング・その他",
            "content": (
                "【在宅チームMTG（Zoom）】\nhttps://us02web.zoom.us/j/85761949820\n"
                "ミーティングID: 857 6194 9820\n\n"
                "【業務管理システム】\nhttps://shikonshosai-app.onrender.com/\n\n"
                "【事務所マニュアル】\nhttps://shikonshosai-manual.onrender.com/"
            ),
            "images": [], "video_url": "", "updated_at": "2026-04-21", "updated_by": "インポート"
        }
    ]
})

content = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
dbx.files_upload(content, "/400000_CC/shikonshosai/manuals.json",
    mode=dropbox.files.WriteMode.overwrite)
print("完了")
