import dropbox, json, os

app_key = os.environ.get("DROPBOX_APP_KEY")
app_secret = os.environ.get("DROPBOX_APP_SECRET")
refresh_token = os.environ.get("DROPBOX_REFRESH_TOKEN")

dbx = dropbox.Dropbox(
    oauth2_refresh_token=refresh_token,
    app_key=app_key,
    app_secret=app_secret,
)

_, res = dbx.files_download("/400000_CC/shikonshosai/manuals.json")
data = json.loads(res.content)

for cat in data["categories"]:
    if cat["name"] == "全般・ルール":
        for manual in cat["manuals"]:
            if manual["title"] == "全般":
                manual["content"] += """

【月次作業の流れ】
1. 記帳代行先で月次試算表が完成したら、Dropboxに保管するとともに各チームチャットで監査担当者及び勝野宛にPDFあげてください。
2. 監査担当者による試算表の査閲が終わったら、会社別チャットグループで会社に納品してください。
3. 自計化先で月次試算表のチェックが完了したら、Dropboxに保管するとともに各チームチャットで監査担当者及び勝野宛にPDFあげてください。

【コンサル資料作成先の業務手順】
１）次回コンサルティング日程を会社チャットグループ（CC）の概要欄に記載します。
２）原則として１の2営業日前をコンサル資料作成期限とし、概要欄に記載します。
３）期限までに作成者は各チームチャットグループ（TC）にPDF資料をアップロードする。
４）該当会社の監査担当者（勝野・西濱・福井）をメンションする。
５）監査担当者はPDFをチェックしTC内で承認コメントをする。
６）作成者は承認後、CCへのアップロードとDropboxの印刷専用フォルダにPDF資料を保存する。
７）現物資料が必要な先については、全体チャットでスタッフ大野をメンションして完了の旨報告する。
８）スタッフ大野は印刷専用フォルダから印刷・製本し、その後ファイルは削除する。

【freeeログイン情報】
URL: https://secure.freee.co.jp/
ID：a.shikonshosai+〇〇@gmail.com
PW：N17162os

【freee税理士招待】
メールアドレス: katsuno@hkcpa.jp
事業所番号: 145-264-6008

【MFログイン情報】
⚠️外部持出厳禁⚠️
https://erp.moneyforward.com/home
ID：nunoi@tkcnf.or.jp
パスワード：N17162os

【Dropbox共有フォルダ】
⚠️外部持出厳禁⚠️
https://www.dropbox.com/sh/5il46han7molkqx/AAAkyI8f6Dq31KN-Ui1oVaAFa?dl=0
"""

    if cat["name"] == "新人ガイダンス":
        for manual in cat["manuals"]:
            if manual["title"] == "新人ガイダンス":
                manual["content"] += """

【外部委託基本取り決め】
1. 毎週木曜日までに次週のシフト予定をスケジューラに登録してください。
2. 翌月10日までに請求書をDW形式でグループ長宛に個別にチャットワークで送ってください。請求書のタイトルは「3桁コード＋名前＋年月＋請求書」としてください。
3. 業務日報を都度作成し、DW形式で請求書に添付してください。
4. 資料の受渡と保管には十分に注意し、受渡管理簿を更新してください。
5. 貸与PCの取り扱いには十分に気を付けてください。
6. 守秘義務は必ず守ってください。
"""

content = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
dbx.files_upload(content, "/400000_CC/shikonshosai/manuals.json",
    mode=dropbox.files.WriteMode.overwrite)
print("完了")
