import io
import json
import math
import os

from flask import Flask, request
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
import tabula

app = Flask(__name__)
MAIN_PATH = ""


def get_key_after(keys, search_key):
    for i in range(len(keys)):
        key = keys[i]
        if key == search_key:
            return keys[i + 1] if i < len(keys) else None
    return None


def clean_costs_result(costs):
    result = []
    for key in costs:
        value = costs[key]
        if type(costs[key]) == float and math.isnan(costs[key]):
            value = "$0"
        result_key = key.capitalize()
        result.append({"key": f"{result_key} Cost" if result_key != "Amount" else result_key, "value": value})
    return result


def get_all_costs(tables):
    for table in tables:
        keys = list(table.keys())
        start_column = get_key_after(keys, 'CITY &')
        if start_column is not None:
            number_of_rows = len(table)
            for i in range(0, number_of_rows):
                result = table.loc[i, start_column:keys[len(keys) - 1]].to_dict()
                if does_it_have_costs(result):
                    return clean_costs_result(result)
    return {}


def download_file_from_drive(drive_service, file_id, file_type):
    request = drive_service.files().get_media(fileId=file_id)
    file_data = io.BytesIO()
    downloader = MediaIoBaseDownload(file_data, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    file_data.seek(0)
    with open(f"{MAIN_PATH}{file_id}.{file_type}", 'wb') as f:
        f.write(file_data.read())
        f.close()


def create_drive_service():
    creds = json.loads(os.environ['CREDENTIALS'])
    scope = ['https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds, scope)
    creds = creds.create_delegated('daniel@hughjoseph.com')
    return build('drive', 'v3', credentials=creds)


def get_document_pdf(drive_service, document_id):
    full_name = f"{MAIN_PATH}{document_id}.pdf"
    download_file_from_drive(drive_service, document_id, "pdf")
    return full_name


def does_it_have_costs(costs):
    for key in costs:
        value = costs[key]
        if not (type(costs[key]) == float and math.isnan(costs[key])) and "$" in value:
            return True
    return False


@app.route('/post_endpoint', methods=['POST'])
def get_invoice_costs():
    request_json = request.json
    pdf_id = request_json["pdfId"]
    drive_service = create_drive_service()
    path = get_document_pdf(drive_service, pdf_id)
    tables = tabula.read_pdf(path, pages='1')
    result = {"status": True, "costs": get_all_costs(tables)}
    return result


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
