'''
Script to download all files on OEP that have 'tag = SEDOS'
'''

import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import zipfile

list_tables = []
daily_table_list = []


# Filter SEDOS tag
SEDOS_TAG_URL = "https://openenergy-platform.org/dataedit/view/model_draft?query=&tags=246"

# constants
DOWNLOAD_LOCATION = fr'{os.environ.get("DOWNLOAD_LOCATION")}'
DATE_OF_TODAY = datetime.today().strftime('%Y-%m-%d')
DAILY_DOWNLOAD_LOCATION = os.path.join(DOWNLOAD_LOCATION, DATE_OF_TODAY)
SECTORS = ["pow", "hea", "x2x", "ind", "tra"]

# create dirs
os.makedirs(DAILY_DOWNLOAD_LOCATION, exist_ok=True)
for sector in SECTORS:
    os.makedirs(os.path.join(DAILY_DOWNLOAD_LOCATION, sector), exist_ok=True)

print(f'Save in DAILY_DOWNLOAD_LOCATION folder: {DAILY_DOWNLOAD_LOCATION}')

def get_tables():
    print("Running get tables()")
    html = requests.get(SEDOS_TAG_URL).content
    soup = BeautifulSoup(html, features="html.parser")
    table = soup.find(id="tables")
    rows = table.find_all("tr")
    for row in rows:
        if "onclick" not in row.contents[1].attrs:
            continue
        raw_url = row.contents[1].attrs["onclick"].split("'")[1]
        yield raw_url.split("/")[-1]

def get_table_dl_link_datapackage_response(table):
    csv_url = f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table}/rows?form=csv"
    json_url = f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table}/rows?form=datapackage"
    zip_url = f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table}/rows?form=datapackage"
    csv_resp = requests.get(csv_url)
    metadata_resp = requests.get(json_url)
    zip_resp = requests.get(zip_url)
    return csv_resp, metadata_resp, zip_resp


def download_datapackage(table):
    csv_resp, metadata_resp, zip_resp = get_table_dl_link_datapackage_response(table)

    prefix_to_directory = {
        "pow_": "pow",
        "hea_": "hea",
        "x2x_": "x2x",
        "ind_": "ind",
        "tra_": "tra"
    }

    # Determine the directory based on the prefix
    for prefix, directory in prefix_to_directory.items():
        if prefix in table:
            datapackage_file_location = os.path.join(DAILY_DOWNLOAD_LOCATION, directory, table)
            break
    else:
        datapackage_file_location = os.path.join(DAILY_DOWNLOAD_LOCATION, table)

    csv_location = datapackage_file_location + ".csv"
    json_location = datapackage_file_location + ".json"
    zip_location = datapackage_file_location + ".zip"   
    

    with open(csv_location, 'wb') as f:
        f.write(csv_resp.content)

    # with open(json_location, 'wb') as f:
    #     f.write(metadata_resp.content)
    #
    # with open(zip_location, 'wb') as f:
    #     f.write(zip_resp.content)
    #
    # # Extract the files within the zip file to the same folder
    # with zipfile.ZipFile(zip_location, 'r') as zip_ref:
    #     extract_location = os.path.dirname(zip_location)
    #     zip_ref.extractall(extract_location)

def download_sedos_tables():
    for table in get_tables():
        try:
            download_datapackage(table)
            print(f"Save: {table}")
            list_tables.append(table)
        except Exception:
            print(f"{table}: Could not be downloaded and saved.")


if __name__ == "__main__":
    download_sedos_tables()




