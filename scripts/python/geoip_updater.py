import os
import requests
import hashlib
import zipfile
from datetime import datetime
import shutil

ACCOUNT_ID = os.getenv('MAXMIND_ACCOUNT_ID')
LICENSE_KEY = os.getenv('MAXMIND_LICENSE_KEY')
DOWNLOAD_DIR = './geolite2'

DATABASES = {
    'ASN': {
        'url': 'https://download.maxmind.com/geoip/databases/GeoLite2-ASN-CSV/download?suffix=zip',
        'sha256_url': 'https://download.maxmind.com/geoip/databases/GeoLite2-ASN-CSV/download?suffix=zip.sha256',
        'extract_file': 'GeoLite2-ASN-Blocks-IPv4.csv'
    },
    'City': {
        'url': 'https://download.maxmind.com/geoip/databases/GeoLite2-City-CSV/download?suffix=zip',
        'sha256_url': 'https://download.maxmind.com/geoip/databases/GeoLite2-City-CSV/download?suffix=zip.sha256',
        'extract_file': 'GeoLite2-City-Locations-en.csv'
    },
    'Country': {
        'url': 'https://download.maxmind.com/geoip/databases/GeoLite2-Country-CSV/download?suffix=zip',
        'sha256_url': 'https://download.maxmind.com/geoip/databases/GeoLite2-Country-CSV/download?suffix=zip.sha256',
        'extract_file': 'GeoLite2-Country-Locations-en.csv'
    }
}

def ensure_download_dir():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

def get_latest_database_info(url):
    headers = requests.head(url, auth=(ACCOUNT_ID, LICENSE_KEY), allow_redirects=True).headers
    last_modified = headers.get('last-modified')
    content_disposition = headers.get('content-disposition', '')
    
    if last_modified:
        latest_date = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S GMT').strftime('%Y%m%d')
    else:
        latest_date = content_disposition.split('_')[-1].split('.')[0]
    
    return latest_date

def download_database(url, db_name):
    response = requests.get(url, auth=(ACCOUNT_ID, LICENSE_KEY), allow_redirects=True)
    if response.status_code == 200:
        latest_date = get_latest_database_info(url)
        filename = os.path.join(DOWNLOAD_DIR, f"GeoLite2-{db_name}-CSV_{latest_date}.zip")
        with open(filename, 'wb') as f:
            f.write(response.content)
        return filename
    else:
        print(f"Failed to download {db_name} database. Status code: {response.status_code}")
        return None

def verify_sha256(filename, sha256_url):
    response = requests.get(sha256_url, auth=(ACCOUNT_ID, LICENSE_KEY), allow_redirects=True)
    if response.status_code == 200:
        expected_sha256 = response.text.strip().split()[0]
        with open(filename, 'rb') as f:
            calculated_sha256 = hashlib.sha256(f.read()).hexdigest()
        return calculated_sha256 == expected_sha256
    else:
        print(f"Failed to download SHA256 for {filename}. Status code: {response.status_code}")
        return False

def extract_specific_file(zip_filename, extract_filename):
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        for file in zip_ref.namelist():
            if file.endswith(extract_filename):
                zip_ref.extract(file, DOWNLOAD_DIR)
                extracted_path = os.path.join(DOWNLOAD_DIR, file)
                new_path = os.path.join(DOWNLOAD_DIR, extract_filename)
                os.rename(extracted_path, new_path)
                return new_path
    return None

def clean_up_extracted_dirs(zip_filename):
    base_name = os.path.splitext(os.path.basename(zip_filename))[0]
    extracted_dir = os.path.join(DOWNLOAD_DIR, base_name)
    if os.path.exists(extracted_dir):
        shutil.rmtree(extracted_dir)

def update_database(db_name, db_info):
    print(f"Checking {db_name} database...")
    latest_date = get_latest_database_info(db_info['url'])
    local_file = os.path.join(DOWNLOAD_DIR, f"GeoLite2-{db_name}-CSV_{latest_date}.zip")
    
    if os.path.exists(local_file):
        print(f"Local {db_name} database {local_file} is up to date.")
        return

    print(f"Updating GeoLite2 {db_name} database...")
    downloaded_file = download_database(db_info['url'], db_name)
    
    if downloaded_file:
        if verify_sha256(downloaded_file, db_info['sha256_url']):
            print(f"SHA256 verification successful for {db_name} database.")
            extracted_file = extract_specific_file(downloaded_file, db_info['extract_file'])
            if extracted_file:
                print(f"Extracted {db_info['extract_file']} to {extracted_file}")
                clean_up_extracted_dirs(downloaded_file)
                print(f"GeoLite2 {db_name} database has been updated to version {latest_date}.")
            else:
                print(f"Failed to extract {db_info['extract_file']} from {downloaded_file}")
        else:
            print(f"SHA256 verification failed for {db_name} database. The downloaded file may be corrupted.")
            os.remove(downloaded_file)
    else:
        print(f"Failed to update the {db_name} database.")

def main():
    ensure_download_dir()
    for db_name, db_info in DATABASES.items():
        update_database(db_name, db_info)

if __name__ == "__main__":
    main()