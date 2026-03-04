import requests
import time
from datetime import datetime, UTC
from google.cloud import storage
import logging
import io

# Configuration
PROJECT_ID = "yellow-taxi-trips-2026"
BUCKET_NAME = f"{PROJECT_ID}-data-bucket"
GCS_FOLDER = "dataset/trips/"
GCS_LOG_FOLDER = "from-git/logs/"

# Initialisation du client GCS
storage_client = storage.Client()

# Configuration du logging
log_stream = io.StringIO()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(log_stream), # Pour envoyer vers GCS à la fin
        logging.StreamHandler()            # Pour voir dans ton terminal
    ]
) 

def file_exists_in_gcs(bucket_name, gcs_path):
    """Vérifie si un fichier existe déjà sur GCS."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    return blob.exists()

def upload_to_gcs(bucket_name, gcs_path, content):
    """Téléverse le contenu binaire vers GCS."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(content)

def upload_log_to_gcs():
    """Téléverse le fichier de log vers GCS."""
    log_filename = f"{GCS_LOG_FOLDER}extract_log_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(log_filename)
        blob.upload_from_string(log_stream.getvalue())
        print(f"Log file uploaded to {log_filename}")
    except Exception as e:
        print(f"Could not upload logs: {e}")

def download_histo_data():
    """Télécharge les fichiers Parquet de 2022 à Nov 2024."""
    try:
        for year in range(2022, 2025): # 2022, 2023, 2024
            for month in range(1, 13):
                if year == 2024 and month > 11:
                    break
                
                file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
                gcs_path = f"{GCS_FOLDER}{file_name}"
                download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

                if file_exists_in_gcs(BUCKET_NAME, gcs_path):
                    logging.info(f"{file_name} exists, skipping...")
                    continue

                logging.info(f"Downloading {file_name}...")
                response = requests.get(download_url, stream=True)

                if response.status_code == 200:
                    upload_to_gcs(BUCKET_NAME, gcs_path, response.content)
                    logging.info(f"Successfully uploaded {file_name}")
                elif response.status_code == 404:
                    logging.warning(f"File {file_name} not found on source.")
                else:
                    logging.error(f"Error {response.status_code} for {file_name}")
                
                time.sleep(1) # Respecter le serveur distant

        logging.info("All downloads completed!")

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
    finally:
        upload_log_to_gcs()

if __name__ == '__main__':
    logging.info(f"Starting historical data download process...")
    download_histo_data() 
