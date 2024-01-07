from minio import Minio
import urllib.request
import pandas as pd
import sys
import requests
from bs4 import BeautifulSoup
import re
import os

page_url = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet"


def main():
    grab_data(page_url, base_url, 23, 24, 1, 8, "../data/raw/")
    

def grab_data(page_url, base_url, start_year, end_year, start_month, end_month, destination_folder):
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    # Send an HTTP request to retrieve the content of the page
    response = requests.get(page_url)
    if response.status_code != 200:
        print(f"Failed to retrieve the web page. Status code: {response.status_code}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    # Generate list of dates for the specified range
    dates = ["20{:02d}-{:02d}".format(year, month) for year in range(start_year, end_year + 1) 
             for month in range(start_month, end_month + 1)]
    links = [base_url.format(date) for date in dates]

    # Ensure the destination folder exists
    os.makedirs(destination_folder, exist_ok=True)

    for link in links:
        # Generate the filename using the date in the URL
        filename = link.split("/")[-1]

        # Full path for the destination file
        file_path = os.path.join(destination_folder, filename)

        # Test if the link is accessible
        link_response = requests.head(link)
        if link_response.status_code != 200:
            print(f"Download failed for: {filename}. Link not accessible.")
            continue

        # The link is accessible, download the file
        file_response = requests.get(link)
        try:
            with open(file_path, 'wb') as file:
                file.write(file_response.content)
            print(f"Successful download: {filename}")
        except IOError as e:
            print(f"Error writing to file: {filename}. Error: {e}")

    print("All files have been downloaded.")


def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "Datalake_01"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    sys.exit(main())
