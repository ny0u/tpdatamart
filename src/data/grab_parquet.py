
import urllib.request
import pandas as pd
import sys
import os
import requests
from minio import Minio
from datetime import datetime
from io import BytesIO#est utilisé pour créer un objet en mémoire qui contient les données téléchargées, et qui a une méthode read(), ce qui le rend compatible avec minio_client.put_object.
from sqlalchemy import create_engine


minio_client = Minio(
    "127.0.0.1:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)


def main():
    grab_data()



def download_and_upload_to_minio(url, bucket_name, object_name):
    response = requests.get(url, stream=True)
    response.raise_for_status()
# Création d'un objet BytesIO pour stocker les données téléchargées
    data = BytesIO(response.content)    #pour créer un objet similaire à un fichier en mémoire
 # Stockage dans Minio
    minio_client.put_object(
        bucket_name, object_name, data, length = int(response.headers['Content-Length'])
    )
    print(f"Fichier {object_name} téléchargé et stocké dans MinIO")


 # Configuration des paramètres
def grab_data() -> None:
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    years = [2023]
    months = range(1, 13)
    bucket_name = "bigbucket"


    for year in years:
        for month in months:
            url = f"{base_url}{year}-{month:02d}.parquet"
            object_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
            
            
            download_and_upload_to_minio(url, bucket_name, object_name)
#l'instruction précedente télécharger et sauvgarde dans minio et puis affiche le succés de l'opération
            print(f"Fichier {object_name} téléchargé dans MinIO")





def grab_data_last_month() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_" 
    data_folder = "data/raw"
    this_day = datetime.today().date()
    month=this_day.month
    year=this_day.year

    if this_day.month == 1 :
        year=year-1
        month=12
    else :
        month=month-1    
# La vérification que le chemin existe d'abord
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    url = f"{base_url}{year}-{month:02d}.parquet"  
    filename = f"{data_folder}/yellow_tripdata_{year}-{month}.parquet"
    
    # Téléchargement du fichier  et ensuite l'affichage du texte
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    print(f"Fichier téléchargé : {filename}")





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
    bucket: str = "NOM_DU_BUCKET_ICI"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    sys.exit(main())