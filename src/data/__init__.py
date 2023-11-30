import requests
from bs4 import BeautifulSoup
import re
import os

# URL de la page contenant les liens vers les datasets
url = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Envoyer une requête HTTP pour récupérer le contenu de la page
response = requests.get(url)

if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')

    # Utiliser BeautifulSoup pour extraire les liens correspondant au modèle d'URL
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet"
    # Utilisez une liste de dates spécifiques de "2018-01" à "2023-08"
    dates = ["20{:02d}-{:02d}".format(year, month) for year in range(23, 24) for month in range(1, 9)]
    links = [base_url.format(date) for date in dates]

    # Dossier de destination pour enregistrer les fichiers téléchargés
    destination_folder = "../data/raw/"

    # Assurez-vous que le dossier de destination existe
    os.makedirs(destination_folder, exist_ok=True)

    for link in links:
        # Générer le nom de fichier en utilisant la date dans l'URL
        filename = link.split("/")[-1]

        # Chemin complet du fichier de destination
        file_path = os.path.join(destination_folder, filename)

        # Tester si le lien est accessible
        link_response = requests.head(link)

        if link_response.status_code == 200:
            # Le lien est accessible, téléchargez le fichier
            file_response = requests.get(link)
            with open(file_path, 'wb') as file:
                file.write(file_response.content)
            print(f"Téléchargement réussi : {filename}")
        else:
            # Le lien n'est pas accessible
            print(f"Échec du téléchargement pour : {filename}. Lien inaccessible.")

    print("Tous les fichiers ont été téléchargés.")

else:
    print(f"Échec de la récupération de la page web. Code d'état : {response.status_code}")