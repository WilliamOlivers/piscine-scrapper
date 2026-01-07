import requests
import pandas as pd
import os
from datetime import datetime

# Nom du fichier de sauvegarde
CSV_FILE = "historique_piscine.csv"
BASE_URL = "https://datahub.bordeaux-metropole.fr/api/records/1.0/search/"
PARAMS = {
    "dataset": "bor_frequentation_piscine_tr",
    "rows": 20,
    "q": ""
}

def job():
    try:
        response = requests.get(BASE_URL, params=PARAMS)
        data = response.json()
        
        timestamp = datetime.now()
        lignes = []

        if data.get('nhits', 0) > 0:
            records = [r['fields'] for r in data['records']]
            df = pd.DataFrame(records)
            
            # Filtre pour Judaïque
            mask = df['etablissement_etalib'].astype(str).str.contains("Juda|Boiteux", case=False, na=False)
            resultat = df[mask]

            for _, row in resultat.iterrows():
                fmy = row.get('fmizonmax', 1)
                freq = row.get('fmicourante', 0)
                taux = round(freq / fmy * 100, 2) if fmy > 0 else 0
                
                lignes.append({
                    "date": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "jour": timestamp.strftime("%A"),
                    "heure": timestamp.hour,
                    "zone": row.get('fmizonlib'),
                    "frequentation": freq,
                    "capacite": fmy,
                    "taux": taux
                })

        # Sauvegarde
        if lignes:
            df_new = pd.DataFrame(lignes)
            # Si le fichier existe, on ajoute sans en-tête, sinon on crée avec
            if os.path.exists(CSV_FILE):
                df_new.to_csv(CSV_FILE, mode='a', header=False, index=False)
            else:
                df_new.to_csv(CSV_FILE, mode='w', header=True, index=False)
            print("Données sauvegardées.")

    except Exception as e:
        print(f"Erreur : {e}")

if __name__ == "__main__":
    job()
