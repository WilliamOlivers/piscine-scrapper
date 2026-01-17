import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor

# --- CONFIGURATION ---
HORAIRES = {
    "Monday":    [], # Fermé
    "Tuesday":   [("11:45", "14:00"), ("16:30", "19:00")],
    "Wednesday": [("10:00", "13:45"), ("15:00", "19:00")],
    "Thursday":  [("07:00", "08:30"), ("11:45", "14:00"), ("16:30", "21:30")],
    "Friday":    [("11:45", "14:00"), ("16:30", "19:00")],
    "Saturday":  [("12:00", "18:00")],
    "Sunday":    [("09:00", "13:00"), ("15:00", "18:00")]
}

DAY_MAP = {'Monday':0, 'Tuesday':1, 'Wednesday':2, 'Thursday':3, 'Friday':4, 'Saturday':5, 'Sunday':6}
FR_DAYS = {"Monday":"Lundi", "Tuesday":"Mardi", "Wednesday":"Mercredi", "Thursday":"Jeudi", "Friday":"Vendredi", "Saturday":"Samedi", "Sunday":"Dimanche"}
DAYS_LIST = list(DAY_MAP.keys())

def is_open(day_str, hour_int):
    for start, end in HORAIRES.get(day_str, []):
        sh, sm = map(int, start.split(':'))
        eh, em = map(int, end.split(':'))
        if max(hour_int, sh + sm/60.0) < min(hour_int + 1, eh + em/60.0):
            return True
    return False

# --- 1. CHARGEMENT & ENTRAINEMENT ---
try:
    df = pd.read_csv('historique_piscine.csv')
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['zone'] == 'Ouest') & (df['frequentation'] >= 0)].copy()
    df['day_code'] = df['jour'].map(DAY_MAP)
    df['hour'] = df['heure']

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(df[['day_code', 'hour']], df['frequentation'])

    # --- 2. PREDICTION ---
    best_time = None
    min_freq = float('inf')

    for day in DAYS_LIST:
        d_code = DAY_MAP[day]
        for h in range(24):
            if is_open(day, h):
                pred = model.predict([[d_code, h]])[0]
                if pred < min_freq:
                    min_freq = pred
                    best_time = f"{FR_DAYS[day]} à {h}h"
    
    if not best_time:
        best_time = "Aucun créneau disponible"

except Exception as e:
    best_time = "Données insuffisantes"
    print(f"Error: {e}")

# --- 3. GENERATION DU SITE (DESIGN TEXTURE) ---
html_content = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Piscine Ouest</title>
    <style>
        body, html {{
            height: 100%;
            margin: 0;
            display: flex;
            justify-content: center;
            align-items: center;
            font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
            background-color: #231f1c; /* Marron très sombre/Noir */
            color: #f0f0f0;
            overflow: hidden;
        }}
        
        /* Effet de grain */
        .bg-grain {{
            position: absolute;
            top: 0; left: 0; width: 100%; height: 100%;
            opacity: 0.15;
            pointer-events: none;
            background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 200 200' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noiseFilter'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='3' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noiseFilter)'/%3E%3C/svg%3E");
            z-index: 1;
        }}

        .content {{
            z-index: 2;
            text-align: center;
            padding: 20px;
            font-size: 2rem;
            font-weight: 300;
            letter-spacing: 1px;
            max-width: 800px;
            line-height: 1.4;
        }}

        @media (max-width: 600px) {{
            .content {{ font-size: 1.5rem; }}
        }}
    </style>
</head>
<body>
    <div class="bg-grain"></div>
    <div class="content">
        Le meilleur moment pour aller à la piscine cette semaine est :<br>
        <strong>{best_time}</strong>
    </div>
</body>
</html>"""

with open('index.html', 'w', encoding='utf-8') as f:
    f.write(html_content)

print("Site updated successfully.")
