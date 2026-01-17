import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor

# --- CONFIGURATION DES HORAIRES ---
HORAIRES = {
    "Monday":    [], 
    "Tuesday":   [("11:45", "14:00"), ("16:30", "19:00")],
    "Wednesday": [("10:00", "13:45"), ("15:00", "19:00")],
    "Thursday":  [("07:00", "08:30"), ("11:45", "14:00"), ("16:30", "21:30")],
    "Friday":    [("11:45", "14:00"), ("16:30", "19:00")],
    "Saturday":  [("12:00", "18:00")],
    "Sunday":    [("09:00", "13:00"), ("15:00", "18:00")]
}

DAY_MAP = {'Monday':0, 'Tuesday':1, 'Wednesday':2, 'Thursday':3, 'Friday':4, 'Saturday':5, 'Sunday':6}
FR_DAYS = {"Monday":"lundi", "Tuesday":"mardi", "Wednesday":"mercredi", "Thursday":"jeudi", "Friday":"vendredi", "Saturday":"samedi", "Sunday":"dimanche"}
DAYS_LIST = list(DAY_MAP.keys())

def is_open(day_str, hour_int):
    for start, end in HORAIRES.get(day_str, []):
        sh, sm = map(int, start.split(':'))
        eh, em = map(int, end.split(':'))
        if max(hour_int, sh + sm/60.0) < min(hour_int + 1, eh + em/60.0):
            return True
    return False

best_time_str = "..."

try:
    df = pd.read_csv('historique_piscine.csv')
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['zone'] == 'Ouest') & (df['frequentation'] >= 0)].copy()
    df['day_code'] = df['jour'].map(DAY_MAP)
    df['hour'] = df['heure']

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(df[['day_code', 'hour']], df['frequentation'])

    min_freq = float('inf')
    best_day = None
    best_hour = None

    for day in DAYS_LIST:
        d_code = DAY_MAP[day]
        for h in range(24):
            if is_open(day, h):
                pred = model.predict([[d_code, h]])[0]
                if pred < min_freq:
                    min_freq = pred
                    best_day = day
                    best_hour = h
    
    if best_day:
        best_time_str = f"{FR_DAYS[best_day]} {best_hour}h"
    else:
        best_time_str = "aucun créneau"

except Exception:
    best_time_str = "données indisponibles"

html_content = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Piscine Ouest</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Space+Mono:ital,wght@0,400;1,400&display=swap" rel="stylesheet">
    
    <style>
        body, html {{
            height: 100%;
            margin: 0;
            display: flex;
            justify-content: center;
            align-items: center;
            background-color: #0f0f10; /* Dark Gray Night */
            color: #b0b0b0; /* Gris clair doux */
            font-family: 'Space Mono', monospace; /* La police style Grok */
            font-size: 14px; /* Petit texte */
            letter-spacing: -0.5px;
        }}

        .container {{
            text-align: center;
            max-width: 600px;
            padding: 20px;
        }}

        .text {{
            font-weight: 400;
        }}

        .prediction {{
            font-style: italic;
            color: #ffffff;
        }}
    </style>
</head>
<body>
    <div class="container">
        <span class="text">le meilleur moment pour aller à la piscine cette semaine est : </span>
        <span class="prediction">{best_time_str}</span>
    </div>
</body>
</html>"""

with open('index.html', 'w', encoding='utf-8') as f:
    f.write(html_content)
