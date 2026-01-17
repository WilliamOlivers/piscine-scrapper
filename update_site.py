import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from datetime import datetime
import pytz

# --- CONFIGURATION ---
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

# --- 1. CALCUL DU TOP 3 ---
top_3_html = ""
update_time = "..."

try:
    tz = pytz.timezone('Europe/Paris')
    now = datetime.now(tz)
    update_time = now.strftime("%H:%M")

    df = pd.read_csv('historique_piscine.csv')
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['zone'] == 'Ouest') & (df['frequentation'] >= 0)].copy()
    df['day_code'] = df['jour'].map(DAY_MAP)
    df['hour'] = df['heure']

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(df[['day_code', 'hour']], df['frequentation'])

    predictions = []
    for day in DAYS_LIST:
        d_code = DAY_MAP[day]
        for h in range(24):
            if is_open(day, h):
                pred = model.predict([[d_code, h]])[0]
                predictions.append({
                    'day': day,
                    'hour': h,
                    'val': pred
                })
    
    # Trier par fréquentation (le plus bas d'abord) et prendre les 3 premiers
    predictions.sort(key=lambda x: x['val'])
    top_3 = predictions[:3]

    # Générer les éléments de la liste HTML
    if top_3:
        for i, p in enumerate(top_3):
            # Le premier est "active" par défaut
            active_class = "active" if i == 0 else ""
            time_str = f"{FR_DAYS[p['day']]} {p['hour']}h"
            top_3_html += f'<div class="wheel-item {active_class}">{time_str}</div>'
    else:
        top_3_html = '<div class="wheel-item active">Aucune donnée</div>'

except Exception as e:
    print(f"Error: {e}")
    top_3_html = '<div class="wheel-item active">Erreur</div>'

# --- 2. GENERATION HTML (iOS Scroll Style) ---
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
            background-color: #0f0f10;
            color: #b0b0b0; 
            font-family: 'Space Mono', monospace;
            font-size: 14px;
            letter-spacing: -0.5px;
            overflow: hidden; /* Empêche le scroll global */
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
        }}

        /* --- CONTENEUR PRINCIPAL --- */
        .container {{
            text-align: center;
            width: 100%;
            max-width: 400px;
            height: 80vh; /* Prend la majorité de l'écran */
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            position: relative;
        }}

        .label-text {{ margin-bottom: 20px; font-weight: 400; }}

        /* --- ROUE DE SELECTION (IOS STYLE) --- */
        .wheel-container {{
            height: 150px; /* Hauteur visible de la roue */
            width: 100%;
            overflow-y: scroll;
            scroll-snap-type: y mandatory; /* Force l'arrêt sur les éléments */
            position: relative;
            scrollbar-width: none; /* Cache la scrollbar Firefox */
            -ms-overflow-style: none; /* Cache la scrollbar IE */
            mask-image: linear-gradient(to bottom, transparent, black 40%, black 60%, transparent);
            -webkit-mask-image: linear-gradient(to bottom, transparent, black 40%, black 60%, transparent);
        }}
        
        .wheel-container::-webkit-scrollbar {{ display: none; }} /* Cache scrollbar Chrome */

        /* Espace vide pour centrer le premier et dernier élément */
        .spacer {{ height: 60px; }} 

        .wheel-item {{
            height: 30px;
            line-height: 30px;
            text-align: center;
            font-size: 18px;
            color: #555;
            scroll-snap-align: center; /* L'élément s'arrête au milieu */
            transition: all 0.2s ease;
            cursor: pointer;
        }}

        .wheel-item.active {{
            color: #fff;
            font-size: 20px;
            font-style: italic;
            font-weight: bold;
            transform: scale(1.1);
        }}

        /* --- STATUT (Point vert) --- */
        .status {{
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            font-size: 10px;
            color: #555;
            margin-top: 30px;
        }}
        .dot {{
            width: 6px; height: 6px;
            background-color: #2ecc71;
            border-radius: 50%;
            box-shadow: 0 0 4px #2ecc71;
            animation: blink 2s infinite ease-in-out;
        }}
        @keyframes blink {{
            0% {{ opacity: 1; }} 50% {{ opacity: 0.4; }} 100% {{ opacity: 1; }}
        }}

        /* --- PARTIE EXPLICATION (Bas de page) --- */
        .details-trigger {{
            position: absolute;
            bottom: 20px;
            display: flex;
            flex-direction: column;
            align-items: center;
            cursor: pointer;
            color: #444;
            transition: color 0.3s;
        }}
        .details-trigger:hover {{ color: #888; }}
        
        .arrow {{ font-size: 12px; margin-bottom: 5px; animation: bounce 2s infinite; }}
        .info-text {{ font-size: 10px; text-transform: uppercase; letter-spacing: 1px; }}

        /* --- MODALE EXPLICATION --- */
        .methodology {{
            display: none; /* Caché par défaut */
            position: fixed;
            bottom: 0; left: 0; width: 100%;
            background: #151516;
            border-top: 1px solid #333;
            padding: 30px 20px;
            box-sizing: border-box;
            font-size: 11px;
            line-height: 1.6;
            color: #888;
            text-align: left;
            z-index: 10;
        }}
        .methodology h3 {{ color: #fff; font-size: 12px; margin: 0 0 10px 0; font-weight: normal; }}
        .close-btn {{
            position: absolute; top: 10px; right: 20px;
            font-size: 16px; cursor: pointer; color: #fff;
        }}

        @keyframes bounce {{
            0%, 20%, 50%, 80%, 100% {{transform: translateY(0);}}
            40% {{transform: translateY(-5px);}}
            60% {{transform: translateY(-3px);}}
        }}
    </style>
</head>
<body>

    <div class="container">
        <div class="label-text">le meilleur moment pour nager est :</div>

        <div class="wheel-container" id="wheel">
            <div class="spacer"></div>
            {top_3_html}
            <div class="spacer"></div>
        </div>

        <div class="status">
            <div class="dot"></div>
            <span>mis à jour à {update_time}</span>
        </div>
    </div>

    <div class="details-trigger" onclick="toggleDetails()">
        <div class="arrow">▼</div>
        <span class="info-text">méthodologie</span>
    </div>

    <div class="methodology" id="methPanel">
        <div class="close-btn" onclick="toggleDetails()">✕</div>
        <h3>Logique de prédiction</h3>
        <p>
            Notre algorithme utilise un modèle de <strong>Forêt Aléatoire (Random Forest)</strong> entraîné sur les données historiques de fréquentation de la piscine (Zone Ouest).<br><br>
            Il croise l'historique des affluences avec les horaires d'ouverture réels pour exclure les fermetures. 
            L'hypothèse est que les motifs d'affluence (pics le mercredi après-midi, calme le jeudi soir) se répètent de manière cyclique. 
            Le modèle prédit le nombre de nageurs pour chaque heure de la semaine à venir et classe les créneaux du moins fréquenté au plus fréquenté.
        </p>
    </div>

    <script>
        // --- LOGIQUE SCROLL TYPE IOS ---
        const container = document.getElementById('wheel');
        const items = document.querySelectorAll('.wheel-item');

        container.addEventListener('scroll', () => {{
            const center = container.scrollTop + (container.clientHeight / 2);
            
            items.forEach(item => {{
                const itemCenter = item.offsetTop + (item.clientHeight / 2);
                const distance = Math.abs(center - itemCenter);
                
                // Si l'élément est proche du centre (< 15px), il devient actif
                if (distance < 15) {{
                    item.classList.add('active');
                }} else {{
                    item.classList.remove('active');
                }}
            }});
        }});

        // --- LOGIQUE MODALE ---
        function toggleDetails() {{
            const panel = document.getElementById('methPanel');
            const trigger = document.querySelector('.details-trigger');
            
            if (panel.style.display === 'block') {{
                panel.style.display = 'none';
                trigger.style.display = 'flex';
            }} else {{
                panel.style.display = 'block';
                trigger.style.display = 'none';
            }}
        }}
    </script>
</body>
</html>"""

with open('index.html', 'w', encoding='utf-8') as f:
    f.write(html_content)
