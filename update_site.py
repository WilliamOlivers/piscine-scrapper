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

DAY_MAP = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3,
           'Friday': 4, 'Saturday': 5, 'Sunday': 6}
FR_DAYS = {"Monday": "lundi", "Tuesday": "mardi", "Wednesday": "mercredi",
           "Thursday": "jeudi", "Friday": "vendredi", "Saturday": "samedi",
           "Sunday": "dimanche"}
DAYS_LIST = list(DAY_MAP.keys())
SESSION_MIN = 45  # durée minimale d'une séance en minutes


def get_slot_info(day_str, hour_int):
    """Retourne (minutes_depuis_ouverture, minutes_avant_fermeture) ou None si invalide."""
    for start, end in HORAIRES.get(day_str, []):
        sh, sm = map(int, start.split(':'))
        eh, em = map(int, end.split(':'))
        open_mins = sh * 60 + sm
        close_mins = eh * 60 + em
        slot_start = hour_int * 60
        if slot_start >= open_mins and slot_start + SESSION_MIN <= close_mins:
            return slot_start - open_mins, close_mins - slot_start
    return None


# --- 1. CHARGEMENT ET NETTOYAGE DES DONNÉES ---
top_3_html = ""
update_time = "..."

try:
    tz = pytz.timezone('Europe/Paris')
    now = datetime.now(tz)
    update_time = now.strftime("%d/%m/%Y à %H:%M")

    df = pd.read_csv('historique_piscine.csv')
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['zone'] == 'Ouest'].copy()

    # Supprimer les valeurs aberrantes (négatifs = bug API source)
    df = df[df['frequentation'] >= 0]

    # Garder uniquement les lignes pendant les heures d'ouverture valides (45min rule)
    df['slot_info'] = df.apply(lambda r: get_slot_info(r['jour'], r['heure']), axis=1)
    df = df[df['slot_info'].notna()].copy()
    df['mins_since_open'] = df['slot_info'].apply(lambda x: x[0])
    df['mins_before_close'] = df['slot_info'].apply(lambda x: x[1])
    df['day_code'] = df['jour'].map(DAY_MAP)
    df['month'] = df['date'].dt.month

    # --- 2. ENTRAÎNEMENT DU MODÈLE ---
    features = ['day_code', 'heure', 'mins_since_open', 'mins_before_close', 'month']
    X = df[features]
    y = df['frequentation']

    model = RandomForestRegressor(n_estimators=200, random_state=42)
    model.fit(X, y)

    # --- 3. PRÉDICTIONS SUR TOUS LES CRÉNEAUX VALIDES ---
    # On prédit pour le mois courant (saisonnalité)
    current_month = now.month
    predictions = []

    for day in DAYS_LIST:
        d_code = DAY_MAP[day]
        for h in range(24):
            info = get_slot_info(day, h)
            if info is None:
                continue
            mins_since, mins_before = info
            pred = model.predict(pd.DataFrame([[d_code, h, mins_since, mins_before, current_month]], columns=features))[0]

            # Statistiques empiriques sur ce créneau (médiane + nb observations)
            mask = (df['jour'] == day) & (df['heure'] == h)
            slot_data = df.loc[mask, 'frequentation']
            n_obs = len(slot_data)
            empirical_median = slot_data.median() if n_obs > 0 else pred

            # Score final : mélange médiane empirique et prédiction ML
            # Si peu d'observations, on fait davantage confiance au modèle
            weight_empirical = min(n_obs / 15, 1.0)  # confiance max à 15 obs
            score = weight_empirical * empirical_median + (1 - weight_empirical) * pred

            predictions.append({
                'day': day,
                'hour': h,
                'score': score,
                'pred_rf': round(pred, 1),
                'median': round(empirical_median, 1),
                'n_obs': n_obs,
            })

    predictions.sort(key=lambda x: x['score'])
    top_3 = predictions[:3]

    if top_3:
        for i, p in enumerate(top_3):
            active_class = "active" if i == 0 else ""
            time_str = f"{FR_DAYS[p['day']]} {p['hour']}h"
            top_3_html += f'<div class="wheel-item {active_class}">{time_str}</div>'
    else:
        top_3_html = '<div class="wheel-item active">Aucune donnée</div>'

except Exception as e:
    print(f"Error: {e}")
    top_3_html = '<div class="wheel-item active">Erreur</div>'

# --- 4. GÉNÉRATION HTML ---
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
            overflow: hidden;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
        }}

        .container {{
            text-align: center;
            width: 100%;
            max-width: 400px;
            height: 80vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            position: relative;
        }}

        .label-text {{ margin-bottom: 20px; font-weight: 400; }}

        .wheel-container {{
            height: 150px;
            width: 100%;
            overflow-y: scroll;
            scroll-snap-type: y mandatory;
            position: relative;
            scrollbar-width: none;
            -ms-overflow-style: none;
            mask-image: linear-gradient(to bottom, transparent, black 40%, black 60%, transparent);
            -webkit-mask-image: linear-gradient(to bottom, transparent, black 40%, black 60%, transparent);
        }}

        .wheel-container::-webkit-scrollbar {{ display: none; }}

        .spacer {{ height: 60px; }}

        .wheel-item {{
            height: 30px;
            line-height: 30px;
            text-align: center;
            font-size: 18px;
            color: #555;
            scroll-snap-align: center;
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

        .methodology {{
            display: none;
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
            L'algorithme utilise un modèle de <strong>Forêt Aléatoire</strong> entraîné uniquement sur les données collectées pendant les heures d'ouverture réelles.<br><br>
            Les données aberrantes (valeurs négatives, heures de fermeture) sont exclues de l'entraînement.
            La règle des <strong>45 minutes</strong> garantit que chaque créneau proposé laisse assez de temps pour une séance complète avant la fermeture.<br><br>
            Le modèle intègre quatre dimensions : jour de la semaine, heure, temps écoulé depuis l'ouverture du créneau, et mois de l'année (saisonnalité).
            Le score final combine la médiane empirique observée et la prédiction du modèle, pondérées par le nombre d'observations disponibles pour ce créneau.
        </p>
    </div>

    <script>
        const container = document.getElementById('wheel');
        const items = document.querySelectorAll('.wheel-item');

        container.addEventListener('scroll', () => {{
            const center = container.scrollTop + (container.clientHeight / 2);

            items.forEach(item => {{
                const itemCenter = item.offsetTop + (item.clientHeight / 2);
                const distance = Math.abs(center - itemCenter);
                if (distance < 15) {{
                    item.classList.add('active');
                }} else {{
                    item.classList.remove('active');
                }}
            }});
        }});

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

print("Site généré.")
