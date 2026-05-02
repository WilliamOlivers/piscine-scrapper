import json
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
SESSION_MIN = 45


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
chart_data_json = "{}"

try:
    tz = pytz.timezone('Europe/Paris')
    now = datetime.now(tz)
    update_time = now.strftime("%d/%m/%Y à %H:%M")

    df = pd.read_csv('historique_piscine.csv')
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['zone'] == 'Ouest'].copy()
    df = df[df['frequentation'] >= 0]

    df['slot_info'] = df.apply(lambda r: get_slot_info(r['jour'], r['heure']), axis=1)
    df = df[df['slot_info'].notna()].copy()
    df['mins_since_open'] = df['slot_info'].apply(lambda x: x[0])
    df['mins_before_close'] = df['slot_info'].apply(lambda x: x[1])
    df['day_code'] = df['jour'].map(DAY_MAP)
    df['month'] = df['date'].dt.month

    # --- 2. ENTRAÎNEMENT DU MODÈLE ---
    features = ['day_code', 'heure', 'mins_since_open', 'mins_before_close', 'month']
    model = RandomForestRegressor(n_estimators=200, random_state=42)
    model.fit(df[features], df['frequentation'])

    # --- 3. PRÉDICTIONS ET PROFILS PAR JOUR ---
    current_month = now.month
    predictions = []

    # Profil complet par jour (toutes les heures valides, pour le graphe)
    day_profiles = {}
    for day in DAYS_LIST:
        profile = []
        for h in range(24):
            if get_slot_info(day, h) is not None:
                mask = (df['jour'] == day) & (df['heure'] == h)
                slot_data = df.loc[mask, 'frequentation']
                n = len(slot_data)
                med = round(float(slot_data.median()), 1) if n > 0 else 0
                profile.append({"h": h, "med": med, "n": n})
        day_profiles[day] = profile

    for day in DAYS_LIST:
        d_code = DAY_MAP[day]
        for h in range(24):
            info = get_slot_info(day, h)
            if info is None:
                continue
            mins_since, mins_before = info
            pred = model.predict(pd.DataFrame(
                [[d_code, h, mins_since, mins_before, current_month]], columns=features
            ))[0]

            mask = (df['jour'] == day) & (df['heure'] == h)
            slot_data = df.loc[mask, 'frequentation']
            n_obs = len(slot_data)
            empirical_median = slot_data.median() if n_obs > 0 else pred

            weight_empirical = min(n_obs / 15, 1.0)
            score = weight_empirical * empirical_median + (1 - weight_empirical) * pred

            predictions.append({
                'day': day,
                'hour': h,
                'score': score,
                'median': round(float(empirical_median), 1),
                'n_obs': n_obs,
            })

    predictions.sort(key=lambda x: x['score'])
    top_3 = predictions[:3]

    # --- 4. DONNÉES POUR LE GRAPHE ---
    chart_data = {}
    for p in top_3:
        key = f"{FR_DAYS[p['day']]} {p['hour']}h"
        chart_data[key] = {
            "median": p['median'],
            "n_obs": p['n_obs'],
            "selected_hour": p['hour'],
            "profile": day_profiles[p['day']],
        }
    chart_data_json = json.dumps(chart_data, ensure_ascii=False)

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

# --- 5. GÉNÉRATION HTML ---
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
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}

        body, html {{
            height: 100%;
            background-color: #0f0f10;
            color: #b0b0b0;
            font-family: 'Space Mono', monospace;
            font-size: 14px;
            letter-spacing: -0.5px;
            overflow: hidden;
        }}

        /* Roue plein écran — reçoit les touches nativement */
        .wheel-container {{
            position: fixed;
            top: 0; left: 0;
            width: 100%; height: 100%;
            overflow-x: hidden;
            overflow-y: scroll;
            scroll-snap-type: y mandatory;
            touch-action: pan-y;
            scrollbar-width: none;
            -ms-overflow-style: none;
            mask-image: linear-gradient(to bottom, transparent 0%, black 36%, black 64%, transparent 100%);
            -webkit-mask-image: linear-gradient(to bottom, transparent 0%, black 36%, black 64%, transparent 100%);
            z-index: 1;
        }}
        .wheel-container::-webkit-scrollbar {{ display: none; }}

        /* Spacer = demi-écran moins demi-item → premier item centré au scroll 0 */
        .spacer {{ height: calc(50vh - 15px); }}

        .wheel-item {{
            height: 30px;
            line-height: 30px;
            text-align: center;
            font-size: 18px;
            color: #555;
            scroll-snap-align: center;
            transition: color 0.2s ease, font-size 0.2s ease, transform 0.2s ease;
            cursor: pointer;
        }}
        .wheel-item.active {{
            color: #fff;
            font-size: 20px;
            font-style: italic;
            font-weight: bold;
            transform: scale(1.1);
        }}

        /* Overlay UI — par dessus la roue, ne capture pas les touches */
        .ui-overlay {{
            position: fixed;
            top: 0; left: 0;
            width: 100%; height: 100%;
            display: flex;
            flex-direction: column;
            align-items: center;
            pointer-events: none;
            z-index: 2;
        }}

        .pool-title {{
            margin-top: 10vh;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 2px;
            color: #b0b0b0;
        }}

        .label-text {{
            margin-top: 28px;
            font-weight: 400;
        }}

        /* Graphe poussé en bas de l'overlay */
        .chart-panel {{
            margin-top: auto;
            margin-bottom: 14vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 10px;
            opacity: 0;
            transform: translateY(6px);
            transition: opacity 0.35s ease, transform 0.35s ease;
        }}
        .chart-panel.visible {{
            opacity: 1;
            transform: translateY(0);
        }}
        .chart-bars {{
            display: flex;
            align-items: flex-end;
            gap: 5px;
            height: 55px;
        }}
        .bar-wrap {{
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 5px;
        }}
        .bar {{
            width: 16px;
            border-radius: 2px 2px 0 0;
            transition: background 0.3s ease, height 0.4s ease;
            min-height: 2px;
        }}
        .bar-hour {{
            font-size: 8px;
            color: #333;
            transition: color 0.3s ease;
        }}
        .bar-hour.active-label {{ color: #888; }}
        .chart-stat {{
            font-size: 9px;
            color: #3a3a3a;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        .chart-stat span {{ color: #666; }}

        /* Statut discret */
        .status {{
            position: fixed;
            bottom: 14px;
            right: 16px;
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 8px;
            color: #2a2a2a;
            z-index: 3;
        }}
        .dot {{
            width: 4px; height: 4px;
            background-color: #2ecc71;
            border-radius: 50%;
            box-shadow: 0 0 3px #2ecc71;
            animation: blink 2s infinite ease-in-out;
        }}
        @keyframes blink {{
            0% {{ opacity: 1; }} 50% {{ opacity: 0.4; }} 100% {{ opacity: 1; }}
        }}

        /* Méthodologie */
        .details-trigger {{
            position: fixed;
            bottom: 20px;
            left: 50%;
            transform: translateX(-50%);
            display: flex;
            flex-direction: column;
            align-items: center;
            cursor: pointer;
            color: #444;
            transition: color 0.3s;
            pointer-events: auto;
            z-index: 3;
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
            0%, 20%, 50%, 80%, 100% {{transform: translateX(-50%) translateY(0);}}
            40% {{transform: translateX(-50%) translateY(-5px);}}
            60% {{transform: translateX(-50%) translateY(-3px);}}
        }}
    </style>
</head>
<body>

    <!-- Roue plein écran : reçoit toutes les touches nativement -->
    <div class="wheel-container" id="wheel">
        <div class="spacer"></div>
        {top_3_html}
        <div class="spacer"></div>
    </div>

    <!-- Overlay UI : titre, label, graphe -->
    <div class="ui-overlay">
        <div class="pool-title">piscine judaïque · bordeaux</div>
        <div class="label-text">le meilleur moment pour nager est :</div>
        <div class="chart-panel" id="chartPanel">
            <div class="chart-bars" id="chartBars"></div>
            <div class="chart-stat" id="chartStat"></div>
        </div>
    </div>

    <div class="status">
        <div class="dot"></div>
        <span>màj {update_time}</span>
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
        const CHART_DATA = {chart_data_json};

        const wheelEl = document.getElementById('wheel');
        const items = document.querySelectorAll('.wheel-item');
        const chartPanel = document.getElementById('chartPanel');
        const chartBars = document.getElementById('chartBars');
        const chartStat = document.getElementById('chartStat');

        let activeLabel = null;

        function renderChart(label) {{
            if (label === activeLabel) return;
            activeLabel = label;
            const data = CHART_DATA[label];
            if (!data || !data.profile.length) {{ chartPanel.classList.remove('visible'); return; }}

            const maxVal = Math.max(...data.profile.map(p => p.med), 1);
            chartBars.innerHTML = data.profile.map(p => {{
                const isSelected = p.h === data.selected_hour;
                const barH = Math.max(2, Math.round((p.med / maxVal) * 50));
                return `<div class="bar-wrap">
                    <div class="bar" style="height:${{barH}}px;background:${{isSelected ? '#c8c8c8' : '#222'}}"></div>
                    <div class="bar-hour ${{isSelected ? 'active-label' : ''}}">${{p.h}}h</div>
                </div>`;
            }}).join('');

            chartStat.innerHTML = data.median === 0
                ? `<span>généralement vide</span> · ${{data.n_obs}} visites observées`
                : `<span>le plus souvent ~${{data.median}} nageurs</span> · ${{data.n_obs}} visites observées`;

            chartPanel.classList.add('visible');
        }}

        wheelEl.addEventListener('scroll', () => {{
            const center = wheelEl.scrollTop + wheelEl.clientHeight / 2;
            items.forEach(item => {{
                const dist = Math.abs((item.offsetTop + item.clientHeight / 2) - center);
                if (dist < 15) {{
                    item.classList.add('active');
                    renderChart(item.textContent.trim());
                }} else {{
                    item.classList.remove('active');
                }}
            }});
        }});

        // Clic sur un item pour le centrer
        items.forEach(item => {{
            item.addEventListener('click', () => {{
                const target = item.offsetTop - wheelEl.clientHeight / 2 + item.clientHeight / 2;
                wheelEl.scrollTo({{ top: target, behavior: 'smooth' }});
            }});
        }});

        const firstActive = document.querySelector('.wheel-item.active');
        if (firstActive) renderChart(firstActive.textContent.trim());

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
