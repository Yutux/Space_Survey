"""
generate_planets.py
───────────────────
Génère un jeu de données pour 15 planètes fictives et sauvegarde
le résultat dans planets.csv.

Caractéristiques générées :
  - name              : nom de la planète
  - radius_km         : rayon en km
  - distance_au       : distance au soleil en unités astronomiques
  - mass_earth        : masse relative à la Terre
  - gravity_g         : gravité relative (1.0 = Terre)
  - avg_temp_celsius  : température moyenne
  - atmosphere_o2     : % d'oxygène dans l'atmosphère
  - atmosphere_co2    : % de CO2
  - atmosphere_n2     : % d'azote
  - has_water         : présence d'eau liquide (bool)
  - magnetic_field    : champ magnétique (bool)
  - moons             : nombre de lunes
  - label             : étiquette manuelle d'habitabilité
                        (pour entraîner le modèle ML)
"""

import pandas as pd
import random

random.seed(42)

# ─── Données manuellement définies pour 15 planètes fictives ──────────────────
planets = [
    {
        "name": "Aeloria",
        "radius_km": 6800,
        "distance_au": 1.1,
        "mass_earth": 1.05,
        "gravity_g": 1.02,
        "avg_temp_celsius": 18,
        "atmosphere_o2": 21,
        "atmosphere_co2": 1,
        "atmosphere_n2": 77,
        "has_water": True,
        "magnetic_field": True,
        "moons": 1,
        "label": "habitable",
    },
    {
        "name": "Vexar Prime",
        "radius_km": 3200,
        "distance_au": 0.4,
        "mass_earth": 0.3,
        "gravity_g": 0.38,
        "avg_temp_celsius": 430,
        "atmosphere_o2": 0,
        "atmosphere_co2": 96,
        "atmosphere_n2": 3,
        "has_water": False,
        "magnetic_field": False,
        "moons": 0,
        "label": "non_habitable",
    },
    {
        "name": "Zyphos",
        "radius_km": 7200,
        "distance_au": 1.3,
        "mass_earth": 1.2,
        "gravity_g": 1.1,
        "avg_temp_celsius": 12,
        "atmosphere_o2": 18,
        "atmosphere_co2": 2,
        "atmosphere_n2": 79,
        "has_water": True,
        "magnetic_field": True,
        "moons": 2,
        "label": "habitable",
    },
    {
        "name": "Drakkon IV",
        "radius_km": 65000,
        "distance_au": 5.2,
        "mass_earth": 318,
        "gravity_g": 2.53,
        "avg_temp_celsius": -145,
        "atmosphere_o2": 0,
        "atmosphere_co2": 0,
        "atmosphere_n2": 0,
        "has_water": False,
        "magnetic_field": True,
        "moons": 79,
        "label": "non_habitable",
    },
    {
        "name": "Nexara",
        "radius_km": 5900,
        "distance_au": 0.95,
        "mass_earth": 0.85,
        "gravity_g": 0.92,
        "avg_temp_celsius": 25,
        "atmosphere_o2": 19,
        "atmosphere_co2": 3,
        "atmosphere_n2": 76,
        "has_water": True,
        "magnetic_field": True,
        "moons": 0,
        "label": "habitable",
    },
    {
        "name": "Cryon",
        "radius_km": 2400,
        "distance_au": 38,
        "mass_earth": 0.1,
        "gravity_g": 0.07,
        "avg_temp_celsius": -220,
        "atmosphere_o2": 0,
        "atmosphere_co2": 0,
        "atmosphere_n2": 95,
        "has_water": False,
        "magnetic_field": False,
        "moons": 1,
        "label": "non_habitable",
    },
    {
        "name": "Theriax",
        "radius_km": 6100,
        "distance_au": 1.5,
        "mass_earth": 0.9,
        "gravity_g": 0.88,
        "avg_temp_celsius": -5,
        "atmosphere_o2": 12,
        "atmosphere_co2": 5,
        "atmosphere_n2": 80,
        "has_water": True,
        "magnetic_field": True,
        "moons": 1,
        "label": "inconnue",
    },
    {
        "name": "Pyroxis",
        "radius_km": 8900,
        "distance_au": 0.2,
        "mass_earth": 2.1,
        "gravity_g": 1.8,
        "avg_temp_celsius": 900,
        "atmosphere_o2": 0,
        "atmosphere_co2": 70,
        "atmosphere_n2": 10,
        "has_water": False,
        "magnetic_field": False,
        "moons": 0,
        "label": "non_habitable",
    },
    {
        "name": "Lumivara",
        "radius_km": 6400,
        "distance_au": 1.0,
        "mass_earth": 1.0,
        "gravity_g": 1.0,
        "avg_temp_celsius": 15,
        "atmosphere_o2": 21,
        "atmosphere_co2": 0,
        "atmosphere_n2": 78,
        "has_water": True,
        "magnetic_field": True,
        "moons": 1,
        "label": "habitable",
    },
    {
        "name": "Sorvek",
        "radius_km": 4100,
        "distance_au": 2.1,
        "mass_earth": 0.4,
        "gravity_g": 0.45,
        "avg_temp_celsius": -60,
        "atmosphere_o2": 3,
        "atmosphere_co2": 50,
        "atmosphere_n2": 40,
        "has_water": False,
        "magnetic_field": False,
        "moons": 2,
        "label": "non_habitable",
    },
    {
        "name": "Echovell",
        "radius_km": 7000,
        "distance_au": 1.2,
        "mass_earth": 1.15,
        "gravity_g": 1.05,
        "avg_temp_celsius": 22,
        "atmosphere_o2": 20,
        "atmosphere_co2": 1,
        "atmosphere_n2": 78,
        "has_water": True,
        "magnetic_field": True,
        "moons": 3,
        "label": "habitable",
    },
    {
        "name": "Glacious",
        "radius_km": 5500,
        "distance_au": 9.5,
        "mass_earth": 0.7,
        "gravity_g": 0.65,
        "avg_temp_celsius": -180,
        "atmosphere_o2": 1,
        "atmosphere_co2": 2,
        "atmosphere_n2": 90,
        "has_water": True,   # eau gelée sous la surface
        "magnetic_field": False,
        "moons": 0,
        "label": "inconnue",
    },
    {
        "name": "Ashenveil",
        "radius_km": 3800,
        "distance_au": 0.7,
        "mass_earth": 0.55,
        "gravity_g": 0.6,
        "avg_temp_celsius": 310,
        "atmosphere_o2": 0,
        "atmosphere_co2": 85,
        "atmosphere_n2": 14,
        "has_water": False,
        "magnetic_field": False,
        "moons": 0,
        "label": "non_habitable",
    },
    {
        "name": "Verdantis",
        "radius_km": 7500,
        "distance_au": 1.4,
        "mass_earth": 1.3,
        "gravity_g": 1.15,
        "avg_temp_celsius": 28,
        "atmosphere_o2": 23,
        "atmosphere_co2": 0,
        "atmosphere_n2": 76,
        "has_water": True,
        "magnetic_field": True,
        "moons": 2,
        "label": "habitable",
    },
    {
        "name": "Stygion",
        "radius_km": 58000,
        "distance_au": 19.2,
        "mass_earth": 14.5,
        "gravity_g": 0.9,
        "avg_temp_celsius": -195,
        "atmosphere_o2": 0,
        "atmosphere_co2": 0,
        "atmosphere_n2": 0,
        "has_water": False,
        "magnetic_field": True,
        "moons": 27,
        "label": "non_habitable",
    },
]

df = pd.DataFrame(planets)

# Conversion booléens → int pour Spark ML
df["has_water_int"] = df["has_water"].astype(int)
df["magnetic_field_int"] = df["magnetic_field"].astype(int)

df.to_csv("planets.csv", index=False)
print(f"✅ {len(df)} planètes générées → planets.csv")
print(df[["name", "avg_temp_celsius", "atmosphere_o2", "has_water", "label"]].to_string())