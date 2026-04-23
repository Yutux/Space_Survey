"""
generate_systems.py  (v2 — Contraintes astrophysiques réelles)
───────────────────────────────────────────────────────────────
Génère des systèmes stellaires fictifs physiquement cohérents.

Contraintes réelles prises en compte :
  ── Étoile ──────────────────────────────────────────────────
  - Flux stellaire (luminosité / distance²) → radiations de surface
  - Type spectral → UV/XUV stripping atmosphérique
  - Âge de l'étoile → stabilité / activité stellaire
  - Étoiles naines rouges : flares UV fréquents → danger même en ZH
  - Géantes : courte durée de vie → pas le temps pour la vie
  - Naines blanches : résidu stellaire, ZH très proche, marées extrêmes

  ── Orbite ──────────────────────────────────────────────────
  - Zone habitable calculée par flux (modèle Kopparapu)
  - Verrouillage marée (tidal locking) si trop proche → rotation synchrone
  - Excentricité orbitale (simplifiée)

  ── Planète ─────────────────────────────────────────────────
  - Température d'équilibre (albédo + flux stellaire)
  - Gravité → rétention atmosphérique (< 0.4g → fuite atmosphérique)
  - Champ magnétique → bouclier contre le vent solaire
  - Eau liquide possible selon T et pression
  - Composition atmosphérique (O2, CO2, N2)
  - Activité tectonique / sismique → renouvellement atmosphérique
  - Type planétaire → éliminatoires directs (gazeuse, lave, etc.)

  ── Label final ─────────────────────────────────────────────
  habitable    : toutes conditions réunies
  inconnue     : conditions partielles, incertain
  non_habitable: au moins une condition éliminatoire

Fichiers produits :
  - star_systems.csv
  - planets.csv
"""

import pandas as pd
import math
import random

random.seed(42)

# ─── TYPES D'ÉTOILES ─────────────────────────────────────────────────────────
# xuv_factor : activité UV/X relative (1.0 = Soleil)
# flare_risk : probabilité de flares destructeurs (0→1)
# tidal_lock_au : distance en deçà de laquelle la planète est verrouillée
STAR_TYPES = {
    "Naine rouge": {
        "luminosity": 0.01, "temp_k": 3200, "mass": 0.3,
        "lifespan_gy": 100, "color": "rouge",
        "xuv_factor": 10.0,   # très actives en XUV surtout jeunes
        "flare_risk": 0.6,    # flares fréquents et intenses
        "tidal_lock_au": 0.3, # verrouillage marée jusqu'à 0.3 UA
        "min_age_gy": 2.0,    # doit être âgée pour se calmer
    },
    "Naine orange": {
        "luminosity": 0.3, "temp_k": 4500, "mass": 0.7,
        "lifespan_gy": 30, "color": "orange",
        "xuv_factor": 2.0,
        "flare_risk": 0.2,
        "tidal_lock_au": 0.15,
        "min_age_gy": 1.0,
    },
    "Naine jaune": {
        "luminosity": 1.0, "temp_k": 5778, "mass": 1.0,
        "lifespan_gy": 10, "color": "jaune",
        "xuv_factor": 1.0,
        "flare_risk": 0.1,
        "tidal_lock_au": 0.05,
        "min_age_gy": 1.0,
    },
    "Naine blanche": {
        "luminosity": 0.001, "temp_k": 25000, "mass": 0.6,
        "lifespan_gy": 999, "color": "blanche",
        "xuv_factor": 50.0,   # rayonnement UV extrême résiduel
        "flare_risk": 0.0,
        "tidal_lock_au": 0.5,
        "min_age_gy": 0,
        # Note : ZH existe mais trop proche → marées + UV → non habitable pratiquement
    },
    "Geante bleue": {
        "luminosity": 50000, "temp_k": 30000, "mass": 20.0,
        "lifespan_gy": 0.01, "color": "bleue",
        "xuv_factor": 100000.0,  # irradiation létale
        "flare_risk": 0.0,
        "tidal_lock_au": 0.0,
        "min_age_gy": 0,
        # Trop courte vie → vie impossible
    },
    "Geante rouge": {
        "luminosity": 1000, "temp_k": 3700, "mass": 5.0,
        "lifespan_gy": 0.1, "color": "rouge",
        "xuv_factor": 5.0,
        "flare_risk": 0.05,
        "tidal_lock_au": 0.0,
        "min_age_gy": 0,
        # Phase courte, engloutit les planètes internes
    },
    "Sous-geante": {
        "luminosity": 3.0, "temp_k": 6000, "mass": 1.5,
        "lifespan_gy": 5, "color": "jaune",
        "xuv_factor": 1.5,
        "flare_risk": 0.08,
        "tidal_lock_au": 0.05,
        "min_age_gy": 1.0,
    },
}

# ─── TYPES DE PLANÈTES ───────────────────────────────────────────────────────
PLANET_TYPES = {
    "Tellurique":     {"base_albedo": 0.30, "habitable_candidate": True,  "min_gravity": 0.3},
    "Super-Terre":    {"base_albedo": 0.35, "habitable_candidate": True,  "min_gravity": 0.5},
    "Ocean":          {"base_albedo": 0.20, "habitable_candidate": True,  "min_gravity": 0.4},
    "Gazeuse":        {"base_albedo": 0.52, "habitable_candidate": False, "min_gravity": 0.0},
    "Geante gazeuse": {"base_albedo": 0.52, "habitable_candidate": False, "min_gravity": 0.0},
    "Naine glacee":   {"base_albedo": 0.70, "habitable_candidate": False, "min_gravity": 0.0},
    "Lave":           {"base_albedo": 0.10, "habitable_candidate": False, "min_gravity": 0.0},
}


# ─── FONCTIONS PHYSIQUES ─────────────────────────────────────────────────────

def compute_temperature(star_luminosity: float, distance_au: float, albedo: float) -> float:
    """Température d'équilibre en °C (modèle corps noir)."""
    if distance_au <= 0:
        return 9999.0
    T_k = 278.0 * (star_luminosity ** 0.25) * ((1.0 - albedo) ** 0.25) / math.sqrt(distance_au)
    return round(T_k - 273.15, 1)


def habitable_zone(star_luminosity: float):
    """Zone habitable conservative (Kopparapu simplifié)."""
    inner = round(0.75 * math.sqrt(star_luminosity), 4)
    outer = round(1.77 * math.sqrt(star_luminosity), 4)
    return inner, outer


def stellar_flux(luminosity: float, distance_au: float) -> float:
    """Flux stellaire relatif (1.0 = Terre autour du Soleil)."""
    if distance_au <= 0:
        return 99999.0
    return round(luminosity / (distance_au ** 2), 4)


def compute_surface_radiation(
    luminosity: float, distance_au: float,
    xuv_factor: float, magnetic_field: bool,
    atmosphere_pressure_factor: float = 1.0
) -> float:
    """
    Radiation de surface en mSv/h.
    Prend en compte :
      - Flux stellaire total
      - Activité XUV de l'étoile
      - Protection magnétique (réduction 90% si présent)
      - Épaisseur atmosphérique (pression relative)
    Référence : Terre = ~0.00011 mSv/h (exposition naturelle)
    """
    flux       = luminosity / max(distance_au ** 2, 0.0001)
    raw_rad    = flux * xuv_factor * 0.0001   # calibré sur Terre

    # Protection magnétique : réduit de 90%
    if magnetic_field:
        raw_rad *= 0.1

    # Atmosphère épaisse réduit les radiations
    raw_rad /= max(atmosphere_pressure_factor, 0.01)

    return round(max(0.00001, raw_rad), 6)


def is_tidally_locked(distance_au: float, tidal_lock_au: float) -> bool:
    """Verrouillage marée : côté jour brûlant, côté nuit gelé."""
    return distance_au <= tidal_lock_au


def can_retain_atmosphere(gravity_g: float, temp_c: float) -> bool:
    """
    Une planète retient son atmosphère si :
    - gravité suffisante (> 0.4g pour retenir N2/O2)
    - température pas trop élevée (fuite thermique)
    """
    if gravity_g < 0.4:
        return False
    if temp_c > 150 and gravity_g < 0.8:
        return False
    return True


def assign_label(row: dict) -> str:
    """
    Label d'habitabilité basé sur des contraintes astrophysiques réelles.

    ÉLIMINATOIRES ABSOLUS (non_habitable immédiat) :
      1. Type planétaire non candidat (gazeuse, lave, naine glacée)
      2. Température d'équilibre < -80°C ou > 80°C (eau liquide impossible)
      3. Flux stellaire > 10× terrestre (même avec magnétisme → trop chaud/radiatif)
      4. Radiations de surface > 5 mSv/h (létales pour toute vie complexe)
      5. Étoile trop jeune pour son type (instabilité, flares)
      6. Durée de vie étoile trop courte (< 1 Gy → pas le temps pour la vie)
      7. Pas de rétention atmosphérique (gravité trop faible)
      8. Verrouillage marée SANS champ magnétique (côté nuit mort, côté jour brûlé)
      9. Pas dans la zone habitable

    CONDITIONS POUR "habitable" :
      - Toutes les éliminatoires écartées
      - Eau liquide possible (T entre -10 et 50°C avec pression)
      - O2 ≥ 15% et N2 présent
      - Eau présente
      - Champ magnétique (protection)
      - Étoile suffisamment âgée et stable
      - Flux stellaire entre 0.25 et 1.5× terrestre (optimal)
      - Radiations ≤ 0.5 mSv/h

    CONDITIONS POUR "inconnue" :
      - Pas d'éliminatoire absolu
      - Conditions partiellement remplies
    """
    ptype      = row["planet_type"]
    ptype_info = PLANET_TYPES[ptype]

    # ── ÉLIMINATOIRES ────────────────────────────────────────────────────────

    # 1. Type planétaire
    if not ptype_info["habitable_candidate"]:
        return "non_habitable"

    # 2. Température hors limites (eau liquide impossible)
    temp = row["avg_temp_celsius"]
    if temp < -80 or temp > 80:
        return "non_habitable"

    # 3. Flux stellaire trop intense
    flux = row["stellar_flux"]
    if flux > 10.0:
        return "non_habitable"

    # 4. Radiations létales (même avec protection)
    if row["surface_radiation_msv_h"] > 5.0:
        return "non_habitable"

    # 5. Étoile trop jeune / instable
    star_info = STAR_TYPES[row["star_type"]]
    if row["star_age_gy"] < star_info["min_age_gy"]:
        return "non_habitable"

    # 6. Durée de vie de l'étoile trop courte
    if star_info["lifespan_gy"] < 1.0:
        return "non_habitable"

    # 7. Rétention atmosphérique impossible
    if not can_retain_atmosphere(row["gravity_g"], temp):
        return "non_habitable"

    # 8. Verrouillage marée sans protection magnétique
    if row["tidally_locked"] and not row["magnetic_field"]:
        return "non_habitable"

    # 9. Hors zone habitable
    if not row["in_habitable_zone"]:
        return "non_habitable"

    # ── CONDITIONS OPTIMALES → habitable ─────────────────────────────────────
    temp_ok        = -10 <= temp <= 50
    flux_ok        = 0.25 <= flux <= 1.5
    o2_ok          = row["atmosphere_o2"] >= 15
    n2_ok          = row["atmosphere_n2"] >= 50
    water_ok       = row["has_water"]
    mag_ok         = row["magnetic_field"]
    rad_ok         = row["surface_radiation_msv_h"] <= 0.5
    age_ok         = row["star_age_gy"] >= 2.0
    gravity_ok     = 0.4 <= row["gravity_g"] <= 2.5
    co2_ok         = row["atmosphere_co2"] <= 5  # pas d'effet de serre runaway

    if (temp_ok and flux_ok and o2_ok and n2_ok and water_ok
            and mag_ok and rad_ok and age_ok and gravity_ok and co2_ok):
        return "habitable"

    # ── CONDITIONS PARTIELLES → inconnue ─────────────────────────────────────
    # Au moins eau OU zone habitable + un minimum d'O2 + pas de radiation létale
    partial_ok = (
        (row["has_water"] or row["in_habitable_zone"])
        and row["atmosphere_o2"] >= 5
        and row["surface_radiation_msv_h"] <= 2.0
        and -30 <= temp <= 70
    )
    if partial_ok:
        return "inconnue"

    return "non_habitable"


# ─── DÉFINITION DES SYSTÈMES STELLAIRES ──────────────────────────────────────
systems_raw = [
    {
        "system_name": "Aeloria Prime",
        "star_type": "Naine jaune",
        "star_age_gy": 4.2,
        "planets": [
            {"name": "Pyrox",      "planet_type": "Lave",         "distance_au": 0.3,  "radius_km": 4200,  "mass_earth": 0.6,  "gravity_g": 0.7,  "atmosphere_o2": 0,  "atmosphere_co2": 90, "atmosphere_n2": 9,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Aeloria-I",  "planet_type": "Tellurique",   "distance_au": 0.95, "radius_km": 6200,  "mass_earth": 0.9,  "gravity_g": 0.93, "atmosphere_o2": 20, "atmosphere_co2": 1,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Aeloria-II", "planet_type": "Ocean",        "distance_au": 1.2,  "radius_km": 7000,  "mass_earth": 1.1,  "gravity_g": 1.05, "atmosphere_o2": 18, "atmosphere_co2": 2,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Garvex",     "planet_type": "Gazeuse",      "distance_au": 4.0,  "radius_km": 55000, "mass_earth": 250,  "gravity_g": 2.4,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 14},
            {"name": "Cryon-I",    "planet_type": "Naine glacee", "distance_au": 12.0, "radius_km": 2800,  "mass_earth": 0.08, "gravity_g": 0.07, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 98, "has_water": False, "magnetic_field": False, "moons": 1},
        ]
    },
    {
        "system_name": "Vexar System",
        "star_type": "Naine rouge",
        "star_age_gy": 8.5,   # âgée → flares calmés
        "planets": [
            {"name": "Vexar-I",   "planet_type": "Lave",         "distance_au": 0.05, "radius_km": 3800,  "mass_earth": 0.5,  "gravity_g": 0.6,  "atmosphere_o2": 0,  "atmosphere_co2": 95, "atmosphere_n2": 4,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Vexar-II",  "planet_type": "Tellurique",   "distance_au": 0.15, "radius_km": 5800,  "mass_earth": 0.85, "gravity_g": 0.88, "atmosphere_o2": 15, "atmosphere_co2": 3,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Vexar-III", "planet_type": "Super-Terre",  "distance_au": 0.25, "radius_km": 9000,  "mass_earth": 2.5,  "gravity_g": 1.8,  "atmosphere_o2": 10, "atmosphere_co2": 8,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Vexar-IV",  "planet_type": "Naine glacee", "distance_au": 1.5,  "radius_km": 2000,  "mass_earth": 0.05, "gravity_g": 0.05, "atmosphere_o2": 0,  "atmosphere_co2": 1,  "atmosphere_n2": 90, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Drakon Nebula",
        "star_type": "Geante bleue",
        "star_age_gy": 0.05,
        "planets": [
            {"name": "Drakon-I",   "planet_type": "Lave",         "distance_au": 5.0,   "radius_km": 6000,  "mass_earth": 1.0,  "gravity_g": 1.0,  "atmosphere_o2": 0,  "atmosphere_co2": 70, "atmosphere_n2": 20, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Drakon-II",  "planet_type": "Gazeuse",      "distance_au": 50.0,  "radius_km": 70000, "mass_earth": 400,  "gravity_g": 3.0,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 32},
            {"name": "Drakon-III", "planet_type": "Naine glacee", "distance_au": 200.0, "radius_km": 3000,  "mass_earth": 0.1,  "gravity_g": 0.1,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 95, "has_water": False, "magnetic_field": False, "moons": 2},
        ]
    },
    {
        "system_name": "Luminos Cluster",
        "star_type": "Naine orange",
        "star_age_gy": 6.1,
        "planets": [
            {"name": "Luminos-I",   "planet_type": "Tellurique",    "distance_au": 0.2,  "radius_km": 4500,  "mass_earth": 0.4,  "gravity_g": 0.5,  "atmosphere_o2": 2,  "atmosphere_co2": 60, "atmosphere_n2": 35, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Luminos-II",  "planet_type": "Ocean",         "distance_au": 0.45, "radius_km": 6800,  "mass_earth": 1.0,  "gravity_g": 0.98, "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Luminos-III", "planet_type": "Super-Terre",   "distance_au": 0.65, "radius_km": 10000, "mass_earth": 3.0,  "gravity_g": 2.0,  "atmosphere_o2": 14, "atmosphere_co2": 5,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Glacior",     "planet_type": "Geante gazeuse","distance_au": 3.0,  "radius_km": 60000, "mass_earth": 300,  "gravity_g": 2.5,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 20},
        ]
    },
    {
        "system_name": "Stygion Void",
        "star_type": "Naine blanche",
        "star_age_gy": 12.0,
        "planets": [
            {"name": "Stygion-I",  "planet_type": "Naine glacee", "distance_au": 0.02, "radius_km": 3500,  "mass_earth": 0.2,  "gravity_g": 0.25, "atmosphere_o2": 0, "atmosphere_co2": 5,  "atmosphere_n2": 90, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Stygion-II", "planet_type": "Naine glacee", "distance_au": 0.08, "radius_km": 4000,  "mass_earth": 0.3,  "gravity_g": 0.35, "atmosphere_o2": 0, "atmosphere_co2": 2,  "atmosphere_n2": 95, "has_water": False, "magnetic_field": False, "moons": 1},
        ]
    },
    {
        "system_name": "Verdantis Alpha",
        "star_type": "Sous-geante",
        "star_age_gy": 3.8,
        "planets": [
            {"name": "Verdantis-I",   "planet_type": "Lave",        "distance_au": 0.4,  "radius_km": 5000,  "mass_earth": 0.7,  "gravity_g": 0.8,  "atmosphere_o2": 0,  "atmosphere_co2": 85, "atmosphere_n2": 14, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Verdantis-II",  "planet_type": "Tellurique",  "distance_au": 1.1,  "radius_km": 6500,  "mass_earth": 0.95, "gravity_g": 0.97, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Verdantis-III", "planet_type": "Ocean",       "distance_au": 1.6,  "radius_km": 7200,  "mass_earth": 1.2,  "gravity_g": 1.1,  "atmosphere_o2": 19, "atmosphere_co2": 2,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Verdantis-IV",  "planet_type": "Gazeuse",     "distance_au": 5.5,  "radius_km": 65000, "mass_earth": 320,  "gravity_g": 2.6,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 18},
            {"name": "Glacius-V",     "planet_type": "Naine glacee","distance_au": 15.0, "radius_km": 2500,  "mass_earth": 0.07, "gravity_g": 0.07, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 99, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Ashenveil Binary",
        "star_type": "Geante rouge",
        "star_age_gy": 9.8,
        "planets": [
            {"name": "Ashenveil-I",   "planet_type": "Lave",        "distance_au": 8.0,  "radius_km": 7000,  "mass_earth": 1.3,  "gravity_g": 1.2,  "atmosphere_o2": 0,  "atmosphere_co2": 75, "atmosphere_n2": 20, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Ashenveil-II",  "planet_type": "Gazeuse",     "distance_au": 25.0, "radius_km": 58000, "mass_earth": 280,  "gravity_g": 2.3,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 25},
            {"name": "Ashenveil-III", "planet_type": "Naine glacee","distance_au": 80.0, "radius_km": 3200,  "mass_earth": 0.15, "gravity_g": 0.15, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 98, "has_water": False, "magnetic_field": False, "moons": 2},
        ]
    },
    {
        "system_name": "Kepler Nova",
        "star_type": "Naine jaune",
        "star_age_gy": 5.5,
        "planets": [
            {"name": "Kepler-I",   "planet_type": "Lave",         "distance_au": 0.2,  "radius_km": 4000,  "mass_earth": 0.5,  "gravity_g": 0.6,  "atmosphere_o2": 0,  "atmosphere_co2": 88, "atmosphere_n2": 10, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Kepler-II",  "planet_type": "Tellurique",   "distance_au": 1.0,  "radius_km": 6400,  "mass_earth": 1.0,  "gravity_g": 1.0,  "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Kepler-III", "planet_type": "Ocean",        "distance_au": 1.4,  "radius_km": 7100,  "mass_earth": 1.2,  "gravity_g": 1.1,  "atmosphere_o2": 20, "atmosphere_co2": 1,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Kepler-IV",  "planet_type": "Gazeuse",      "distance_au": 5.0,  "radius_km": 60000, "mass_earth": 300,  "gravity_g": 2.5,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 20},
        ]
    },
    {
        "system_name": "Orion Belt",
        "star_type": "Naine orange",
        "star_age_gy": 7.2,
        "planets": [
            {"name": "Orion-I",   "planet_type": "Tellurique",   "distance_au": 0.4,  "radius_km": 5500,  "mass_earth": 0.7,  "gravity_g": 0.8,  "atmosphere_o2": 18, "atmosphere_co2": 2,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Orion-II",  "planet_type": "Super-Terre",  "distance_au": 0.6,  "radius_km": 9500,  "mass_earth": 2.8,  "gravity_g": 1.9,  "atmosphere_o2": 16, "atmosphere_co2": 3,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Orion-III", "planet_type": "Naine glacee", "distance_au": 3.0,  "radius_km": 2500,  "mass_earth": 0.06, "gravity_g": 0.06, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 99, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Solara Minor",
        "star_type": "Sous-geante",
        "star_age_gy": 4.5,
        "planets": [
            {"name": "Solara-I",   "planet_type": "Lave",        "distance_au": 0.3,  "radius_km": 4800,  "mass_earth": 0.6,  "gravity_g": 0.7,  "atmosphere_o2": 0,  "atmosphere_co2": 92, "atmosphere_n2": 7,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Solara-II",  "planet_type": "Ocean",       "distance_au": 1.3,  "radius_km": 7000,  "mass_earth": 1.1,  "gravity_g": 1.05, "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 3},
            {"name": "Solara-III", "planet_type": "Tellurique",  "distance_au": 1.8,  "radius_km": 6200,  "mass_earth": 0.9,  "gravity_g": 0.92, "atmosphere_o2": 19, "atmosphere_co2": 2,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Solara-IV",  "planet_type": "Gazeuse",     "distance_au": 6.0,  "radius_km": 55000, "mass_earth": 270,  "gravity_g": 2.3,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 15},
        ]
    },
    {
        "system_name": "Cygnus Deep",
        "star_type": "Naine rouge",
        "star_age_gy": 6.0,
        "planets": [
            {"name": "Cygnus-I",   "planet_type": "Tellurique",   "distance_au": 0.1,  "radius_km": 5800,  "mass_earth": 0.8,  "gravity_g": 0.85, "atmosphere_o2": 17, "atmosphere_co2": 2,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Cygnus-II",  "planet_type": "Super-Terre",  "distance_au": 0.2,  "radius_km": 8500,  "mass_earth": 2.2,  "gravity_g": 1.7,  "atmosphere_o2": 12, "atmosphere_co2": 6,  "atmosphere_n2": 81, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Cygnus-III", "planet_type": "Naine glacee", "distance_au": 1.0,  "radius_km": 2200,  "mass_earth": 0.04, "gravity_g": 0.04, "atmosphere_o2": 0,  "atmosphere_co2": 1,  "atmosphere_n2": 95, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Arcturus Prime",
        "star_type": "Geante rouge",
        "star_age_gy": 8.0,
        "planets": [
            {"name": "Arcturus-I",   "planet_type": "Lave",        "distance_au": 5.0,  "radius_km": 6500,  "mass_earth": 1.1,  "gravity_g": 1.1,  "atmosphere_o2": 0,  "atmosphere_co2": 80, "atmosphere_n2": 18, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Arcturus-II",  "planet_type": "Gazeuse",     "distance_au": 20.0, "radius_km": 62000, "mass_earth": 290,  "gravity_g": 2.4,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 22},
            {"name": "Arcturus-III", "planet_type": "Naine glacee","distance_au": 60.0, "radius_km": 3100,  "mass_earth": 0.12, "gravity_g": 0.12, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 97, "has_water": False, "magnetic_field": False, "moons": 1},
        ]
    },
    {
        "system_name": "Helios Secundus",
        "star_type": "Naine jaune",
        "star_age_gy": 3.1,
        "planets": [
            {"name": "Helios-I",   "planet_type": "Lave",        "distance_au": 0.4,  "radius_km": 4600,  "mass_earth": 0.65, "gravity_g": 0.72, "atmosphere_o2": 0,  "atmosphere_co2": 91, "atmosphere_n2": 8,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Helios-II",  "planet_type": "Tellurique",  "distance_au": 0.85, "radius_km": 6300,  "mass_earth": 0.95, "gravity_g": 0.96, "atmosphere_o2": 20, "atmosphere_co2": 1,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Helios-III", "planet_type": "Super-Terre", "distance_au": 1.3,  "radius_km": 9200,  "mass_earth": 2.6,  "gravity_g": 1.85, "atmosphere_o2": 17, "atmosphere_co2": 3,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Helios-IV",  "planet_type": "Gazeuse",     "distance_au": 4.5,  "radius_km": 58000, "mass_earth": 260,  "gravity_g": 2.2,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 16},
            {"name": "Helios-V",   "planet_type": "Naine glacee","distance_au": 10.0, "radius_km": 2600,  "mass_earth": 0.07, "gravity_g": 0.07, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 98, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Proxima Obscura",
        "star_type": "Naine rouge",
        "star_age_gy": 10.2,  # très vieille → stable
        "planets": [
            {"name": "Proxima-I",   "planet_type": "Tellurique",  "distance_au": 0.08, "radius_km": 5900,  "mass_earth": 0.82, "gravity_g": 0.87, "atmosphere_o2": 19, "atmosphere_co2": 2,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Proxima-II",  "planet_type": "Ocean",       "distance_au": 0.12, "radius_km": 6700,  "mass_earth": 1.0,  "gravity_g": 0.99, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Proxima-III", "planet_type": "Naine glacee","distance_au": 0.8,  "radius_km": 2100,  "mass_earth": 0.03, "gravity_g": 0.03, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 98, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Tauri Expanse",
        "star_type": "Naine orange",
        "star_age_gy": 5.3,
        "planets": [
            {"name": "Tauri-I",   "planet_type": "Lave",          "distance_au": 0.15, "radius_km": 4100,  "mass_earth": 0.55, "gravity_g": 0.65, "atmosphere_o2": 0,  "atmosphere_co2": 87, "atmosphere_n2": 12, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Tauri-II",  "planet_type": "Tellurique",    "distance_au": 0.5,  "radius_km": 6100,  "mass_earth": 0.88, "gravity_g": 0.91, "atmosphere_o2": 20, "atmosphere_co2": 1,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Tauri-III", "planet_type": "Ocean",         "distance_au": 0.7,  "radius_km": 6900,  "mass_earth": 1.05, "gravity_g": 1.02, "atmosphere_o2": 23, "atmosphere_co2": 1,  "atmosphere_n2": 75, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Tauri-IV",  "planet_type": "Geante gazeuse","distance_au": 4.0,  "radius_km": 65000, "mass_earth": 310,  "gravity_g": 2.55, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 24},
        ]
    },
    {
        "system_name": "Meridian Cross",
        "star_type": "Sous-geante",
        "star_age_gy": 6.7,
        "planets": [
            {"name": "Meridian-I",   "planet_type": "Tellurique",  "distance_au": 0.9,  "radius_km": 6000,  "mass_earth": 0.87, "gravity_g": 0.9,  "atmosphere_o2": 16, "atmosphere_co2": 3,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Meridian-II",  "planet_type": "Ocean",       "distance_au": 1.5,  "radius_km": 7300,  "mass_earth": 1.25, "gravity_g": 1.12, "atmosphere_o2": 20, "atmosphere_co2": 2,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Meridian-III", "planet_type": "Super-Terre", "distance_au": 2.2,  "radius_km": 9800,  "mass_earth": 2.9,  "gravity_g": 1.95, "atmosphere_o2": 8,  "atmosphere_co2": 10, "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Meridian-IV",  "planet_type": "Gazeuse",     "distance_au": 7.0,  "radius_km": 57000, "mass_earth": 285,  "gravity_g": 2.35, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 19},
            {"name": "Meridian-V",   "planet_type": "Naine glacee","distance_au": 18.0, "radius_km": 2700,  "mass_earth": 0.09, "gravity_g": 0.09, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 99, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Ferox Cluster",
        "star_type": "Naine blanche",
        "star_age_gy": 9.5,
        "planets": [
            {"name": "Ferox-I",   "planet_type": "Naine glacee", "distance_au": 0.01, "radius_km": 3300,  "mass_earth": 0.18, "gravity_g": 0.22, "atmosphere_o2": 0,  "atmosphere_co2": 4,  "atmosphere_n2": 92, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Ferox-II",  "planet_type": "Tellurique",   "distance_au": 0.05, "radius_km": 5700,  "mass_earth": 0.78, "gravity_g": 0.83, "atmosphere_o2": 0,  "atmosphere_co2": 3,  "atmosphere_n2": 94, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Ferox-III", "planet_type": "Naine glacee", "distance_au": 0.2,  "radius_km": 3900,  "mass_earth": 0.28, "gravity_g": 0.33, "atmosphere_o2": 0,  "atmosphere_co2": 1,  "atmosphere_n2": 96, "has_water": False, "magnetic_field": False, "moons": 1},
        ]
    },
    {
        "system_name": "Nova Serpentis",
        "star_type": "Naine jaune",
        "star_age_gy": 7.8,
        "planets": [
            {"name": "Serpentis-I",   "planet_type": "Lave",       "distance_au": 0.25, "radius_km": 4300,  "mass_earth": 0.58, "gravity_g": 0.68, "atmosphere_o2": 0,  "atmosphere_co2": 89, "atmosphere_n2": 10, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Serpentis-II",  "planet_type": "Ocean",      "distance_au": 1.1,  "radius_km": 6600,  "mass_earth": 0.97, "gravity_g": 0.97, "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Serpentis-III", "planet_type": "Tellurique", "distance_au": 1.5,  "radius_km": 6100,  "mass_earth": 0.88, "gravity_g": 0.91, "atmosphere_o2": 18, "atmosphere_co2": 2,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Serpentis-IV",  "planet_type": "Gazeuse",    "distance_au": 6.0,  "radius_km": 56000, "mass_earth": 265,  "gravity_g": 2.25, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 17},
        ]
    },
    {
        "system_name": "Azar Twilight",
        "star_type": "Naine rouge",
        "star_age_gy": 4.8,
        "planets": [
            {"name": "Azar-I",   "planet_type": "Lave",          "distance_au": 0.04, "radius_km": 3700,  "mass_earth": 0.48, "gravity_g": 0.58, "atmosphere_o2": 0,  "atmosphere_co2": 93, "atmosphere_n2": 6,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Azar-II",  "planet_type": "Super-Terre",   "distance_au": 0.09, "radius_km": 8800,  "mass_earth": 2.3,  "gravity_g": 1.75, "atmosphere_o2": 15, "atmosphere_co2": 4,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Azar-III", "planet_type": "Naine glacee",  "distance_au": 0.6,  "radius_km": 2300,  "mass_earth": 0.05, "gravity_g": 0.05, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 97, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    # ── Systèmes supplémentaires pour enrichir le dataset ─────────────────────
    {
        "system_name": "Crystallis Reach",
        "star_type": "Naine jaune",
        "star_age_gy": 6.8,
        "planets": [
            {"name": "Crystal-I",  "planet_type": "Lave",        "distance_au": 0.35, "radius_km": 4500,  "mass_earth": 0.62, "gravity_g": 0.75, "atmosphere_o2": 0,  "atmosphere_co2": 88, "atmosphere_n2": 11, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Crystal-II", "planet_type": "Tellurique",  "distance_au": 0.9,  "radius_km": 6350,  "mass_earth": 0.92, "gravity_g": 0.94, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Crystal-III","planet_type": "Ocean",       "distance_au": 1.3,  "radius_km": 7050,  "mass_earth": 1.15, "gravity_g": 1.08, "atmosphere_o2": 20, "atmosphere_co2": 1,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Crystal-IV", "planet_type": "Gazeuse",     "distance_au": 5.5,  "radius_km": 58000, "mass_earth": 280,  "gravity_g": 2.35, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 18},
        ]
    },
    {
        "system_name": "Ember Drift",
        "star_type": "Naine rouge",
        "star_age_gy": 3.2,   # jeune → flares actifs
        "planets": [
            {"name": "Ember-I",  "planet_type": "Lave",        "distance_au": 0.03, "radius_km": 3600,  "mass_earth": 0.42, "gravity_g": 0.55, "atmosphere_o2": 0,  "atmosphere_co2": 96, "atmosphere_n2": 3,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Ember-II", "planet_type": "Tellurique",  "distance_au": 0.1,  "radius_km": 5600,  "mass_earth": 0.75, "gravity_g": 0.82, "atmosphere_o2": 14, "atmosphere_co2": 4,  "atmosphere_n2": 81, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Ember-III","planet_type": "Naine glacee","distance_au": 0.7,  "radius_km": 2400,  "mass_earth": 0.06, "gravity_g": 0.06, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 97, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Solstice Veil",
        "star_type": "Naine orange",
        "star_age_gy": 9.1,
        "planets": [
            {"name": "Solstice-I",   "planet_type": "Lave",        "distance_au": 0.1,  "radius_km": 3900,  "mass_earth": 0.5,  "gravity_g": 0.62, "atmosphere_o2": 0,  "atmosphere_co2": 91, "atmosphere_n2": 8,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Solstice-II",  "planet_type": "Super-Terre", "distance_au": 0.4,  "radius_km": 9100,  "mass_earth": 2.4,  "gravity_g": 1.82, "atmosphere_o2": 16, "atmosphere_co2": 4,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Solstice-III", "planet_type": "Ocean",       "distance_au": 0.6,  "radius_km": 6750,  "mass_earth": 1.05, "gravity_g": 1.0,  "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Solstice-IV",  "planet_type": "Gazeuse",     "distance_au": 4.0,  "radius_km": 60000, "mass_earth": 295,  "gravity_g": 2.45, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 21},
        ]
    },
    {
        "system_name": "Phantom Reach",
        "star_type": "Naine blanche",
        "star_age_gy": 7.3,
        "planets": [
            {"name": "Phantom-I",  "planet_type": "Tellurique",  "distance_au": 0.03, "radius_km": 5500,  "mass_earth": 0.72, "gravity_g": 0.79, "atmosphere_o2": 5,  "atmosphere_co2": 10, "atmosphere_n2": 82, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Phantom-II", "planet_type": "Naine glacee","distance_au": 0.15, "radius_km": 3800,  "mass_earth": 0.25, "gravity_g": 0.28, "atmosphere_o2": 0,  "atmosphere_co2": 2,  "atmosphere_n2": 95, "has_water": False, "magnetic_field": False, "moons": 1},
        ]
    },
    {
        "system_name": "Vireon Expanse",
        "star_type": "Sous-geante",
        "star_age_gy": 2.1,
        "planets": [
            {"name": "Vireon-I",   "planet_type": "Lave",        "distance_au": 0.5,  "radius_km": 5200,  "mass_earth": 0.75, "gravity_g": 0.85, "atmosphere_o2": 0,  "atmosphere_co2": 87, "atmosphere_n2": 12, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Vireon-II",  "planet_type": "Tellurique",  "distance_au": 1.2,  "radius_km": 6400,  "mass_earth": 0.93, "gravity_g": 0.95, "atmosphere_o2": 19, "atmosphere_co2": 2,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Vireon-III", "planet_type": "Super-Terre", "distance_au": 1.9,  "radius_km": 9400,  "mass_earth": 2.7,  "gravity_g": 1.88, "atmosphere_o2": 13, "atmosphere_co2": 6,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Vireon-IV",  "planet_type": "Gazeuse",     "distance_au": 7.0,  "radius_km": 62000, "mass_earth": 305,  "gravity_g": 2.5,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 16},
            {"name": "Vireon-V",   "planet_type": "Naine glacee","distance_au": 20.0, "radius_km": 2600,  "mass_earth": 0.08, "gravity_g": 0.08, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 99, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    
        # ══════════════════════════════════════════════════════════════════════════
    # NOUVEAUX SYSTÈMES (v3) — +40 systèmes, ~200 nouvelles planètes
    # Objectif : enrichir les classes habitable et inconnue
    # ══════════════════════════════════════════════════════════════════════════
 
    # ── Naines jaunes supplémentaires (habitable focus) ───────────────────────
    {
        "system_name": "Solarius Rex", "star_type": "Naine jaune", "star_age_gy": 4.8,
        "planets": [
            {"name": "Solarius-I",   "planet_type": "Lave",        "distance_au": 0.28, "radius_km": 4100,  "mass_earth": 0.55, "gravity_g": 0.65, "atmosphere_o2": 0,  "atmosphere_co2": 87, "atmosphere_n2": 12, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Solarius-II",  "planet_type": "Tellurique",  "distance_au": 0.88, "radius_km": 6250,  "mass_earth": 0.91, "gravity_g": 0.93, "atmosphere_o2": 20, "atmosphere_co2": 1,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Solarius-III", "planet_type": "Ocean",       "distance_au": 1.15, "radius_km": 6900,  "mass_earth": 1.08, "gravity_g": 1.03, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Solarius-IV",  "planet_type": "Super-Terre", "distance_au": 1.6,  "radius_km": 9300,  "mass_earth": 2.5,  "gravity_g": 1.82, "atmosphere_o2": 16, "atmosphere_co2": 3,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Solarius-V",   "planet_type": "Gazeuse",     "distance_au": 5.2,  "radius_km": 61000, "mass_earth": 290,  "gravity_g": 2.4,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 19},
        ]
    },
    {
        "system_name": "Aurum Spire", "star_type": "Naine jaune", "star_age_gy": 6.3,
        "planets": [
            {"name": "Aurum-I",   "planet_type": "Lave",         "distance_au": 0.32, "radius_km": 4300,  "mass_earth": 0.6,  "gravity_g": 0.7,  "atmosphere_o2": 0,  "atmosphere_co2": 89, "atmosphere_n2": 10, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Aurum-II",  "planet_type": "Ocean",        "distance_au": 1.05, "radius_km": 7100,  "mass_earth": 1.12, "gravity_g": 1.06, "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Aurum-III", "planet_type": "Tellurique",   "distance_au": 1.45, "radius_km": 6100,  "mass_earth": 0.87, "gravity_g": 0.89, "atmosphere_o2": 19, "atmosphere_co2": 2,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Aurum-IV",  "planet_type": "Gazeuse",      "distance_au": 4.8,  "radius_km": 59000, "mass_earth": 275,  "gravity_g": 2.3,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 17},
        ]
    },
    {
        "system_name": "Calypso Dawn", "star_type": "Naine jaune", "star_age_gy": 5.1,
        "planets": [
            {"name": "Calypso-I",  "planet_type": "Lave",        "distance_au": 0.22, "radius_km": 3900,  "mass_earth": 0.48, "gravity_g": 0.58, "atmosphere_o2": 0,  "atmosphere_co2": 91, "atmosphere_n2": 8,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Calypso-II", "planet_type": "Tellurique",  "distance_au": 0.92, "radius_km": 6400,  "mass_earth": 0.96, "gravity_g": 0.97, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Calypso-III","planet_type": "Ocean",       "distance_au": 1.25, "radius_km": 7200,  "mass_earth": 1.18, "gravity_g": 1.1,  "atmosphere_o2": 20, "atmosphere_co2": 1,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Calypso-IV", "planet_type": "Naine glacee","distance_au": 9.0,  "radius_km": 2700,  "mass_earth": 0.08, "gravity_g": 0.08, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 98, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Phoebe Light", "star_type": "Naine jaune", "star_age_gy": 7.5,
        "planets": [
            {"name": "Phoebe-I",   "planet_type": "Tellurique",  "distance_au": 0.78, "radius_km": 6100,  "mass_earth": 0.85, "gravity_g": 0.88, "atmosphere_o2": 19, "atmosphere_co2": 2,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Phoebe-II",  "planet_type": "Ocean",       "distance_au": 1.1,  "radius_km": 6800,  "mass_earth": 1.05, "gravity_g": 1.01, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Phoebe-III", "planet_type": "Super-Terre", "distance_au": 1.7,  "radius_km": 9100,  "mass_earth": 2.4,  "gravity_g": 1.78, "atmosphere_o2": 14, "atmosphere_co2": 5,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Phoebe-IV",  "planet_type": "Gazeuse",     "distance_au": 6.5,  "radius_km": 62000, "mass_earth": 310,  "gravity_g": 2.5,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 22},
        ]
    },
    {
        "system_name": "Theron Gate", "star_type": "Naine jaune", "star_age_gy": 3.8,
        "planets": [
            {"name": "Theron-I",  "planet_type": "Lave",        "distance_au": 0.18, "radius_km": 3800,  "mass_earth": 0.44, "gravity_g": 0.56, "atmosphere_o2": 0,  "atmosphere_co2": 93, "atmosphere_n2": 6,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Theron-II", "planet_type": "Tellurique",  "distance_au": 0.95, "radius_km": 6300,  "mass_earth": 0.93, "gravity_g": 0.95, "atmosphere_o2": 20, "atmosphere_co2": 2,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Theron-III","planet_type": "Ocean",       "distance_au": 1.3,  "radius_km": 7000,  "mass_earth": 1.1,  "gravity_g": 1.04, "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Theron-IV", "planet_type": "Gazeuse",     "distance_au": 5.8,  "radius_km": 60000, "mass_earth": 285,  "gravity_g": 2.35, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 18},
        ]
    },
 
    # ── Naines oranges supplémentaires ────────────────────────────────────────
    {
        "system_name": "Vesper Chain", "star_type": "Naine orange", "star_age_gy": 4.7,
        "planets": [
            {"name": "Vesper-I",   "planet_type": "Lave",        "distance_au": 0.12, "radius_km": 3800,  "mass_earth": 0.5,  "gravity_g": 0.6,  "atmosphere_o2": 0,  "atmosphere_co2": 90, "atmosphere_n2": 9,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Vesper-II",  "planet_type": "Ocean",       "distance_au": 0.42, "radius_km": 6700,  "mass_earth": 1.0,  "gravity_g": 0.97, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Vesper-III", "planet_type": "Tellurique",  "distance_au": 0.62, "radius_km": 6000,  "mass_earth": 0.84, "gravity_g": 0.88, "atmosphere_o2": 18, "atmosphere_co2": 2,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Vesper-IV",  "planet_type": "Gazeuse",     "distance_au": 3.5,  "radius_km": 58000, "mass_earth": 270,  "gravity_g": 2.3,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 15},
        ]
    },
    {
        "system_name": "Cerise Hollow", "star_type": "Naine orange", "star_age_gy": 8.3,
        "planets": [
            {"name": "Cerise-I",  "planet_type": "Tellurique",   "distance_au": 0.35, "radius_km": 5800,  "mass_earth": 0.76, "gravity_g": 0.82, "atmosphere_o2": 17, "atmosphere_co2": 3,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Cerise-II", "planet_type": "Ocean",        "distance_au": 0.52, "radius_km": 6900,  "mass_earth": 1.05, "gravity_g": 1.01, "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Cerise-III","planet_type": "Super-Terre",  "distance_au": 0.78, "radius_km": 9200,  "mass_earth": 2.5,  "gravity_g": 1.8,  "atmosphere_o2": 15, "atmosphere_co2": 4,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Cerise-IV", "planet_type": "Naine glacee", "distance_au": 4.0,  "radius_km": 2600,  "mass_earth": 0.07, "gravity_g": 0.07, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 98, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Ambrel Crossing", "star_type": "Naine orange", "star_age_gy": 5.9,
        "planets": [
            {"name": "Ambrel-I",   "planet_type": "Lave",        "distance_au": 0.08, "radius_km": 3600,  "mass_earth": 0.43, "gravity_g": 0.53, "atmosphere_o2": 0,  "atmosphere_co2": 94, "atmosphere_n2": 5,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Ambrel-II",  "planet_type": "Tellurique",  "distance_au": 0.48, "radius_km": 6200,  "mass_earth": 0.9,  "gravity_g": 0.92, "atmosphere_o2": 20, "atmosphere_co2": 1,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Ambrel-III", "planet_type": "Ocean",       "distance_au": 0.68, "radius_km": 7000,  "mass_earth": 1.1,  "gravity_g": 1.04, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Ambrel-IV",  "planet_type": "Gazeuse",     "distance_au": 3.8,  "radius_km": 59000, "mass_earth": 280,  "gravity_g": 2.35, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 16},
        ]
    },
    {
        "system_name": "Dusken Arc", "star_type": "Naine orange", "star_age_gy": 3.5,
        "planets": [
            {"name": "Dusken-I",  "planet_type": "Tellurique",  "distance_au": 0.45, "radius_km": 5900,  "mass_earth": 0.8,  "gravity_g": 0.84, "atmosphere_o2": 16, "atmosphere_co2": 4,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Dusken-II", "planet_type": "Super-Terre", "distance_au": 0.65, "radius_km": 9000,  "mass_earth": 2.3,  "gravity_g": 1.74, "atmosphere_o2": 13, "atmosphere_co2": 6,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Dusken-III","planet_type": "Gazeuse",     "distance_au": 4.5,  "radius_km": 60000, "mass_earth": 290,  "gravity_g": 2.4,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 20},
        ]
    },
 
    # ── Sous-géantes supplémentaires ──────────────────────────────────────────
    {
        "system_name": "Elysian Reach", "star_type": "Sous-geante", "star_age_gy": 5.2,
        "planets": [
            {"name": "Elysian-I",   "planet_type": "Lave",        "distance_au": 0.35, "radius_km": 4700,  "mass_earth": 0.65, "gravity_g": 0.75, "atmosphere_o2": 0,  "atmosphere_co2": 88, "atmosphere_n2": 11, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Elysian-II",  "planet_type": "Ocean",       "distance_au": 1.2,  "radius_km": 7100,  "mass_earth": 1.15, "gravity_g": 1.08, "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Elysian-III", "planet_type": "Tellurique",  "distance_au": 1.7,  "radius_km": 6300,  "mass_earth": 0.92, "gravity_g": 0.94, "atmosphere_o2": 19, "atmosphere_co2": 2,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Elysian-IV",  "planet_type": "Super-Terre", "distance_au": 2.4,  "radius_km": 9500,  "mass_earth": 2.8,  "gravity_g": 1.9,  "atmosphere_o2": 12, "atmosphere_co2": 6,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Elysian-V",   "planet_type": "Gazeuse",     "distance_au": 7.5,  "radius_km": 63000, "mass_earth": 320,  "gravity_g": 2.55, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 23},
        ]
    },
    {
        "system_name": "Novara Path", "star_type": "Sous-geante", "star_age_gy": 4.1,
        "planets": [
            {"name": "Novara-I",   "planet_type": "Lave",        "distance_au": 0.45, "radius_km": 5100,  "mass_earth": 0.72, "gravity_g": 0.82, "atmosphere_o2": 0,  "atmosphere_co2": 86, "atmosphere_n2": 13, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Novara-II",  "planet_type": "Tellurique",  "distance_au": 1.15, "radius_km": 6450,  "mass_earth": 0.94, "gravity_g": 0.96, "atmosphere_o2": 20, "atmosphere_co2": 1,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Novara-III", "planet_type": "Ocean",       "distance_au": 1.65, "radius_km": 7250,  "mass_earth": 1.22, "gravity_g": 1.12, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Novara-IV",  "planet_type": "Gazeuse",     "distance_au": 6.2,  "radius_km": 58000, "mass_earth": 275,  "gravity_g": 2.3,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 17},
        ]
    },
    {
        "system_name": "Tethis Grove", "star_type": "Sous-geante", "star_age_gy": 7.3,
        "planets": [
            {"name": "Tethis-I",   "planet_type": "Tellurique",  "distance_au": 1.0,  "radius_km": 6200,  "mass_earth": 0.9,  "gravity_g": 0.92, "atmosphere_o2": 19, "atmosphere_co2": 2,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Tethis-II",  "planet_type": "Ocean",       "distance_au": 1.4,  "radius_km": 7100,  "mass_earth": 1.18, "gravity_g": 1.1,  "atmosphere_o2": 23, "atmosphere_co2": 1,  "atmosphere_n2": 75, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Tethis-III", "planet_type": "Super-Terre", "distance_au": 2.1,  "radius_km": 9600,  "mass_earth": 2.9,  "gravity_g": 1.93, "atmosphere_o2": 10, "atmosphere_co2": 8,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Tethis-IV",  "planet_type": "Gazeuse",     "distance_au": 7.8,  "radius_km": 61000, "mass_earth": 300,  "gravity_g": 2.45, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 21},
        ]
    },
 
    # ── Naines rouges supplémentaires (âgées, stables) ───────────────────────
    {
        "system_name": "Cinderfall", "star_type": "Naine rouge", "star_age_gy": 9.3,
        "planets": [
            {"name": "Cinder-I",   "planet_type": "Lave",        "distance_au": 0.04, "radius_km": 3500,  "mass_earth": 0.44, "gravity_g": 0.55, "atmosphere_o2": 0,  "atmosphere_co2": 94, "atmosphere_n2": 5,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Cinder-II",  "planet_type": "Tellurique",  "distance_au": 0.12, "radius_km": 5700,  "mass_earth": 0.78, "gravity_g": 0.84, "atmosphere_o2": 18, "atmosphere_co2": 2,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Cinder-III", "planet_type": "Ocean",       "distance_au": 0.18, "radius_km": 6600,  "mass_earth": 1.0,  "gravity_g": 0.98, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Cinder-IV",  "planet_type": "Naine glacee","distance_au": 1.2,  "radius_km": 2300,  "mass_earth": 0.05, "gravity_g": 0.05, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 97, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Redhaven", "star_type": "Naine rouge", "star_age_gy": 7.8,
        "planets": [
            {"name": "Redhaven-I",  "planet_type": "Tellurique",  "distance_au": 0.09, "radius_km": 5800,  "mass_earth": 0.81, "gravity_g": 0.86, "atmosphere_o2": 17, "atmosphere_co2": 3,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Redhaven-II", "planet_type": "Super-Terre", "distance_au": 0.16, "radius_km": 8700,  "mass_earth": 2.2,  "gravity_g": 1.72, "atmosphere_o2": 13, "atmosphere_co2": 5,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Redhaven-III","planet_type": "Naine glacee","distance_au": 1.0,  "radius_km": 2200,  "mass_earth": 0.04, "gravity_g": 0.04, "atmosphere_o2": 0,  "atmosphere_co2": 1,  "atmosphere_n2": 96, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Scarlet Rim", "star_type": "Naine rouge", "star_age_gy": 11.5,
        "planets": [
            {"name": "Scarlet-I",   "planet_type": "Lave",        "distance_au": 0.03, "radius_km": 3400,  "mass_earth": 0.4,  "gravity_g": 0.52, "atmosphere_o2": 0,  "atmosphere_co2": 95, "atmosphere_n2": 4,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Scarlet-II",  "planet_type": "Ocean",       "distance_au": 0.1,  "radius_km": 6500,  "mass_earth": 0.95, "gravity_g": 0.95, "atmosphere_o2": 20, "atmosphere_co2": 2,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Scarlet-III", "planet_type": "Tellurique",  "distance_au": 0.15, "radius_km": 5900,  "mass_earth": 0.82, "gravity_g": 0.87, "atmosphere_o2": 18, "atmosphere_co2": 2,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Scarlet-IV",  "planet_type": "Naine glacee","distance_au": 0.9,  "radius_km": 2100,  "mass_earth": 0.03, "gravity_g": 0.04, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 98, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Ignis Minor", "star_type": "Naine rouge", "star_age_gy": 5.5,
        "planets": [
            {"name": "Ignis-I",  "planet_type": "Lave",        "distance_au": 0.05, "radius_km": 3700,  "mass_earth": 0.46, "gravity_g": 0.56, "atmosphere_o2": 0,  "atmosphere_co2": 93, "atmosphere_n2": 6,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Ignis-II", "planet_type": "Super-Terre", "distance_au": 0.13, "radius_km": 8600,  "mass_earth": 2.1,  "gravity_g": 1.7,  "atmosphere_o2": 14, "atmosphere_co2": 5,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Ignis-III","planet_type": "Tellurique",  "distance_au": 0.2,  "radius_km": 5800,  "mass_earth": 0.8,  "gravity_g": 0.84, "atmosphere_o2": 16, "atmosphere_co2": 3,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 0},
        ]
    },
 
    # ── Systèmes avec planètes "inconnue" intentionnelles ────────────────────
    {
        "system_name": "Ambiguous Veil", "star_type": "Naine jaune", "star_age_gy": 4.5,
        "planets": [
            {"name": "AV-I",   "planet_type": "Lave",        "distance_au": 0.3,  "radius_km": 4200,  "mass_earth": 0.58, "gravity_g": 0.68, "atmosphere_o2": 0,  "atmosphere_co2": 88, "atmosphere_n2": 11, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "AV-II",  "planet_type": "Tellurique",  "distance_au": 1.0,  "radius_km": 6300,  "mass_earth": 0.9,  "gravity_g": 0.92, "atmosphere_o2": 8,  "atmosphere_co2": 4,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},   # inconnue: o2 < 15
            {"name": "AV-III", "planet_type": "Ocean",       "distance_au": 1.35, "radius_km": 7000,  "mass_earth": 1.1,  "gravity_g": 1.04, "atmosphere_o2": 12, "atmosphere_co2": 3,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 2},   # inconnue: o2 borderline
            {"name": "AV-IV",  "planet_type": "Gazeuse",     "distance_au": 5.0,  "radius_km": 60000, "mass_earth": 295,  "gravity_g": 2.4,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 18},
        ]
    },
    {
        "system_name": "Twilight Margin", "star_type": "Naine orange", "star_age_gy": 6.5,
        "planets": [
            {"name": "TM-I",   "planet_type": "Tellurique",  "distance_au": 0.38, "radius_km": 5700,  "mass_earth": 0.75, "gravity_g": 0.8,  "atmosphere_o2": 10, "atmosphere_co2": 3,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 0},   # inconnue
            {"name": "TM-II",  "planet_type": "Super-Terre", "distance_au": 0.55, "radius_km": 9000,  "mass_earth": 2.3,  "gravity_g": 1.75, "atmosphere_o2": 7,  "atmosphere_co2": 7,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},   # inconnue
            {"name": "TM-III", "planet_type": "Ocean",       "distance_au": 0.72, "radius_km": 6800,  "mass_earth": 1.05, "gravity_g": 1.01, "atmosphere_o2": 20, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 2},   # habitable
            {"name": "TM-IV",  "planet_type": "Naine glacee","distance_au": 5.0,  "radius_km": 2500,  "mass_earth": 0.06, "gravity_g": 0.06, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 99, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Borderland", "star_type": "Sous-geante", "star_age_gy": 3.5,
        "planets": [
            {"name": "Border-I",   "planet_type": "Tellurique",  "distance_au": 1.1,  "radius_km": 6100,  "mass_earth": 0.87, "gravity_g": 0.9,  "atmosphere_o2": 11, "atmosphere_co2": 4,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},   # inconnue
            {"name": "Border-II",  "planet_type": "Ocean",       "distance_au": 1.55, "radius_km": 7000,  "mass_earth": 1.12, "gravity_g": 1.06, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 1},   # habitable
            {"name": "Border-III", "planet_type": "Super-Terre", "distance_au": 2.0,  "radius_km": 9200,  "mass_earth": 2.6,  "gravity_g": 1.85, "atmosphere_o2": 9,  "atmosphere_co2": 8,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 0},   # inconnue
            {"name": "Border-IV",  "planet_type": "Gazeuse",     "distance_au": 7.2,  "radius_km": 60000, "mass_earth": 285,  "gravity_g": 2.35, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 20},
        ]
    },
    {
        "system_name": "Penumbra Zone", "star_type": "Naine rouge", "star_age_gy": 8.1,
        "planets": [
            {"name": "Penumbra-I",  "planet_type": "Tellurique",  "distance_au": 0.11, "radius_km": 5700,  "mass_earth": 0.77, "gravity_g": 0.82, "atmosphere_o2": 9,  "atmosphere_co2": 5,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 0},   # inconnue
            {"name": "Penumbra-II", "planet_type": "Ocean",       "distance_au": 0.16, "radius_km": 6600,  "mass_earth": 1.0,  "gravity_g": 0.98, "atmosphere_o2": 6,  "atmosphere_co2": 4,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},   # inconnue
            {"name": "Penumbra-III","planet_type": "Naine glacee","distance_au": 0.8,  "radius_km": 2200,  "mass_earth": 0.04, "gravity_g": 0.05, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 97, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
 
    # ── Non-habitables variés (diversité du dataset) ──────────────────────────
    {
        "system_name": "Inferno Basin", "star_type": "Naine jaune", "star_age_gy": 2.5,
        "planets": [
            {"name": "Inferno-I",   "planet_type": "Lave",        "distance_au": 0.15, "radius_km": 4000,  "mass_earth": 0.5,  "gravity_g": 0.6,  "atmosphere_o2": 0,  "atmosphere_co2": 92, "atmosphere_n2": 7,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Inferno-II",  "planet_type": "Tellurique",  "distance_au": 0.5,  "radius_km": 5800,  "mass_earth": 0.78, "gravity_g": 0.83, "atmosphere_o2": 0,  "atmosphere_co2": 80, "atmosphere_n2": 18, "has_water": False, "magnetic_field": False, "moons": 0},  # trop chaud, pas O2
            {"name": "Inferno-III", "planet_type": "Gazeuse",     "distance_au": 3.5,  "radius_km": 59000, "mass_earth": 280,  "gravity_g": 2.35, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 16},
        ]
    },
    {
        "system_name": "Frostbound", "star_type": "Naine orange", "star_age_gy": 7.0,
        "planets": [
            {"name": "Frost-I",  "planet_type": "Naine glacee", "distance_au": 0.05, "radius_km": 3200,  "mass_earth": 0.15, "gravity_g": 0.2,  "atmosphere_o2": 0,  "atmosphere_co2": 3,  "atmosphere_n2": 94, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Frost-II", "planet_type": "Tellurique",   "distance_au": 2.5,  "radius_km": 6000,  "mass_earth": 0.82, "gravity_g": 0.86, "atmosphere_o2": 5,  "atmosphere_co2": 2,  "atmosphere_n2": 90, "has_water": False, "magnetic_field": False, "moons": 0},  # trop froid, hors HZ
            {"name": "Frost-III","planet_type": "Naine glacee", "distance_au": 8.0,  "radius_km": 2800,  "mass_earth": 0.09, "gravity_g": 0.09, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 99, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Toxic Cloud", "star_type": "Naine jaune", "star_age_gy": 5.8,
        "planets": [
            {"name": "Toxic-I",   "planet_type": "Lave",        "distance_au": 0.25, "radius_km": 4100,  "mass_earth": 0.52, "gravity_g": 0.63, "atmosphere_o2": 0,  "atmosphere_co2": 90, "atmosphere_n2": 9,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Toxic-II",  "planet_type": "Tellurique",  "distance_au": 0.85, "radius_km": 6200,  "mass_earth": 0.88, "gravity_g": 0.91, "atmosphere_o2": 0,  "atmosphere_co2": 75, "atmosphere_n2": 24, "has_water": False, "magnetic_field": False, "moons": 0},  # Venere-like
            {"name": "Toxic-III", "planet_type": "Ocean",       "distance_au": 1.2,  "radius_km": 6900,  "mass_earth": 1.05, "gravity_g": 1.02, "atmosphere_o2": 3,  "atmosphere_co2": 15, "atmosphere_n2": 75, "has_water": True,  "magnetic_field": True,  "moons": 1},  # inconnue: co2 élevé
            {"name": "Toxic-IV",  "planet_type": "Gazeuse",     "distance_au": 5.5,  "radius_km": 61000, "mass_earth": 295,  "gravity_g": 2.42, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 19},
        ]
    },
    {
        "system_name": "Dusty Fringe", "star_type": "Naine orange", "star_age_gy": 4.2,
        "planets": [
            {"name": "Dusty-I",  "planet_type": "Tellurique",  "distance_au": 0.3,  "radius_km": 5400,  "mass_earth": 0.68, "gravity_g": 0.75, "atmosphere_o2": 1,  "atmosphere_co2": 55, "atmosphere_n2": 40, "has_water": False, "magnetic_field": False, "moons": 0},  # Mars-like
            {"name": "Dusty-II", "planet_type": "Tellurique",  "distance_au": 0.55, "radius_km": 5900,  "mass_earth": 0.8,  "gravity_g": 0.84, "atmosphere_o2": 14, "atmosphere_co2": 6,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": False, "moons": 0},  # pas de champ mag
            {"name": "Dusty-III","planet_type": "Gazeuse",     "distance_au": 3.2,  "radius_km": 57000, "mass_earth": 265,  "gravity_g": 2.25, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 14},
        ]
    },
    {
        "system_name": "Gravel Storm", "star_type": "Sous-geante", "star_age_gy": 2.8,
        "planets": [
            {"name": "Gravel-I",   "planet_type": "Lave",        "distance_au": 0.28, "radius_km": 4500,  "mass_earth": 0.62, "gravity_g": 0.72, "atmosphere_o2": 0,  "atmosphere_co2": 86, "atmosphere_n2": 13, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Gravel-II",  "planet_type": "Super-Terre", "distance_au": 0.95, "radius_km": 8800,  "mass_earth": 2.3,  "gravity_g": 1.74, "atmosphere_o2": 0,  "atmosphere_co2": 70, "atmosphere_n2": 28, "has_water": False, "magnetic_field": False, "moons": 0},  # pas d'O2 ni eau
            {"name": "Gravel-III", "planet_type": "Gazeuse",     "distance_au": 5.0,  "radius_km": 60000, "mass_earth": 285,  "gravity_g": 2.38, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 18},
        ]
    },
    {
        "system_name": "Null Void", "star_type": "Geante bleue", "star_age_gy": 0.02,
        "planets": [
            {"name": "Null-I",  "planet_type": "Lave",         "distance_au": 10.0,  "radius_km": 6500,  "mass_earth": 1.2,  "gravity_g": 1.1,  "atmosphere_o2": 0,  "atmosphere_co2": 60, "atmosphere_n2": 25, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Null-II", "planet_type": "Gazeuse",      "distance_au": 100.0, "radius_km": 72000, "mass_earth": 450,  "gravity_g": 3.2,  "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 35},
        ]
    },
    {
        "system_name": "Iron Tomb", "star_type": "Geante rouge", "star_age_gy": 7.5,
        "planets": [
            {"name": "Iron-I",   "planet_type": "Lave",        "distance_au": 6.0,  "radius_km": 6800,  "mass_earth": 1.2,  "gravity_g": 1.15, "atmosphere_o2": 0,  "atmosphere_co2": 78, "atmosphere_n2": 19, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Iron-II",  "planet_type": "Gazeuse",     "distance_au": 18.0, "radius_km": 63000, "mass_earth": 310,  "gravity_g": 2.48, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 26},
            {"name": "Iron-III", "planet_type": "Naine glacee","distance_au": 50.0, "radius_km": 3000,  "mass_earth": 0.11, "gravity_g": 0.11, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 98, "has_water": False, "magnetic_field": False, "moons": 1},
        ]
    },
    {
        "system_name": "Static Relic", "star_type": "Naine blanche", "star_age_gy": 5.2,
        "planets": [
            {"name": "Static-I",  "planet_type": "Naine glacee", "distance_au": 0.015,"radius_km": 3200,  "mass_earth": 0.16, "gravity_g": 0.2,  "atmosphere_o2": 0,  "atmosphere_co2": 3,  "atmosphere_n2": 93, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Static-II", "planet_type": "Tellurique",   "distance_au": 0.06, "radius_km": 5600,  "mass_earth": 0.75, "gravity_g": 0.81, "atmosphere_o2": 0,  "atmosphere_co2": 4,  "atmosphere_n2": 93, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Static-III","planet_type": "Naine glacee", "distance_au": 0.3,  "radius_km": 3800,  "mass_earth": 0.24, "gravity_g": 0.29, "atmosphere_o2": 0,  "atmosphere_co2": 1,  "atmosphere_n2": 96, "has_water": False, "magnetic_field": False, "moons": 1},
        ]
    },
 
    # ── Systèmes mixtes (diversité supplémentaire) ────────────────────────────
    {
        "system_name": "Cascade Blue", "star_type": "Naine jaune", "star_age_gy": 8.1,
        "planets": [
            {"name": "Cascade-I",   "planet_type": "Lave",        "distance_au": 0.38, "radius_km": 4400,  "mass_earth": 0.63, "gravity_g": 0.73, "atmosphere_o2": 0,  "atmosphere_co2": 87, "atmosphere_n2": 12, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Cascade-II",  "planet_type": "Tellurique",  "distance_au": 0.97, "radius_km": 6350,  "mass_earth": 0.93, "gravity_g": 0.94, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Cascade-III", "planet_type": "Ocean",       "distance_au": 1.28, "radius_km": 7050,  "mass_earth": 1.14, "gravity_g": 1.07, "atmosphere_o2": 20, "atmosphere_co2": 1,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Cascade-IV",  "planet_type": "Super-Terre", "distance_au": 1.85, "radius_km": 9400,  "mass_earth": 2.7,  "gravity_g": 1.87, "atmosphere_o2": 11, "atmosphere_co2": 7,  "atmosphere_n2": 80, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Cascade-V",   "planet_type": "Gazeuse",     "distance_au": 6.0,  "radius_km": 62000, "mass_earth": 300,  "gravity_g": 2.45, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 21},
        ]
    },
    {
        "system_name": "Palidor Watch", "star_type": "Naine orange", "star_age_gy": 7.6,
        "planets": [
            {"name": "Palidor-I",   "planet_type": "Lave",        "distance_au": 0.11, "radius_km": 3700,  "mass_earth": 0.47, "gravity_g": 0.57, "atmosphere_o2": 0,  "atmosphere_co2": 92, "atmosphere_n2": 7,  "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Palidor-II",  "planet_type": "Ocean",       "distance_au": 0.43, "radius_km": 6750,  "mass_earth": 1.02, "gravity_g": 0.99, "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Palidor-III", "planet_type": "Tellurique",  "distance_au": 0.62, "radius_km": 6050,  "mass_earth": 0.85, "gravity_g": 0.88, "atmosphere_o2": 18, "atmosphere_co2": 2,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 0},
            {"name": "Palidor-IV",  "planet_type": "Super-Terre", "distance_au": 0.9,  "radius_km": 9100,  "mass_earth": 2.4,  "gravity_g": 1.79, "atmosphere_o2": 10, "atmosphere_co2": 6,  "atmosphere_n2": 82, "has_water": True,  "magnetic_field": True,  "moons": 1},  # inconnue
            {"name": "Palidor-V",   "planet_type": "Gazeuse",     "distance_au": 4.5,  "radius_km": 60000, "mass_earth": 288,  "gravity_g": 2.38, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 17},
        ]
    },
    {
        "system_name": "Halcyon Deep", "star_type": "Sous-geante", "star_age_gy": 6.1,
        "planets": [
            {"name": "Halcyon-I",   "planet_type": "Lave",        "distance_au": 0.42, "radius_km": 4900,  "mass_earth": 0.7,  "gravity_g": 0.8,  "atmosphere_o2": 0,  "atmosphere_co2": 84, "atmosphere_n2": 15, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Halcyon-II",  "planet_type": "Tellurique",  "distance_au": 1.08, "radius_km": 6300,  "mass_earth": 0.91, "gravity_g": 0.93, "atmosphere_o2": 20, "atmosphere_co2": 2,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Halcyon-III", "planet_type": "Ocean",       "distance_au": 1.52, "radius_km": 7150,  "mass_earth": 1.2,  "gravity_g": 1.11, "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Halcyon-IV",  "planet_type": "Gazeuse",     "distance_au": 6.8,  "radius_km": 59000, "mass_earth": 282,  "gravity_g": 2.33, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 19},
            {"name": "Halcyon-V",   "planet_type": "Naine glacee","distance_au": 16.0, "radius_km": 2650,  "mass_earth": 0.08, "gravity_g": 0.08, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 99, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
    {
        "system_name": "Zephyr Cradle", "star_type": "Naine jaune", "star_age_gy": 5.7,
        "planets": [
            {"name": "Zephyr-I",   "planet_type": "Lave",        "distance_au": 0.26, "radius_km": 4050,  "mass_earth": 0.53, "gravity_g": 0.64, "atmosphere_o2": 0,  "atmosphere_co2": 89, "atmosphere_n2": 10, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Zephyr-II",  "planet_type": "Ocean",       "distance_au": 0.98, "radius_km": 6950,  "mass_earth": 1.08, "gravity_g": 1.04, "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Zephyr-III", "planet_type": "Tellurique",  "distance_au": 1.32, "radius_km": 6200,  "mass_earth": 0.9,  "gravity_g": 0.91, "atmosphere_o2": 19, "atmosphere_co2": 2,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Zephyr-IV",  "planet_type": "Super-Terre", "distance_au": 1.9,  "radius_km": 9300,  "mass_earth": 2.65, "gravity_g": 1.86, "atmosphere_o2": 8,  "atmosphere_co2": 9,  "atmosphere_n2": 81, "has_water": True,  "magnetic_field": True,  "moons": 0},  # inconnue
            {"name": "Zephyr-V",   "planet_type": "Gazeuse",     "distance_au": 5.8,  "radius_km": 61000, "mass_earth": 295,  "gravity_g": 2.42, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 20},
        ]
    },
    {
        "system_name": "Opal Sanctum", "star_type": "Naine orange", "star_age_gy": 9.5,
        "planets": [
            {"name": "Opal-I",   "planet_type": "Tellurique",  "distance_au": 0.38, "radius_km": 5750,  "mass_earth": 0.77, "gravity_g": 0.82, "atmosphere_o2": 18, "atmosphere_co2": 2,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Opal-II",  "planet_type": "Ocean",       "distance_au": 0.53, "radius_km": 6850,  "mass_earth": 1.04, "gravity_g": 1.01, "atmosphere_o2": 21, "atmosphere_co2": 1,  "atmosphere_n2": 77, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Opal-III", "planet_type": "Super-Terre", "distance_au": 0.75, "radius_km": 9050,  "mass_earth": 2.35, "gravity_g": 1.76, "atmosphere_o2": 16, "atmosphere_co2": 4,  "atmosphere_n2": 79, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Opal-IV",  "planet_type": "Gazeuse",     "distance_au": 4.2,  "radius_km": 60500, "mass_earth": 292,  "gravity_g": 2.41, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 18},
        ]
    },
    {
        "system_name": "Marble Watch", "star_type": "Sous-geante", "star_age_gy": 5.5,
        "planets": [
            {"name": "Marble-I",   "planet_type": "Lave",        "distance_au": 0.38, "radius_km": 4750,  "mass_earth": 0.68, "gravity_g": 0.77, "atmosphere_o2": 0,  "atmosphere_co2": 85, "atmosphere_n2": 14, "has_water": False, "magnetic_field": False, "moons": 0},
            {"name": "Marble-II",  "planet_type": "Ocean",       "distance_au": 1.18, "radius_km": 7050,  "mass_earth": 1.16, "gravity_g": 1.09, "atmosphere_o2": 22, "atmosphere_co2": 1,  "atmosphere_n2": 76, "has_water": True,  "magnetic_field": True,  "moons": 2},
            {"name": "Marble-III", "planet_type": "Tellurique",  "distance_au": 1.62, "radius_km": 6280,  "mass_earth": 0.91, "gravity_g": 0.93, "atmosphere_o2": 19, "atmosphere_co2": 2,  "atmosphere_n2": 78, "has_water": True,  "magnetic_field": True,  "moons": 1},
            {"name": "Marble-IV",  "planet_type": "Gazeuse",     "distance_au": 6.5,  "radius_km": 62000, "mass_earth": 308,  "gravity_g": 2.48, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 0,  "has_water": False, "magnetic_field": True,  "moons": 22},
            {"name": "Marble-V",   "planet_type": "Naine glacee","distance_au": 17.0, "radius_km": 2600,  "mass_earth": 0.08, "gravity_g": 0.08, "atmosphere_o2": 0,  "atmosphere_co2": 0,  "atmosphere_n2": 99, "has_water": False, "magnetic_field": False, "moons": 0},
        ]
    },
]



# ─── CONSTRUCTION DES DATAFRAMES ─────────────────────────────────────────────
star_rows   = []
planet_rows = []

for system in systems_raw:
    stype      = system["star_type"]
    sinfo      = STAR_TYPES[stype]
    luminosity = sinfo["luminosity"]
    hz_in, hz_out = habitable_zone(luminosity)

    star_rows.append({
        "system_name":      system["system_name"],
        "star_type":        stype,
        "star_luminosity":  luminosity,
        "star_temp_k":      sinfo["temp_k"],
        "star_mass_solar":  sinfo["mass"],
        "star_age_gy":      system["star_age_gy"],
        "star_lifespan_gy": sinfo["lifespan_gy"],
        "star_color":       sinfo["color"],
        "xuv_factor":       sinfo["xuv_factor"],
        "flare_risk":       sinfo["flare_risk"],
        "hz_inner_au":      hz_in,
        "hz_outer_au":      hz_out,
    })

    for p in system["planets"]:
        ptype      = p["planet_type"]
        ptype_info = PLANET_TYPES[ptype]
        albedo     = ptype_info["base_albedo"]
        temp_c     = compute_temperature(luminosity, p["distance_au"], albedo)
        in_hz      = (hz_in <= p["distance_au"] <= hz_out)
        s_flux     = stellar_flux(luminosity, p["distance_au"])
        tidal      = is_tidally_locked(p["distance_au"], sinfo["tidal_lock_au"])

        # Facteur pression atmosphérique (protection contre radiations)
        # Basé sur N2 + présence d'atmosphère dense
        atm_n2    = p["atmosphere_n2"]
        atm_press = max(0.01, atm_n2 / 78.0)  # relatif à la Terre (78% N2)

        surf_rad = compute_surface_radiation(
            luminosity, p["distance_au"],
            sinfo["xuv_factor"], p["magnetic_field"], atm_press
        )

        row = {
            # ── Étoile ──────────────────────────────────────────────────
            "system_name":             system["system_name"],
            "star_type":               stype,
            "star_luminosity":         luminosity,
            "star_age_gy":             system["star_age_gy"],
            "xuv_factor":              sinfo["xuv_factor"],
            "flare_risk":              sinfo["flare_risk"],

            # ── Planète ─────────────────────────────────────────────────
            "name":                    p["name"],
            "planet_type":             ptype,
            "distance_au":             p["distance_au"],
            "radius_km":               p["radius_km"],
            "mass_earth":              p["mass_earth"],
            "gravity_g":               p["gravity_g"],

            # ── Physique calculée ───────────────────────────────────────
            "avg_temp_celsius":        temp_c,
            "stellar_flux":            s_flux,
            "surface_radiation_msv_h": surf_rad,
            "tidally_locked":          tidal,
            "retains_atmosphere":      can_retain_atmosphere(p["gravity_g"], temp_c),

            # ── Atmosphère ──────────────────────────────────────────────
            "atmosphere_o2":           p["atmosphere_o2"],
            "atmosphere_co2":          p["atmosphere_co2"],
            "atmosphere_n2":           p["atmosphere_n2"],

            # ── Conditions de surface ───────────────────────────────────
            "has_water":               p["has_water"],
            "magnetic_field":          p["magnetic_field"],
            "moons":                   p["moons"],

            # ── Zone habitable ──────────────────────────────────────────
            "in_habitable_zone":       in_hz,
            "albedo":                  albedo,
            "habitable_candidate":     ptype_info["habitable_candidate"],

            # ── Colonnes int pour ML ────────────────────────────────────
            "has_water_int":           int(p["has_water"]),
            "magnetic_field_int":      int(p["magnetic_field"]),
            "in_hz_int":               int(in_hz),
            "habitable_candidate_int": int(ptype_info["habitable_candidate"]),
            "tidally_locked_int":      int(tidal),
            "retains_atmosphere_int":  int(can_retain_atmosphere(p["gravity_g"], temp_c)),
        }

        row["label"] = assign_label(row)
        planet_rows.append(row)


df_stars   = pd.DataFrame(star_rows)
df_planets = pd.DataFrame(planet_rows)

# ─── SAUVEGARDE ──────────────────────────────────────────────────────────────
df_stars.to_csv("star_systems.csv", index=False)
df_planets.to_csv("planets.csv", index=False)

print(f"✅ {len(df_stars)} systèmes stellaires → star_systems.csv")
print(f"✅ {len(df_planets)} planètes → planets.csv\n")

print("📊 Distribution des labels :")
print(df_planets["label"].value_counts().to_string())

print("\n📊 Détail par type d'étoile :")
print(df_planets.groupby(["star_type", "label"]).size().to_string())

print("\n🌡️  Aperçu physique des planètes candidates :")
candidates = df_planets[df_planets["habitable_candidate"] == True]
print(candidates[[
    "name", "star_type", "distance_au", "avg_temp_celsius",
    "stellar_flux", "surface_radiation_msv_h", "tidally_locked",
    "in_habitable_zone", "planet_type", "label"
]].to_string(index=False))

print("\n☢️  Radiations par système :")
print(df_planets[["name", "star_type", "distance_au",
                   "surface_radiation_msv_h", "magnetic_field", "label"]]
      .sort_values("surface_radiation_msv_h", ascending=False)
      .head(20).to_string(index=False))