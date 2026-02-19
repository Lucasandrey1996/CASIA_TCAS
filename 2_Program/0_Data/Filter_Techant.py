"""
Génère config_export_controlCAD.csv à partir des points de transmission CAD
et de la liste des techant, en appliquant les filtres définis.
"""

import pandas as pd
from pathlib import Path

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

# Chemins des fichiers
BASE_DIR = Path(__file__).resolve().parent
EXCEL_PATH = BASE_DIR / "0_Raw" / "RefFiles" / "Point_transmission_CAD_20260202.xlsx"
TECHANT_PATH = BASE_DIR / "0_Raw" / "liste_techant" / "20260219_techant.csv"
OUTPUT_PATH = BASE_DIR / "0_Raw" / "liste_techant" / "config_export_controlCAD.csv"

# Définition des filtres : (champ, libelle_2, frequence, type, is_temperature)
# libelle_2=None signifie pas de filtre sur libelle_2
FILTERS = [
    #("CH1_2B4 SONDE DE TEMPERATURE", None, "15 MINUTES", "Valeur moyenne", True),
    ("CH1_1C2 COMPTEUR", "TEMPERATURE RETOUR", "15 MINUTES", "Valeur moyenne", True),
    ("CH1_2Y7 VANNE", None, "15 MINUTES", "Valeur maximum", False),
    ("CH2_4Y4 VANNE", None, "15 MINUTES", "Valeur maximum", False),
    ("CH3_5Y4 VANNE", None, "15 MINUTES", "Valeur maximum", False),
    ("CH4_6Y4 VANNE", None, "15 MINUTES", "Valeur maximum", False),
]


def _champ_matches(champ: str, filter_champ: str) -> bool:
    """Vérifie si le champ correspond (exact ou préfixe pour VANNE 2 VOIES)."""
    return champ == filter_champ or champ.startswith(f"{filter_champ} ")


def main() -> None:
    # Chargement des données
    df_cad = pd.read_excel(EXCEL_PATH)
    if "U_NO_EGID" not in df_cad.columns:
        raise ValueError(f"Colonne U_NO_EGID absente dans {EXCEL_PATH}")

    df_techant = pd.read_csv(TECHANT_PATH, sep=";", dtype=str)
    required_cols = ["ref_techant", "ouvrage", "champ", "libelle_2", "frequence", "type"]
    for col in required_cols:
        if col not in df_techant.columns:
            raise ValueError(f"Colonne {col} absente dans {TECHANT_PATH}")

    # Index des correspondances techant (une seule passe)
    techant_index = {}
    for _, row in df_techant.iterrows():
        ouvrage = str(row["ouvrage"]).strip()
        champ = str(row["champ"]).strip()
        libelle_2 = str(row["libelle_2"]).strip()
        frequence = str(row["frequence"]).strip()
        type_val = str(row["type"]).strip()

        for champ_f, libelle_2_f, freq_f, type_f, is_temp in FILTERS:
            if not _champ_matches(champ, champ_f):
                continue
            if libelle_2_f is not None and libelle_2 != libelle_2_f:
                continue
            if frequence != freq_f or type_val != type_f:
                continue
            key = (ouvrage, champ_f, libelle_2_f)
            if key not in techant_index:
                techant_index[key] = (row["ref_techant"], is_temp)
            break

    # Parcours des lignes Excel avec barre de progression
    results = []
    seen = set()
    rows_iter = df_cad.iterrows()
    if tqdm:
        rows_iter = tqdm(rows_iter, total=len(df_cad), desc="Traitement des points CAD")

    for _, row_cad in rows_iter:
        val = row_cad.get("U_NO_EGID")
        if pd.isna(val):
            continue
        name_nummer = str(val).strip()
        if not name_nummer:
            continue

        ouvrage = f"{name_nummer} SST"
        for champ_f, libelle_2_f, _, _, is_temp in FILTERS:
            key = (name_nummer, champ_f, libelle_2_f)
            if key in seen:
                continue
            techant_key = (ouvrage, champ_f, libelle_2_f)
            if techant_key in techant_index:
                seen.add(key)
                ref_techant, _ = techant_index[techant_key]
                suffix = "TempRet" if is_temp else "PosVan"
                results.append({
                    "Nom": f"{name_nummer}_{suffix}",
                    "Table": f"techant{ref_techant}",
                    "nbr": 120,
                })

    if not results:
        print("Aucune correspondance trouvée.")
        return

    out_df = pd.DataFrame(results)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUTPUT_PATH, sep=";", index=False)
    print(f"Fichier généré : {OUTPUT_PATH} ({len(results)} lignes)")


if __name__ == "__main__":
    main()
