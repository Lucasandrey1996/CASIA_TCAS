"""
Génère des fichiers config_export_{suffix}_controlCAD.csv (un par type de donnée)
à partir des points de transmission CAD et de la liste des techant.
"""

from collections import defaultdict

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
OUTPUT_DIR = BASE_DIR / "0_Raw" / "liste_techant"

# Définition des filtres : (champ, libelle_2, frequence, type, suffix)
# libelle_2=None signifie pas de filtre sur libelle_2
# suffix : suffixe du Nom en sortie (TempRet, PosVan, PuisCpt, etc.)
FILTERS = [
    ("CH1_2B4 SONDE DE TEMPERATURE", None, "15 MINUTES", "Valeur moyenne", "TempRet"),
    ("CH1_2B4 SONDE DE TEMPERATURE COMMUN", None, "15 MINUTES", "Valeur moyenne", "TempRet"),
    ("CH1_1C2 COMPTEUR", "TEMPERATURE RETOUR", "15 MINUTES", "Valeur moyenne", "TempRet"),
    ("CH1_1C2 COMPTEUR", "PUISSANCE", "1 MINUTE", "Valeur moyenne", "PuisCpt"),
    ("CH1_2Y7 VANNE", None, "15 MINUTES", "Valeur maximum", "PosVan"),
    ("CH2_4Y4 VANNE", None, "15 MINUTES", "Valeur maximum", "PosVan"),
    ("CH3_5Y4 VANNE", None, "15 MINUTES", "Valeur maximum", "PosVan"),
    ("CH4_6Y4 VANNE", None, "15 MINUTES", "Valeur maximum", "PosVan"),
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

        for champ_f, libelle_2_f, freq_f, type_f, suffix in FILTERS:
            if not _champ_matches(champ, champ_f):
                continue
            if libelle_2_f is not None and libelle_2 != libelle_2_f:
                continue
            if frequence != freq_f or type_val != type_f:
                continue
            key = (ouvrage, champ_f, libelle_2_f)
            if key not in techant_index:
                techant_index[key] = (row["ref_techant"], suffix)
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
        for champ_f, libelle_2_f, _, _, suffix in FILTERS:
            key = (name_nummer, champ_f, libelle_2_f)
            if key in seen:
                continue
            techant_key = (ouvrage, champ_f, libelle_2_f)
            if techant_key in techant_index:
                seen.add(key)
                ref_techant, suffix = techant_index[techant_key]
                results.append({
                    "Nom": f"{name_nummer}_{suffix}",
                    "Table": f"techant{ref_techant}",
                    "nbr": 120,
                    "_suffix": suffix,
                })

    if not results:
        print("Aucune correspondance trouvée.")
        return

    # Regroupement par suffix et export d'un fichier par type
    results_by_suffix = defaultdict(list)
    for r in results:
        suffix = r.pop("_suffix")
        results_by_suffix[suffix].append(r)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for suffix, suffix_results in results_by_suffix.items():
        out_path = OUTPUT_DIR / f"config_export_{suffix}_controlCAD.csv"
        pd.DataFrame(suffix_results).to_csv(
            out_path, sep=";", index=False, lineterminator="\n"
        )
        print(f"Fichier généré : {out_path} ({len(suffix_results)} lignes)")


if __name__ == "__main__":
    main()
