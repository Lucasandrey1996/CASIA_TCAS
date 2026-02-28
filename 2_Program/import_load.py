"""
import_load.py — Chargement et analyse des données SST

Charge tous les CSV d'export SST, les structure en DataFrame pandas
avec index timestamp_utc au quart d'heure (00/15/30/45).
Ajoute température extérieure (Bulle) et exporte en Parquet.
"""

from pathlib import Path
import logging
import json
from typing import Literal
from urllib.request import urlopen
from urllib.error import URLError

import pandas as pd

# =============================================================================
# CONFIGURATION
# =============================================================================

PATH_RAW = Path(__file__).resolve().parent / "0_Data" / "0_Raw" / "ExportSST" / "export_SSTCAD_20260227"
PATH_LOGS = Path(__file__).resolve().parent / "97_logs"
PATH_STRUCTURED = Path(__file__).resolve().parent / "0_Data" / "1_Structured"

# Fichiers de log
LOG_FILE_HOURLY = "fichiers_echantillon_horaire_ou_plus.txt"
LOG_FILE_DUPLICATES = "colonnes_dupliquees.txt"
OUTPUT_PARQUET = "sst_unified.parquet"

# Bulle (Suisse) : 46° 37′ 03″ N, 7° 03′ 29″ E
BULLE_LAT = 46.6175
BULLE_LON = 7.0581

# Types d'échantillonnage
SamplingType = Literal["minute", "15min", "hour", "coarser"]

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================


def _ensure_dirs() -> None:
    """Crée les dossiers nécessaires."""
    PATH_LOGS.mkdir(parents=True, exist_ok=True)
    PATH_STRUCTURED.mkdir(parents=True, exist_ok=True)


def _parse_date_column(series: pd.Series) -> pd.DatetimeIndex:
    """Parse la colonne date au format DD/MM/YYYY HH:MM (CET/CEST)."""
    return pd.to_datetime(series, format="%d/%m/%Y %H:%M", errors="coerce")


def detect_sampling_interval(df: pd.DataFrame, date_col: str = "date") -> SamplingType:
    """
    Détecte l'intervalle d'échantillonnage à partir des timestamps.

    Returns:
        'minute' : ~1 min
        '15min' : ~15 min
        'hour' : ~1 h
        'coarser' : > 1 h
    """
    if df.empty or len(df) < 2:
        return "coarser"

    dates = _parse_date_column(df[date_col])
    dates = dates.dropna().sort_values()
    if len(dates) < 2:
        return "coarser"

    deltas = dates.diff().dropna()
    if deltas.empty:
        return "coarser"

    median_delta = deltas.median()
    minutes = median_delta.total_seconds() / 60

    if minutes <= 2:
        return "minute"
    if minutes <= 20:
        return "15min"
    if minutes <= 90:
        return "hour"
    return "coarser"


def load_and_process_csv(filepath: Path) -> tuple[pd.DataFrame | None, str | None, SamplingType | None]:
    """
    Charge un CSV, applique la validité (inv) et détecte l'échantillonnage.

    Returns:
        (DataFrame avec date, valeur), nom_colonne, sampling
        ou (None, None, None) en cas d'erreur.
    """
    try:
        df = pd.read_csv(filepath, sep=";", encoding="utf-8", on_bad_lines="warn")
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        logger.warning("Fichier %s ignoré (vide ou format invalide) : %s", filepath.name, e)
        return None, None, None
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(filepath, sep=";", encoding="cp1252", on_bad_lines="warn")
        except Exception as e:
            logger.error("Erreur lecture %s : %s", filepath.name, e)
            return None, None, None
    except Exception as e:
        logger.error("Erreur lecture %s : %s", filepath.name, e)
        return None, None, None

    if df.shape[1] < 3:
        logger.warning("Fichier %s : moins de 3 colonnes, ignoré", filepath.name)
        return None, None, None

    date_col = df.columns[0]
    value_col = df.columns[1]
    inv_col = df.columns[2]
    col_name = value_col

    mask_invalid = pd.to_numeric(df[inv_col], errors="coerce").fillna(0) != 0
    df.loc[mask_invalid, value_col] = 0.0

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce").fillna(0.0)

    df["date"] = _parse_date_column(df[date_col])
    df = df.dropna(subset=["date"])

    if df.empty:
        logger.warning("Fichier %s : aucune date valide", filepath.name)
        return None, None, None

    sampling = detect_sampling_interval(df)

    return df[["date", value_col]].rename(columns={value_col: "valeur"}), col_name, sampling


def aggregate_to_quarter_hour(
    df: pd.DataFrame,
    col_name: str,
    sampling: SamplingType,
) -> pd.DataFrame:
    """
    Agrège les données au quart d'heure (00/15/30/45).
    Les dates sont en heure locale (CET/CEST).
    """
    df = df.copy()
    df = df.set_index("date").sort_index()

    if sampling == "15min":
        df.index = df.index.floor("15min")
        agg = df.groupby(level=0)["valeur"].mean()
    elif sampling == "minute":
        quarters = df.resample("15min", label="right", closed="right").mean()
        agg = quarters["valeur"]
    else:
        return pd.DataFrame()

    result = pd.DataFrame({col_name: agg})
    result.index.name = "timestamp_loc"
    return result


def localize_to_utc(ts: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """
    Convertit des timestamps heure locale (CET/CEST) en UTC.
    - Heures ambiguës (passage à l'hiver) : première occurrence (CEST).
    - Heures inexistantes (passage à l'été) : décalage vers l'avant (02:00 → 03:00).
    """
    return ts.tz_localize(
        "Europe/Zurich",
        ambiguous=True,
        nonexistent="shift_forward",
    ).tz_convert("UTC")


def fetch_temp_ext_bulle(start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> pd.Series:
    """
    Récupère la température extérieure pour Bulle via Open-Meteo (fallback si MétéoSuisse complexe).
    Données horaires, répliquées sur les quarts d'heure.
    """
    start_str = start_utc.strftime("%Y-%m-%d")
    end_str = end_utc.strftime("%Y-%m-%d")

    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={BULLE_LAT}&longitude={BULLE_LON}"
        f"&start_date={start_str}&end_date={end_str}"
        "&hourly=temperature_2m"
        "&timezone=UTC"
    )

    try:
        with urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except (URLError, TimeoutError, json.JSONDecodeError) as e:
        logger.warning("Impossible de récupérer temp_ext (Open-Meteo) : %s", e)
        return pd.Series(dtype=float)

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])

    if not times or not temps:
        return pd.Series(dtype=float)

    df = pd.DataFrame({"time": times, "temp": temps})
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.set_index("time")

    # Réindexer sur quarts d'heure : forward-fill des valeurs horaires
    full_range = pd.date_range(start=start_utc, end=end_utc, freq="15min", tz="UTC")
    series = df["temp"].reindex(full_range, method="ffill")
    series = series.fillna(0.0)

    return series


def log_hourly_file(filename: str) -> None:
    """Enregistre un fichier échantillonné à l'heure ou plus dans le log."""
    _ensure_dirs()
    log_path = PATH_LOGS / LOG_FILE_HOURLY
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{filename}\n")
    except OSError as e:
        logger.error("Impossible d'écrire dans %s : %s", log_path, e)


def log_duplicate_column(original: str, renamed: str) -> None:
    """Enregistre un doublon de colonne dans le log."""
    _ensure_dirs()
    log_path = PATH_LOGS / LOG_FILE_DUPLICATES
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{original} -> {renamed}\n")
    except OSError as e:
        logger.error("Impossible d'écrire dans %s : %s", log_path, e)


# =============================================================================
# FONCTION PRINCIPALE
# =============================================================================


def build_unified_dataframe() -> pd.DataFrame:
    """
    Charge tous les CSV, les agrège au quart d'heure et fusionne en un DataFrame.
    Index : timestamp_utc. Colonnes : timestamp_loc, temp_ext_api, + une par fichier.
    """
    if not PATH_RAW.exists():
        raise FileNotFoundError(f"Dossier source introuvable : {PATH_RAW}")

    csv_files = list(PATH_RAW.glob("*.csv"))
    if not csv_files:
        logger.warning("Aucun fichier CSV trouvé dans %s", PATH_RAW)

    _ensure_dirs()

    # Réinitialiser les fichiers de log
    for log_name in (LOG_FILE_HOURLY, LOG_FILE_DUPLICATES):
        log_path = PATH_LOGS / log_name
        if log_path.exists():
            log_path.unlink()

    frames: list[pd.DataFrame] = []
    all_timestamps: set[pd.Timestamp] = set()

    for filepath in sorted(csv_files):
        result, col_name, sampling = load_and_process_csv(filepath)
        if result is None or col_name is None or sampling is None:
            continue

        if sampling in ("hour", "coarser"):
            log_hourly_file(filepath.name)
            logger.info("Fichier %s exclu (échantillonnage >= 1h)", filepath.name)
            continue

        df_agg = aggregate_to_quarter_hour(result, col_name, sampling)
        if df_agg.empty:
            continue

        existing_cols = [c for f in frames for c in f.columns]
        unique_col = col_name
        suffix = 0
        while unique_col in existing_cols:
            suffix += 1
            unique_col = f"{col_name}_{suffix}"
            log_duplicate_column(col_name, unique_col)
        if unique_col != col_name:
            df_agg = df_agg.rename(columns={col_name: unique_col})

        frames.append(df_agg)
        all_timestamps.update(df_agg.index.tolist())

    if not frames:
        logger.warning("Aucune donnée valide à fusionner")
        return pd.DataFrame()

    if not all_timestamps:
        return pd.DataFrame()

    ts_min = min(all_timestamps).floor("15min")
    ts_max = max(all_timestamps).ceil("15min")
    full_index_loc = pd.date_range(start=ts_min, end=ts_max, freq="15min", name="timestamp_loc")

    merged = pd.DataFrame(index=full_index_loc)
    for df in frames:
        merged = merged.join(df, how="left")

    merged = merged.fillna(0.0)

    # Conversion en UTC pour l'index
    merged.index = localize_to_utc(merged.index)
    merged.index.name = "timestamp_utc"

    # Colonne timestamp_loc pour les graphiques (heure locale)
    merged["timestamp_loc"] = merged.index.tz_convert("Europe/Zurich")

    # Température extérieure
    temp_ext = fetch_temp_ext_bulle(merged.index.min(), merged.index.max())
    merged["temp_ext_api"] = temp_ext.reindex(merged.index).fillna(0.0).values

    # Réordonner : timestamp_loc et temp_ext_api en premier
    cols = ["timestamp_loc", "temp_ext_api"] + [c for c in merged.columns if c not in ("timestamp_loc", "temp_ext_api")]
    merged = merged[cols]

    logger.info("DataFrame construit : %d lignes, %d colonnes", len(merged), len(merged.columns))
    return merged


def export_to_parquet(df: pd.DataFrame) -> Path:
    """Exporte le DataFrame en Parquet."""
    _ensure_dirs()
    output_path = PATH_STRUCTURED / OUTPUT_PARQUET
    df.to_parquet(output_path, index=True)
    logger.info("Export Parquet : %s", output_path)
    return output_path


# =============================================================================
# POINT D'ENTRÉE
# =============================================================================


if __name__ == "__main__":
    try:
        df = build_unified_dataframe()
        if not df.empty:
            print(df.head(20))
            export_to_parquet(df)
    except Exception as e:
        logger.exception("Erreur lors de l'exécution : %s", e)
        raise
