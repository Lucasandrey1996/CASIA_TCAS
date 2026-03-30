# -*- coding: utf-8 -*-
"""
Exporte le cluster 3 (filtré, nettoyé, transfo) en CSV pour inspection.

Les parquets complets (~24 M lignes pour le cluster 3) sont trop volumineux en CSV :
par défaut, export d'un échantillon (--max-rows) en parcourant le fichier dans l'ordre
(premières lignes du cluster 3 rencontrées par batch).

Usage (depuis 2_Program) :
  .venv\\Scripts\\python.exe export_cluster3_csv_samples.py
  .venv\\Scripts\\python.exe export_cluster3_csv_samples.py --max-rows 800000
  .venv\\Scripts\\python.exe export_cluster3_csv_samples.py --summary-only
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
STRUCT = ROOT / "0_Data/1_Structured"
OUT_DIR = STRUCT / "cluster3_csv_export"
CLUSTER = 3

PATHS = {
    "filtered": STRUCT / "sst_filtered.parquet",
    "clean": STRUCT / "sst_filtered_clean.parquet",
    "transfo": STRUCT / "sst_filtered_transfo.parquet",
}

# Colonnes utiles pour visualiser le décalage TempRet / PuisCpt (évite CSV trop larges)
COLS_FILTERED_CLEAN = [
    "date",
    "date_15min",
    "EGID",
    "DATA_TYPE",
    "valeur",
    "inv",
    "cluster",
    "TempExt",
]

COLS_TRANSFO = COLS_FILTERED_CLEAN + [
    "TempExt_norm",
    "dayofyear_cos",
    "dayofyear_sin",
    "hour_cos",
    "hour_sin",
    "valeur_fc",
    "valeur_norm",
]


def available_columns(path: Path) -> set[str]:
    return set(pq.ParquetFile(path).schema_arrow.names)


def stream_cluster_sample_to_csv(
    path: Path,
    out_csv: Path,
    columns: list[str],
    cluster_id: int,
    max_rows: int,
) -> int:
    """Écrit au plus max_rows lignes (cluster_id) en scannant le parquet par row groups."""
    pf = pq.ParquetFile(path)
    cols = [c for c in columns if c in available_columns(path)]
    if "cluster" not in cols:
        raise ValueError(f"Colonne cluster manquante dans {path}")

    written = 0
    batches_out: list[pa.RecordBatch] = []

    for rg in range(pf.num_row_groups):
        t = pf.read_row_group(rg, columns=cols)
        fmask = pc.equal(t["cluster"], pa.scalar(cluster_id, type=t["cluster"].type))
        sub = pc.filter(t, fmask)
        if sub.num_rows == 0:
            continue
        for batch in sub.to_batches():
            if written >= max_rows:
                break
            take = min(batch.num_rows, max_rows - written)
            if take < batch.num_rows:
                batch = batch.slice(0, take)
            batches_out.append(batch)
            written += take
        if written >= max_rows:
            break

    if not batches_out:
        out_csv.write_text("no_data\n", encoding="utf-8")
        return 0

    out_tbl = pa.Table.from_batches(batches_out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    # pandas.to_csv évite les erreurs PyArrow « timezone database » sous Windows
    import pandas as pd

    df = out_tbl.to_pandas()
    for c in df.select_dtypes(include=["datetimetz"]).columns:
        df[c] = df[c].dt.tz_convert("UTC").dt.tz_localize(None)
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    return written


def write_egid_summary_transfo(path: Path, out_csv: Path, cluster_id: int) -> None:
    """Résumé par (EGID, DATA_TYPE) en parcourant le parquet par row groups (RAM maîtrisée)."""
    import pandas as pd

    cols = [
        "date",
        "EGID",
        "DATA_TYPE",
        "valeur_fc",
        "valeur_norm",
        "cluster",
    ]
    cols = [c for c in cols if c in available_columns(path)]
    pf = pq.ParquetFile(path)
    agg: dict[tuple[str, str], dict] = {}

    for rg in range(pf.num_row_groups):
        t = pf.read_row_group(rg, columns=cols)
        fmask = pc.equal(t["cluster"], pa.scalar(cluster_id, type=t["cluster"].type))
        sub = pc.filter(t, fmask)
        if sub.num_rows == 0:
            continue
        df = sub.to_pandas()
        df["EGID"] = df["EGID"].astype(str)
        for (eg, dt), g in df.groupby(["EGID", "DATA_TYPE"], sort=False):
            key = (eg, dt)
            dmn, dmx = g["date"].min(), g["date"].max()
            if key not in agg:
                agg[key] = {
                    "n_rows": 0,
                    "date_min": dmn,
                    "date_max": dmx,
                    "fc_ok": 0,
                    "fc_tot": 0,
                    "vn_ok": 0,
                    "vn_tot": 0,
                }
            a = agg[key]
            a["n_rows"] += len(g)
            a["date_min"] = min(a["date_min"], dmn)
            a["date_max"] = max(a["date_max"], dmx)
            if dt == "PuisCpt" and "valeur_fc" in g.columns:
                a["fc_ok"] += int(g["valeur_fc"].notna().sum())
                a["fc_tot"] += len(g)
            if dt == "TempRet" and "valeur_norm" in g.columns:
                a["vn_ok"] += int(g["valeur_norm"].notna().sum())
                a["vn_tot"] += len(g)

    if not agg:
        out_csv.write_text("no_data\n", encoding="utf-8")
        return

    rows = []
    for (eg, dt), a in sorted(agg.items(), key=lambda x: (x[0][1], x[0][0])):
        rec = {
            "EGID": eg,
            "DATA_TYPE": dt,
            "n_rows": a["n_rows"],
            "date_min": a["date_min"],
            "date_max": a["date_max"],
        }
        if dt == "PuisCpt" and a["fc_tot"]:
            rec["pct_valeur_fc_ok"] = round(100.0 * a["fc_ok"] / a["fc_tot"], 4)
        if dt == "TempRet" and a["vn_tot"]:
            rec["pct_valeur_norm_ok"] = round(100.0 * a["vn_ok"] / a["vn_tot"], 4)
        rows.append(rec)
    summary = pd.DataFrame(rows)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_csv, index=False, encoding="utf-8-sig")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-rows", type=int, default=500_000, help="Lignes max par CSV échantillon")
    ap.add_argument("--summary-only", action="store_true", help="Uniquement résumé par EGID/type (transfo)")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.summary_only:
        p = PATHS["transfo"]
        if not p.exists():
            raise SystemExit(f"Manquant : {p}")
        out = OUT_DIR / "cluster3_transfo_summary_by_egid_datatype.csv"
        write_egid_summary_transfo(p, out, CLUSTER)
        print(f"Écrit : {out}")
        return

    for stage, path in PATHS.items():
        if not path.exists():
            print(f"Ignoré (absent) : {path}")
            continue
        cols = COLS_TRANSFO if stage == "transfo" else COLS_FILTERED_CLEAN
        cols = [c for c in cols if c in available_columns(path)]
        out = OUT_DIR / f"cluster3_{stage}_sample_{args.max_rows}rows.csv"
        n = stream_cluster_sample_to_csv(path, out, cols, CLUSTER, args.max_rows)
        print(f"Écrit {n} lignes → {out}")

    # Résumé compact (plus léger à ouvrir)
    p = PATHS["transfo"]
    if p.exists():
        out = OUT_DIR / "cluster3_transfo_summary_by_egid_datatype.csv"
        write_egid_summary_transfo(p, out, CLUSTER)
        print(f"Écrit résumé → {out}")

    print(
        "\nNote : les *_sample_*.csv sont les premières lignes du cluster 3 dans l'ordre du parquet "
        f"(plafonnées à {args.max_rows}). Pour un autre tirage, augmentez --max-rows ou adaptez le script."
    )


if __name__ == "__main__":
    main()
