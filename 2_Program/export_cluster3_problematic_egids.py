# -*- coding: utf-8 -*-
"""
Sélectionne les EGID du cluster 3 avec le plus grand écart (début PuisCpt − début TempRet)
et exporte toutes leurs lignes (filtré, nettoyé, transfo) en CSV.

Usage (depuis 2_Program) :
  .venv\\Scripts\\python.exe export_cluster3_problematic_egids.py
  .venv\\Scripts\\python.exe export_cluster3_problematic_egids.py --top 3
  .venv\\Scripts\\python.exe export_cluster3_problematic_egids.py --egids 1511188,190198380,235554367
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
STRUCT = ROOT / "0_Data/1_Structured"
OUT_DIR = STRUCT / "cluster3_csv_export" / "problematic_egids"
CLUSTER = 3

PATHS = {
    "filtered": STRUCT / "sst_filtered.parquet",
    "clean": STRUCT / "sst_filtered_clean.parquet",
    "transfo": STRUCT / "sst_filtered_transfo.parquet",
}

SUMMARY_CSV = STRUCT / "cluster3_csv_export" / "cluster3_transfo_summary_by_egid_datatype.csv"


def pick_top_problematic_from_summary(summary_path: Path, top_n: int) -> list[str]:
    s = pd.read_csv(summary_path)
    s["date_min"] = pd.to_datetime(s["date_min"])
    tr = s[s["DATA_TYPE"] == "TempRet"][["EGID", "date_min"]].rename(columns={"date_min": "tret_min"})
    pu = s[s["DATA_TYPE"] == "PuisCpt"][["EGID", "date_min"]].rename(columns={"date_min": "puis_min"})
    m = tr.merge(pu, on="EGID", how="inner")
    m["gap_days"] = (m["puis_min"] - m["tret_min"]).dt.days
    m = m.sort_values("gap_days", ascending=False)
    return m.head(top_n)["EGID"].astype(str).tolist()


def egid_scalar_type(sample_table: pa.Table, egids: list[str]) -> list:
    col = sample_table["EGID"]
    # convert egids to same type as parquet
    if pa.types.is_integer(col.type):
        return [int(e) for e in egids]
    return egids


def export_stage(path: Path, egids: list[str], out_csv: Path) -> int:
    if not path.exists():
        raise FileNotFoundError(path)
    pf = pq.ParquetFile(path)
    t0 = pf.read_row_group(0, columns=["EGID", "cluster"])
    egids_typed = egid_scalar_type(t0, egids)
    del t0

    chunks: list[pd.DataFrame] = []
    n_total = 0
    for rg in range(pf.num_row_groups):
        t = pf.read_row_group(rg)
        f1 = pc.equal(t["cluster"], pa.scalar(CLUSTER, type=t["cluster"].type))
        f2 = pc.is_in(t["EGID"], pa.array(egids_typed))
        sub = pc.filter(t, pc.and_(f1, f2))
        if sub.num_rows == 0:
            continue
        df = sub.to_pandas()
        for c in df.select_dtypes(include=["datetimetz"]).columns:
            df[c] = df[c].dt.tz_convert("UTC").dt.tz_localize(None)
        chunks.append(df)
        n_total += len(df)

    if not chunks:
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        out_csv.write_text("no_data\n", encoding="utf-8")
        return 0

    out = pd.concat(chunks, ignore_index=True)
    out = out.sort_values(["EGID", "date", "DATA_TYPE"], kind="mergesort")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")
    return n_total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=3, help="Nombre d'EGID les plus « décalés » (résumé)")
    ap.add_argument(
        "--egids",
        type=str,
        default="",
        help="Liste forcée d'EGID (ex: 1511188,190198380,235554367)",
    )
    args = ap.parse_args()

    meta_lines = []
    if args.egids.strip():
        egids = [e.strip() for e in args.egids.split(",") if e.strip()]
        meta_lines.append("Sélection : liste --egids fournie par l'utilisateur.")
    else:
        if not SUMMARY_CSV.exists():
            raise SystemExit(
                f"Résumé absent : {SUMMARY_CSV}. Lancez d'abord export_cluster3_csv_samples.py "
                "ou fournissez --egids."
            )
        s = pd.read_csv(SUMMARY_CSV)
        s["date_min"] = pd.to_datetime(s["date_min"])
        tr = s[s["DATA_TYPE"] == "TempRet"][["EGID", "date_min"]].rename(columns={"date_min": "tret_min"})
        pu = s[s["DATA_TYPE"] == "PuisCpt"][["EGID", "date_min"]].rename(columns={"date_min": "puis_min"})
        m = tr.merge(pu, on="EGID", how="inner")
        m["gap_days"] = (m["puis_min"] - m["tret_min"]).dt.days
        m = m.sort_values("gap_days", ascending=False)
        top = m.head(args.top)
        egids = top["EGID"].astype(str).tolist()
        for _, r in top.iterrows():
            meta_lines.append(
                f"EGID {r['EGID']} : TempRet dès {r['tret_min']}, PuisCpt dès {r['puis_min']}, "
                f"écart = {int(r['gap_days'])} j"
            )

    print("EGID exportés :", egids)
    meta = OUT_DIR / "selection_meta.txt"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    meta.write_text(
        "Cluster 3 — EGID retenus pour export CSV (fort décalage début TempRet vs PuisCpt si auto).\n\n"
        + "\n".join(meta_lines),
        encoding="utf-8",
    )

    for stage, path in PATHS.items():
        suffix = "_".join(egids)
        out = OUT_DIR / f"cluster3_{stage}_EGID_{suffix}.csv"
        if len(out.name) > 200:
            out = OUT_DIR / f"cluster3_{stage}_n{len(egids)}_egids.csv"
        n = export_stage(path, egids, out)
        print(f"{stage}: {n:,} lignes → {out}")


if __name__ == "__main__":
    main()
