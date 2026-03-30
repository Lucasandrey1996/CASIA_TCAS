# -*- coding: utf-8 -*-
"""Trace un EGID : sst_enrichi → transfo long → parquet train large (défaut cluster 3).

Usage (depuis 2_Program) :
  .venv\\Scripts\\python.exe trace_egid_sst_pipeline.py [EGID]
  .venv\\Scripts\\python.exe trace_egid_sst_pipeline.py 1511188 0_Data/3_training/cluster5.parquet
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
STRUCT = ROOT / "0_Data/1_Structured"
PATH_ENRICHED = STRUCT / "sst_enriched.parquet"
PATH_TRANSFO = STRUCT / "sst_filtered_transfo.parquet"


def fmt_pct(x: float) -> str:
    return f"{100 * x:.2f} %"


def summarize_series(dates: pd.Series, mask_ok: pd.Series, label: str) -> None:
    n = len(dates)
    nok = int(mask_ok.sum())
    if n == 0:
        print(f"  {label}: aucune ligne")
        return
    dsub = dates[mask_ok]
    print(
        f"  {label}: {nok} / {n} points ({fmt_pct(nok / n)}), "
        f"min={dsub.min()}, max={dsub.max()}, span={(dsub.max() - dsub.min()).days} j"
    )


def main(egid: str, path_wide: Path) -> None:
    egid = str(egid)

    print("=" * 72)
    print(f"EGID = {egid}  |  wide = {path_wide}")
    print("=" * 72)

    df_e = pd.read_parquet(PATH_ENRICHED, columns=["date", "EGID", "DATA_TYPE", "valeur", "inv"])
    df_e["EGID"] = df_e["EGID"].astype(str)
    sub_e = df_e[df_e["EGID"] == egid]
    del df_e

    for dt in ["TempRet", "PuisCpt"]:
        s = sub_e[sub_e["DATA_TYPE"] == dt]
        summarize_series(
            s["date"],
            s["inv"].eq(0) & s["valeur"].notna(),
            f"Enrichi — {dt} (inv=0 & valeur non NaN)",
        )
        if not s.empty:
            nz = s["date"].sort_values()
            print(f"    première date toute ligne: {nz.iloc[0]}, dernière: {nz.iloc[-1]}")

    df_t = pd.read_parquet(
        PATH_TRANSFO,
        columns=["date", "date_15min", "EGID", "DATA_TYPE", "valeur", "valeur_fc", "valeur_norm", "inv"],
    )
    df_t["EGID"] = df_t["EGID"].astype(str)
    sub_t = df_t[df_t["EGID"] == egid]
    del df_t

    for dt, col_fc in [("PuisCpt", "valeur_fc"), ("TempRet", "valeur_norm")]:
        s = sub_t[sub_t["DATA_TYPE"] == dt]
        if s.empty:
            print(f"\nTransfo — {dt}: aucune ligne")
            continue
        vc = s[col_fc]
        ok = vc.notna()
        print(f"\nTransfo — {dt} ({col_fc}): non-NaN {ok.sum()}/{len(s)} ({fmt_pct(ok.mean())})")
        if ok.any():
            d_ok = s.loc[ok, "date"]
            print(f"  1re date avec {col_fc} non NaN: {d_ok.min()}")
            print(f"  dernière: {d_ok.max()}")
        if dt == "PuisCpt":
            has_val = s["valeur"].notna()
            fc_nan = s["valeur_fc"].isna()
            bad = has_val & fc_nan
            print(f"  Lignes PuisCpt avec valeur renseignée mais valeur_fc NaN: {bad.sum()}")
            if bad.any():
                print(f"    exemple dates: {s.loc[bad, 'date'].head(3).tolist()}")

    if not path_wide.exists():
        print(f"\nFichier large absent : {path_wide}")
        return

    df_w = pd.read_parquet(path_wide)
    df_w["Dates"] = pd.to_datetime(df_w["Dates"], utc=True)
    print(
        f"\nGrille globale du parquet large: min={df_w['Dates'].min()}, max={df_w['Dates'].max()}, "
        f"n={len(df_w)} pas"
    )

    c_tr = f"{egid}.TempRet_norm"
    c_pc = f"{egid}.PuisCpt_fc"
    for name, col in [("TempRet_norm", c_tr), ("PuisCpt_fc", c_pc)]:
        if col not in df_w.columns:
            print(f"\nWide train — {name}: colonne absente ({col})")
            continue
        ser = df_w[col]
        ok = ser.notna()
        print(f"\nWide train — {col}: non-NaN {ok.sum()}/{len(ser)} ({fmt_pct(ok.mean())})")
        if ok.any():
            d_ok = df_w.loc[ok, "Dates"]
            print(f"  1re date non-NaN: {d_ok.min()}, dernière: {d_ok.max()}")
        t0 = df_w["Dates"].min()
        m3 = t0 + pd.Timedelta(days=90)
        early = df_w["Dates"] < m3
        late = ~early
        if ok.any():
            r_early = ok & early
            r_late = ok & late
            print(
                f"  fenêtre 90j depuis min(grille globale): non-NaN {r_early.sum()}/{early.sum()} "
                f"({fmt_pct(r_early.sum() / max(early.sum(), 1))})"
            )
            print(
                f"  après ces 90j: non-NaN {r_late.sum()}/{late.sum()} "
                f"({fmt_pct(r_late.sum() / max(late.sum(), 1))})"
            )


if __name__ == "__main__":
    eg = sys.argv[1] if len(sys.argv) > 1 else "1511188"
    wide = Path(sys.argv[2]) if len(sys.argv) > 2 else ROOT / "0_Data/3_training/cluster3.parquet"
    main(eg, wide)
