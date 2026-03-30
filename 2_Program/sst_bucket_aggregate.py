# -*- coding: utf-8 -*-
"""
Agrégation quart d’heure (cadres Europe/Zurich, fin de cadre en UTC) et règle 10/15 sur inv/valeur.

Utilisé par dataset_preparation_V2 : section 2 (pré-agrégation PuisCpt) et section 4 (TempRet + voie rapide PuisCpt).
"""
from __future__ import annotations

import gc

import numpy as np
import pandas as pd

_Q15_VALID_MIN = 10
_Q15_REF = 15


def localize_zurich_infer_order(dates_naive: pd.Series) -> pd.Series:
    """Interprète `date` comme heure murale Zurich (naïf). DST : infer / boucle."""

    def localize_one_day(ts: np.ndarray, row_idx: np.ndarray) -> pd.Series:
        order = np.argsort(ts, kind="mergesort")
        row_idx = row_idx[order]
        ts = ts[order]
        idx = pd.DatetimeIndex(ts)
        try:
            loc = idx.tz_localize(
                "Europe/Zurich", ambiguous="infer", nonexistent="shift_forward"
            )
        except ValueError:
            seen: dict[int, int] = {}
            built: list = []
            for t in ts:
                ti = pd.Timestamp(t)
                try:
                    built.append(
                        ti.tz_localize(
                            "Europe/Zurich", nonexistent="shift_forward"
                        )
                    )
                except ValueError:
                    k = int(ti.value)
                    n = seen.get(k, 0)
                    seen[k] = n + 1
                    built.append(
                        ti.tz_localize(
                            "Europe/Zurich",
                            ambiguous=(n % 2 == 0),
                            nonexistent="shift_forward",
                        )
                    )
            loc = pd.DatetimeIndex(built)
        return pd.Series(loc.to_numpy(), index=row_idx)

    s = pd.to_datetime(dates_naive, utc=False)
    if s.empty:
        return pd.Series(dtype="datetime64[ns, Europe/Zurich]")
    row = np.arange(len(s), dtype=np.int64)
    tmp = pd.DataFrame({"row": row, "t": s.to_numpy()})
    tmp["day"] = pd.DatetimeIndex(tmp["t"]).normalize()
    pieces = []
    for _, grp in tmp.groupby("day", sort=False):
        sub = localize_one_day(grp["t"].to_numpy(), grp["row"].to_numpy())
        pieces.append(sub)
    out = pd.concat(pieces, copy=False).sort_index()
    return pd.Series(out.to_numpy(), index=s.index)


def local_bucket_end_utc(dates_naive: pd.Series) -> pd.Series:
    """Fin de cadre 15 min en UTC (Zurich) : :15,:30,:45,:00 suivant."""
    loc = localize_zurich_infer_order(dates_naive)
    m = loc.dt.minute.to_numpy(dtype=np.int16)
    h_start = loc - pd.to_timedelta(
        loc.dt.minute.astype(np.float64) * 60.0
        + loc.dt.second.astype(np.float64)
        + loc.dt.microsecond.astype(np.float64) / 1.0e6,
        unit="s",
    )
    add_min = np.where(m <= 14, 15, np.where(m <= 29, 30, np.where(m <= 44, 45, 60)))
    bucket_local = h_start + pd.to_timedelta(add_min, unit="m")
    return bucket_local.dt.tz_convert("UTC")


def _aggregate_bucket_group(g: pd.DataFrame) -> pd.Series:
    """Une fenêtre (EGID, DATA_TYPE, bucket) : règle 10/15 proportionnelle à n."""
    val = g["valeur"].to_numpy(dtype=np.float64)
    inv = g["inv"].to_numpy()
    n = len(val)
    if n == 0:
        return pd.Series({"valeur": np.nan, "inv": np.int8(1)})
    ok = (inv == 0) & np.isfinite(val)
    v = int(ok.sum())
    if v * _Q15_REF >= _Q15_VALID_MIN * n:
        mean_v = float(val[ok].mean()) if v > 0 else np.nan
        return pd.Series({"valeur": mean_v, "inv": np.int8(0)})
    return pd.Series(
        {"valeur": float(np.nanmean(val)), "inv": np.int8(1)}
    )


def norm_utc_naive_series(s: pd.Series) -> pd.Series:
    """datetime64[ns] sans fuseau (= instant UTC)."""
    t = pd.to_datetime(s, utc=True)
    if getattr(t.dt, "tz", None) is not None:
        t = t.dt.tz_convert("UTC")
    return t.dt.tz_localize(None)


def aggregate_puiscpt_to_15min_raw(df_pc: pd.DataFrame) -> pd.DataFrame:
    """
    Réduit les lignes PuisCpt au pas 15 min (même logique que l’agrégation section 4).

    Entrée / sortie : colonnes ``date`` (Zurich naïf), ``EGID``, ``DATA_TYPE``, ``valeur``, ``inv``.
    ``date`` en sortie = fin de cadre en heure locale Zurich (naïf), cohérent avec le pipeline enrichi.
    """
    if df_pc.empty:
        return df_pc
    df = df_pc.copy()
    df["_egid"] = df["EGID"].astype(str)
    df["_bucket"] = local_bucket_end_utc(df["date"])
    gcols = ["_egid", "EGID", "DATA_TYPE", "_bucket"]
    grouped = df.groupby(gcols, sort=False, observed=True)
    try:
        df_out = grouped.apply(_aggregate_bucket_group, include_groups=False)
    except TypeError:
        df_out = grouped.apply(_aggregate_bucket_group)
    df_out = df_out.reset_index()
    df_out = df_out.drop(columns=["_egid"], errors="ignore")
    df_out["date"] = (
        pd.to_datetime(df_out["_bucket"], utc=True)
        .dt.tz_convert("Europe/Zurich")
        .dt.tz_localize(None)
    )
    df_out = df_out.drop(columns=["_bucket"])
    return df_out[["date", "EGID", "DATA_TYPE", "valeur", "inv"]]


def _aggregate_long_all_types(df: pd.DataFrame, *, aggregation_15min: bool, freq: str) -> pd.DataFrame:
    """Groupby complet (comportement historique section 4)."""
    df = df.copy()
    df["_egid"] = df["EGID"].astype(str)
    if aggregation_15min:
        df["_bucket"] = local_bucket_end_utc(df["date"])
        del df["date"]
        gc.collect()
        gcols = ["_egid", "EGID", "DATA_TYPE", "_bucket"]
    else:
        loc_ts = localize_zurich_infer_order(df["date"])
        df["_utc_floor"] = loc_ts.dt.tz_convert("UTC").dt.floor(freq)
        del df["date"]
        gc.collect()
        gcols = ["_egid", "EGID", "DATA_TYPE", "_utc_floor"]

    grouped = df.groupby(gcols, sort=False, observed=True)
    try:
        df_out = grouped.apply(_aggregate_bucket_group, include_groups=False)
    except TypeError:
        df_out = grouped.apply(_aggregate_bucket_group)
    df_out = df_out.reset_index()
    rename_bucket = "_bucket" if aggregation_15min else "_utc_floor"
    df_out = df_out.rename(columns={rename_bucket: "date_15min"}).drop(
        columns=["_egid"], errors="ignore"
    )
    return df_out


def aggregate_long(
    df: pd.DataFrame,
    *,
    aggregation_15min: bool,
    freq: str,
    puiscpt_preaggregated: bool,
) -> pd.DataFrame:
    """
    Agrège vers ``date_15min``.

    Si ``aggregation_15min`` et ``puiscpt_preaggregated`` : PuisCpt est supposé déjà au pas 15 min
    (section 2) — recalcul vectoriel de ``date_15min`` sans groupby.apply sur la puissance.
    """
    if not aggregation_15min or not puiscpt_preaggregated:
        return _aggregate_long_all_types(
            df, aggregation_15min=aggregation_15min, freq=freq
        )

    m_pc = df["DATA_TYPE"].eq("PuisCpt")
    df_pc = df.loc[m_pc]
    df_tr = df.loc[~m_pc]
    parts: list[pd.DataFrame] = []
    if not df_tr.empty:
        parts.append(_aggregate_long_all_types(df_tr, aggregation_15min=True, freq=freq))
    if not df_pc.empty:
        buck = local_bucket_end_utc(df_pc["date"])
        out_pc = pd.DataFrame(
            {
                "EGID": df_pc["EGID"].values,
                "DATA_TYPE": df_pc["DATA_TYPE"].values,
                "date_15min": norm_utc_naive_series(buck),
                "valeur": df_pc["valeur"].values,
                "inv": df_pc["inv"].values,
            }
        )
        parts.append(out_pc)
    if not parts:
        return pd.DataFrame(
            columns=["EGID", "DATA_TYPE", "date_15min", "valeur", "inv"]
        )
    return pd.concat(parts, ignore_index=True, copy=False)
