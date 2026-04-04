"""Optimisation des coupures chronologiques train / val / test sur TempExt (grille globale)."""
from __future__ import annotations

import gc
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import wasserstein_distance


@dataclass(frozen=True)
class ChronoSplitResult:
    val_start_utc: pd.Timestamp
    test_start_utc: pd.Timestamp
    dmin_g_utc: pd.Timestamp
    dmax_g_utc: pd.Timestamp
    best_score: float
    best_i: int
    best_j: int
    frac_train: float
    frac_val: float
    frac_test: float
    cold_threshold_c: float
    train_cold_pct: float
    val_cold_pct: float
    test_cold_pct: float
    n_train: int
    n_val: int
    n_test: int


def _score_split_triplet(
    t_full: tuple[np.ndarray, np.ndarray, np.ndarray],
    t_cold: tuple[np.ndarray, np.ndarray, np.ndarray],
    quant_levels: np.ndarray,
    cold_w1_weight: float,
    quantile_weight: float,
    min_cold_each: int,
) -> float:
    a, b, c = t_full
    w1_triplet = (
        wasserstein_distance(a, b)
        + wasserstein_distance(a, c)
        + wasserstein_distance(b, c)
    ) / 3.0
    qa = np.quantile(a, quant_levels)
    qb = np.quantile(b, quant_levels)
    qc = np.quantile(c, quant_levels)
    q_loss = (np.mean(np.abs(qa - qb)) + np.mean(np.abs(qa - qc)) + np.mean(np.abs(qb - qc))) / 3.0
    ca, cb, cc = t_cold
    if len(ca) >= min_cold_each and len(cb) >= min_cold_each and len(cc) >= min_cold_each:
        w1_cold = (
            wasserstein_distance(ca, cb)
            + wasserstein_distance(ca, cc)
            + wasserstein_distance(cb, cc)
        ) / 3.0
    else:
        w1_cold = w1_triplet
    return w1_triplet + cold_w1_weight * w1_cold + quantile_weight * q_loss


def _as_utc(ts: pd.Timestamp | str) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")


def compute_chrono_split_bounds(
    path_sst_enriched: Path | str,
    *,
    frac_train_min: float = 0.5,
    frac_val_min: float = 0.2,
    frac_test_min: float = 0.1,
    tempext_cold_threshold_c: float = 15.0,
    cold_w1_weight: float = 2.0,
    quantile_weight: float = 0.5,
    grid_stride: int | None = None,
    min_cold_each: int = 20,
    clip_timeline_start_utc: pd.Timestamp | str | None = None,
    clip_timeline_end_utc: pd.Timestamp | str | None = None,
) -> ChronoSplitResult:
    path = Path(path_sst_enriched)
    if not path.is_file():
        raise FileNotFoundError(f"Parquet enrichi introuvable : {path}")

    quant_levels = np.linspace(0.05, 0.95, 19)
    cold_thr = float(tempext_cold_threshold_c)

    dfm = pd.read_parquet(path, columns=["date_15min", "TempExt"])
    dfm = dfm.dropna(subset=["date_15min", "TempExt"])
    dfm["date_15min"] = pd.to_datetime(dfm["date_15min"], utc=True)
    dfm = dfm.sort_values("date_15min").drop_duplicates(subset=["date_15min"], keep="last")
    if clip_timeline_start_utc is not None:
        dfm = dfm[dfm["date_15min"] >= _as_utc(clip_timeline_start_utc)]
    if clip_timeline_end_utc is not None:
        dfm = dfm[dfm["date_15min"] <= _as_utc(clip_timeline_end_utc)]
    if len(dfm) < 3:
        raise ValueError(
            "Pas assez de pas TempExt après clip [clip_timeline_start_utc, clip_timeline_end_utc] "
            "(aligner sur la plage réelle des données ML, ex. sst_filtered_transfo)."
        )
    ts = dfm["date_15min"].to_numpy(dtype="datetime64[ns]")
    temps = dfm["TempExt"].to_numpy(dtype=np.float64)
    del dfm
    gc.collect()

    if ts.size < 3:
        raise ValueError("Pas assez de pas de temps TempExt pour un split en trois blocs.")

    dmin_g_utc = pd.to_datetime(ts[0], utc=True)
    dmax_g_utc = pd.to_datetime(ts[-1], utc=True)
    tdelta = dmax_g_utc - dmin_g_utc
    t_sec = max(float(tdelta.total_seconds()), 1.0)
    t_train_min = dmin_g_utc + pd.Timedelta(seconds=frac_train_min * t_sec)
    t_val_span_min = pd.Timedelta(seconds=frac_val_min * t_sec)
    t_test_span_min = pd.Timedelta(seconds=frac_test_min * t_sec)

    n = len(ts)
    stride = grid_stride or max(1, n // 200)
    ts_utc_idx = pd.DatetimeIndex(pd.to_datetime(ts, utc=True))
    mask_trmin = ts_utc_idx >= t_train_min
    i_low = int(np.argmax(mask_trmin)) if bool(mask_trmin.any()) else n

    best: float | None = None
    best_pair: tuple[int, int] | None = None

    for i in range(i_low, n - 1, stride):
        t_val_start = ts_utc_idx[i]
        if (t_val_start - dmin_g_utc) < pd.Timedelta(seconds=frac_train_min * t_sec):
            continue
        j_min_time = t_val_start + t_val_span_min
        mask_j = ts_utc_idx >= j_min_time
        j0 = int(np.argmax(mask_j)) if bool(mask_j.any()) else n
        for j in range(max(i + 1, j0), n, stride):
            t_test_start = ts_utc_idx[j]
            if (t_test_start - t_val_start) < t_val_span_min:
                continue
            if (dmax_g_utc - t_test_start) < t_test_span_min:
                break
            sl_tr = temps[:i]
            sl_va = temps[i:j]
            sl_te = temps[j:]
            if sl_tr.size == 0 or sl_va.size == 0 or sl_te.size == 0:
                continue
            c_tr = sl_tr[sl_tr < cold_thr]
            c_va = sl_va[sl_va < cold_thr]
            c_te = sl_te[sl_te < cold_thr]
            sc = _score_split_triplet(
                (sl_tr, sl_va, sl_te),
                (c_tr, c_va, c_te),
                quant_levels,
                cold_w1_weight,
                quantile_weight,
                min_cold_each,
            )
            if best is None or sc < best:
                best = sc
                best_pair = (i, j)

    if best_pair is None or best is None:
        raise RuntimeError(
            "Aucune paire (val, test) ne satisfait les contraintes de fractions sur cette grille. "
            "Réduire le pas (grid_stride, ex. 1) ou assouplir frac_train_min / frac_val_min / frac_test_min."
        )

    bi, bj = best_pair
    val_start = pd.to_datetime(ts[bi], utc=True)
    test_start = pd.to_datetime(ts[bj], utc=True)
    sl_tr = temps[:bi]
    sl_va = temps[bi:bj]
    sl_te = temps[bj:]

    def _pct(x: np.ndarray) -> float:
        if x.size == 0:
            return 0.0
        return 100.0 * float(np.sum(x < cold_thr)) / float(x.size)

    fr_tr = (val_start - dmin_g_utc).total_seconds() / t_sec
    fr_va = (test_start - val_start).total_seconds() / t_sec
    fr_te = (dmax_g_utc - test_start).total_seconds() / t_sec

    return ChronoSplitResult(
        val_start_utc=val_start,
        test_start_utc=test_start,
        dmin_g_utc=dmin_g_utc,
        dmax_g_utc=dmax_g_utc,
        best_score=best,
        best_i=bi,
        best_j=bj,
        frac_train=fr_tr,
        frac_val=fr_va,
        frac_test=fr_te,
        cold_threshold_c=cold_thr,
        train_cold_pct=_pct(sl_tr),
        val_cold_pct=_pct(sl_va),
        test_cold_pct=_pct(sl_te),
        n_train=int(sl_tr.size),
        n_val=int(sl_va.size),
        n_test=int(sl_te.size),
    )


def print_chrono_split_report(res: ChronoSplitResult) -> None:
    tdelta = res.dmax_g_utc - res.dmin_g_utc
    thr = res.cold_threshold_c
    print("Optimisation split chronologique (score W1 + froid + quantiles) :")
    print(f"  dmin_g_utc={res.dmin_g_utc}, dmax_g_utc={res.dmax_g_utc}, T={tdelta}")
    print(f"  score={res.best_score:.5f}, indices grille (_val, _test)=({res.best_i}, {res.best_j})")
    print(f"  SPLIT_CHRONO_VAL_START_UTC = {res.val_start_utc}")
    print(f"  SPLIT_CHRONO_TEST_START_UTC = {res.test_start_utc}")
    print(
        f"  Parts calendaires : train={res.frac_train:.3f}, val={res.frac_val:.3f}, test={res.frac_test:.3f}"
    )
    print(
        f"  Train TempExt : n={res.n_train}, froid<{thr:g}°C: {res.train_cold_pct:.1f}%"
    )
    print(f"  Val   TempExt : n={res.n_val}, froid<{thr:g}°C: {res.val_cold_pct:.1f}%")
    print(f"  Test  TempExt : n={res.n_test}, froid<{thr:g}°C: {res.test_cold_pct:.1f}%")
