


def cluster_deep_analysis(cluster_id: int, path_training=None):
    """Analyse section 9 : corrélations features × cibles normalisées, hétérogénéité sur MIN_YEARS_DATA (jeu train)."""
    if path_training is None:
        path_training = PATH_TRAINING
    path_cl = path_training / f"cluster{cluster_id}.parquet"
    if not path_cl.exists():
        raise FileNotFoundError(
            f"{path_cl} introuvable — exécuter la section 7 (Split) depuis le répertoire 2_Program."
        )

    df_cl = pd.read_parquet(path_cl)
    df_cl["Dates"] = pd.to_datetime(df_cl["Dates"], utc=True)

    FEAT_BASE = [
        "dayofyear_cos", "dayofyear_sin", "dayofweek_cos", "dayofweek_sin",
        "hour_cos", "hour_sin", "TempExt_norm",
    ]
    feat_cols = [c for c in FEAT_BASE if c in df_cl.columns]
    tret_cols = [c for c in df_cl.columns if c.endswith(".TempRet_norm")]
    puis_cols = [c for c in df_cl.columns if c.endswith(".PuisCpt_fc")]

    MAX_TARGETS_HEAT = 45

    def pick_targets(cols, d, k=MAX_TARGETS_HEAT):
        if len(cols) <= k:
            return cols
        v = d[cols].var().replace(0, np.nan).dropna().sort_values(ascending=False)
        return v.head(k).index.tolist()

    def short_label(c: str) -> str:
        if ".TempRet_norm" in c:
            return c.replace(".TempRet_norm", "")
        if ".PuisCpt_fc" in c:
            return c.replace(".PuisCpt_fc", "")
        return c[:18]

    def corr_features_targets(d, features, targets):
        mat = pd.DataFrame(np.nan, index=features, columns=targets, dtype=float)
        for t in targets:
            use = [c for c in features if c in d.columns] + [t]
            sub = d[use].dropna()
            if len(sub) < 80 or float(sub[t].std(skipna=True) or 0) < 1e-12:
                continue
            for f in features:
                if f not in sub.columns:
                    continue
                sf = float(sub[f].std(skipna=True) or 0)
                if sf < 1e-12:
                    continue
                mat.loc[f, t] = sub[f].corr(sub[t])
        return mat

    tret_sel = pick_targets(tret_cols, df_cl)
    puis_sel = pick_targets(puis_cols, df_cl)
    corr_tret = corr_features_targets(df_cl, feat_cols, tret_sel)
    corr_puis = corr_features_targets(df_cl, feat_cols, puis_sel)

    if len(tret_sel):
        corr_tret.columns = [short_label(c) for c in corr_tret.columns]
    if len(puis_sel):
        corr_puis.columns = [short_label(c) for c in corr_puis.columns]

    h0 = max(4.0, len(feat_cols) * 0.5)
    w_heat = max(10.0, min(32.0, 0.2 * max(len(tret_sel), len(puis_sel), 5)))

    fig_c, ax_c = plt.subplots(2, 1, figsize=(w_heat, h0 * 2 + 1.2))
    if tret_sel:
        sns.heatmap(
            corr_tret,
            ax=ax_c[0],
            cmap="RdBu_r",
            center=0,
            vmin=-1,
            vmax=1,
            cbar_kws={"label": "Pearson r"},
        )
        ax_c[0].set_title(
            f"Cluster {cluster_id} — features × TempRet_norm ({len(tret_sel)} cibles affichées)"
        )
    else:
        ax_c[0].text(0.5, 0.5, "Aucune colonne *.TempRet_norm", ha="center", va="center")
        ax_c[0].set_axis_off()

    if puis_sel:
        sns.heatmap(
            corr_puis,
            ax=ax_c[1],
            cmap="RdBu_r",
            center=0,
            vmin=-1,
            vmax=1,
            cbar_kws={"label": "Pearson r"},
        )
        ax_c[1].set_title(
            f"Cluster {cluster_id} — features × PuisCpt_fc ({len(puis_sel)} cibles affichées)"
        )
    else:
        ax_c[1].text(0.5, 0.5, "Aucune colonne *.PuisCpt_fc", ha="center", va="center")
        ax_c[1].set_axis_off()

    for ax in ax_c:
        if ax.get_visible():
            ax.tick_params(axis="x", rotation=90, labelsize=7)
    plt.tight_layout()
    plt.show()

    def span_train_window(t_start):
        return t_start + pd.Timedelta(days=int(round(365.25 * MIN_YEARS_DATA)))

    # TempRet : fenêtre MIN_YEARS_DATA depuis le début du fichier train
    t0 = df_cl["Dates"].min()
    df15_tret = df_cl[(df_cl["Dates"] >= t0) & (df_cl["Dates"] < span_train_window(t0))].copy()

    # PuisCpt : les séries commencent souvent après TempRet — fenêtre depuis la 1re mesure non nulle
    df15_puis = None
    if puis_cols:
        m_first = df_cl[puis_cols].notna().any(axis=1)
        if m_first.any():
            t0p = df_cl.loc[m_first, "Dates"].min()
            df15_puis = df_cl[(df_cl["Dates"] >= t0p) & (df_cl["Dates"] < span_train_window(t0p))].copy()

    if df15_tret.empty and (df15_puis is None or df15_puis.empty):
        print(f"Pas de lignes dans les fenêtres {MIN_YEARS_DATA} an.")
    else:

        def plot_heterogeneity(d, columns, title_short, ax_bar, ax_line):
            if not columns:
                ax_bar.text(0.5, 0.5, f"Pas de colonnes {title_short}", ha="center", va="center")
                ax_bar.set_axis_off()
                ax_line.set_axis_off()
                return
            X = d[["Dates"] + columns].copy()
            X = X.dropna(subset=["Dates"])
            X = X.drop_duplicates(subset=["Dates"], keep="last").set_index("Dates").sort_index()[columns]
            if X.notna().sum().sum() == 0:
                msg = f"Aucune donnée pour {title_short} sur cette fenêtre"
                ax_bar.text(0.5, 0.5, msg, ha="center", va="center", fontsize=9)
                ax_bar.set_axis_off()
                ax_line.text(0.5, 0.5, msg, ha="center", va="center", fontsize=9)
                ax_line.set_axis_off()
                return
            mu = X.mean(axis=1, skipna=True)
            dev = X.sub(mu, axis=0)
            spread = dev.std(axis=0, skipna=True).sort_values(ascending=False).dropna()
            if spread.empty:
                ax_bar.text(0.5, 0.5, "Dispersion non calculable (séries vides)", ha="center", va="center")
                ax_bar.set_axis_off()
                ax_line.set_axis_off()
                return
            top_bar = spread.head(28)
            ax_bar.barh(
                [short_label(str(i)) for i in top_bar.index],
                top_bar.values.astype(float),
                color="steelblue",
                alpha=0.88,
            )
            ax_bar.set_xlabel("Écart-type (écarts au profil moyen instantané du cluster)")
            ax_bar.set_title(f"Cluster {cluster_id} — dispersion relative — {title_short}")
            ax_bar.tick_params(axis="y", labelsize=6)

            w = X.resample("W").mean()
            if w.empty:
                ax_line.text(0.5, 0.5, "Agrégation hebdo vide", ha="center", va="center")
                ax_line.set_axis_off()
                return
            w_mu = w.mean(axis=1, skipna=True)
            if not np.isfinite(w_mu.to_numpy(dtype=float, copy=False)).any():
                ax_line.text(0.5, 0.5, "Moyenne cluster hebdo non disponible (NaN)", ha="center", va="center")
                ax_line.set_axis_off()
                return
            ax_line.plot(w_mu.index, w_mu.values, color="black", linewidth=2.2, label="Moyenne cluster")
            top4 = spread.head(4).index.tolist()
            colors = plt.cm.tab10(np.linspace(0, 0.9, len(top4)))
            for j, col in enumerate(top4):
                lbl = short_label(str(col))
                ax_line.plot(w.index, w[col], alpha=0.85, color=colors[j], label=lbl)
            ax_line.set_ylabel("Valeur (agrégat hebdo)")
            ax_line.set_title(f"Hebdomadaire — {title_short} vs moyenne")
            ax_line.legend(loc="upper left", fontsize=7)
            ax_line.grid(alpha=0.3)

        fig_h, axes_h = plt.subplots(2, 2, figsize=(14, 10))
        plot_heterogeneity(df15_tret, tret_cols, "TempRet_norm", axes_h[0, 0], axes_h[0, 1])
        if df15_puis is not None and not df15_puis.empty:
            plot_heterogeneity(df15_puis, puis_cols, "PuisCpt_fc", axes_h[1, 0], axes_h[1, 1])
        else:
            msg = "PuisCpt_fc : pas de données dans le train, ou fenêtre vide"
            axes_h[1, 0].text(0.5, 0.5, msg, ha="center", va="center", fontsize=9)
            axes_h[1, 0].set_axis_off()
            axes_h[1, 1].text(0.5, 0.5, msg, ha="center", va="center", fontsize=9)
            axes_h[1, 1].set_axis_off()
        plt.tight_layout()
        plt.show()

    del df_cl
    gc.collect()
