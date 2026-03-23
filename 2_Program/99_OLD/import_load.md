# import_load.py — Documentation

## Objectif

Script Python pour charger, structurer et analyser les fichiers CSV d'export SST dans un DataFrame pandas unifié, avec index temporel au quart d'heure en UTC.

## Configuration

| Variable | Description |
|----------|-------------|
| `PATH_RAW` | Dossier contenant les CSV (`0_Data/0_Raw/ExportSST/export_SSTCAD_20260227`) |
| `PATH_LOGS` | Dossier des logs (`97_logs`) |
| `PATH_STRUCTURED` | Dossier de sortie Parquet (`0_Data/1_Structured`) |

## Format des données sources

- **Structure :** 3 colonnes par fichier
  - Colonne 1 : `date` (format `DD/MM/YYYY HH:MM`, CET/CEST)
  - Colonne 2 : mesure (nom variable, ex. `1510837_TempRet`)
  - Colonne 3 : `inv` (0 = valide, ≠ 0 = invalide)
- **Séparateur :** `;`
- **Encodage :** UTF-8 (fallback cp1252)

## Traitement

1. **Lecture** : tous les `.csv` du dossier `PATH_RAW`
2. **Validité** : si `inv != 0`, la valeur est remplacée par `0.0`
3. **Échantillonnage** :
   - **15 min** : alignement direct sur 00/15/30/45
   - **minute** : moyenne des 15 dernières minutes (ex. 13:15 = moyenne 13:01–13:15)
   - **heure ou plus** : fichier exclu, nom enregistré dans `97_logs/fichiers_echantillon_horaire_ou_plus.txt`
4. **Fuseau horaire** : index `timestamp_utc` en UTC, colonne `timestamp_loc` pour les graphiques
5. **Température extérieure** : colonne `temp_ext_api` (Bulle, via Open-Meteo)
6. **Fusion** : une colonne par fichier, valeurs manquantes = `0.0`
7. **Export** : `sst_unified.parquet` dans `0_Data/1_Structured/`

## Utilisation

Exécuter le script (voir ci-dessous) ou appeler `build_unified_dataframe()` et `export_to_parquet()` depuis un autre module.

Ou via le lanceur batch :

```batch
run_import_load.bat
```

Ou en ligne de commande :

```bash
python import_load.py
```

## Dépendances

- `pandas`
- `pyarrow` (pour l'export Parquet)

## Voir aussi

- [Synthèse du projet](../../1_Documentation/3_devellopement/import_load_projet.md)
