# Projet : import_load.py — Chargement et analyse des données SST

**Date :** 28 février 2026  
**Objectif :** Script Python pour charger, structurer et analyser les CSV d'export SST dans un DataFrame pandas unifié.

---

## 1. Contexte et périmètre

### 1.1 Données sources
- **Emplacement :** `2_Program/0_Data/0_Raw/ExportSST/export_SSTCAD_20260227/`
- **Format :** Fichiers CSV avec séparateur `;`
- **Structure :** 3 colonnes par fichier
  - Colonne 1 : `date` (format `DD/MM/YYYY HH:MM`)
  - Colonne 2 : mesure (nom variable selon le fichier, ex. `1510837_TempRet`, `1511122_PuisCpt`)
  - Colonne 3 : `inv` (validité : 0 = valide, ≠ 0 = invalide)

### 1.2 Observations terrain
- **Échantillonnage variable :**
  - Certains fichiers : **15 minutes** (ex. techant21704, techant265, techant150)
  - D'autres : **1 minute** (ex. techant18524)
  - Cas possibles : **heure ou plus** (à traiter à part)
- **Volume :** Nombre de lignes variable selon les fichiers (quelques milliers à plusieurs centaines de milliers)

### 1.3 Structure du programme
- structurer le code en fonctions réutilisables (lecture, agrégation, fusion) pour faciliter un remplacement ultérieur de Pandas par PySpark.

---

## 2. Actions à réaliser

### 2.1 Configuration
- Définir en début de script une variable `PATH_RAW` pointant vers le dossier des CSV
- Définir une variable `PATH_LOGS` pour `2_Program/97_logs`

### 2.2 Lecture des CSV
- Lister tous les fichiers `.csv` du dossier `PATH_RAW`
- Lire chaque fichier avec `pd.read_csv(..., sep=';')`
- Gérer les erreurs de lecture (fichier corrompu, format inattendu) avec try/except et logs

### 2.3 Aquisition des données de température
- Lire la température au sol pour la ville de bulle (46° 37′ 03″ nord, 7° 03′ 29″ est)
- Si possible utiliser les open data de météo suisse (voir: https://www.meteosuisse.admin.ch/services-et-publications/service/open-data.html)
- Si les données de météo suisse sont trop complexes ou lourdes à manipuler : utiliser un autre API fiable

### 2.4 Détection de l'échantillonnage
Pour chaque fichier :
- Calculer l'intervalle entre les timestamps (différence entre lignes consécutives)
- Déduire le pas : **minute**, **15 min**, **heure**, ou **supérieur**
- Si pas ≥ 1 heure : enregistrer le nom du fichier dans un fichier `.txt` dans `2_Program/97_logs`

### 2.5 Agrégation temporelle
- **Cible :** index `timestamp_UTC` au quart d'heure (00, 15, 30, 45)
- **Fichiers à 15 min :** aligner directement sur les timestamps 00/15/30/45
- **Fichiers à la minute :** pour chaque quart d'heure T, calculer la **moyenne des 15 dernières minutes** (T-14min à T inclus, ou T-15min à T-1min selon la convention choisie)
- **Fichiers à l'heure ou plus :** exclus du DataFrame principal, uniquement loggés

### 2.6 Gestion de la validité
- Si `inv != 0` : remplacer la valeur de la colonne 2 par `0.0`
- Si `inv == 0` : conserver la valeur de la colonne 2

### 2.7 Construction du DataFrame final
- **Index :** `timestamp_utc` unique, couvrant la plage temporelle globale (min à max de tous les fichiers)
- **Colonnes :** `timestamp_loc` unique, couvrant la plage temporelle globale, permettant d'afficher l'heure locale dans les graphiques.
- **Colonnes :** `temp_ext_api` unique, couvrant la plage temporelle globale, contenant la température mesurée pour la ville de Bulle. (à récupérer depuis l'API de météo suisse ou autre)
- **Colonnes :** une colonne par fichier, nommée d'après la colonne 2 du CSV (ex. `1510837_TempRet`)
- **Valeurs manquantes :** remplacer par `0.0` si une timestamp n'existe pas dans un fichier donné

### 2.8 export du dataframe au format .parquet
- Chemin de sortie : `2_Program/0_Data/1_Structured`
- utiliser la fonction incluse dans pandas `df.to_parquet()`

### 2.9 Robustesse et sécurité
- Validation des entrées (chemins, types)
- Gestion des erreurs avec logs
- Pas de secrets en dur
- Libération des ressources (fichiers)
- structure facile à migrer vers pySpark

---

## 3. Structure du script attendu

```
import_load.py (2_Program/)
├── Variables de configuration (PATH_RAW, PATH_LOGS, PATH_STRUCTURED)
├── Fonctions utilitaires
│   ├── detect_sampling_interval(df) → 'minute' | '15min' | 'hour' | 'coarser'
│   ├── load_and_process_csv(filepath) → (df_processed, col_name, sampling)
│   ├── aggregate_to_quarter_hour(df, sampling) → df_15min
│   ├── fetch_temp_ext_bulle(start_utc, end_utc) → Series temp_ext_api
│   └── localize_to_utc(series) → timestamps UTC
├── Boucle principale
│   ├── Lister les CSV
│   ├── Pour chaque fichier : charger, détecter échantillonnage, traiter
│   ├── Fusionner dans un DataFrame global
│   ├── Ajouter timestamp_loc (heure locale pour graphiques)
│   ├── Récupérer temp_ext_api (API MétéoSuisse ou Open-Meteo)
│   └── Convertir index en timestamp_utc
└── Export Parquet (2_Program/0_Data/1_Structured)
```

---

## 4. Fichiers à produire

| Fichier | Emplacement | Rôle |
|---------|-------------|------|
| `import_load.py` | `2_Program/` | Script principal |
| `import_load.md` | `2_Program/` | Documentation associée |
| `run_import_load.bat` | `2_Program/` | Lanceur batch (utilise .venv) |
| `fichiers_echantillon_horaire_ou_plus.txt` | `2_Program/97_logs/` | Liste des fichiers exclus (généré à l'exécution) |
| `colonnes_dupliquees.txt` | `2_Program/97_logs/` | Liste des colonnes renommées (doublons) |
| `sst_unified.parquet` | `2_Program/0_Data/1_Structured/` | DataFrame final exporté |

---

## 5. Points d'attention

- **Convention « 15 dernières minutes » :** préciser si pour 13:15 on prend 13:01–13:15 ou 13:00–13:14
- **Doublons de noms de colonnes :** si plusieurs fichiers ont la même colonne 2, prévoir un suffixe (ex: _0, _1, etc) et afficher une erreur dans le fichier de logs.
- **Encodage :** vérifier l'encodage des CSV (UTF-8, Latin-1, etc.)
- **Fuseau horaire :** la colonne date est en CET ou CEST. l'index doit être en UTC afin d'éviter les problèmes lors du changement d'heure. une colonne timestamp_loc doit être créée pour la réalisation des graphiques.

---

## 6. Prochaines étapes

1. Valider ce document avec l'équipe
2. Implémenter `import_load.py` selon cette synthèse
3. Tester sur un sous-ensemble de fichiers
4. Exécuter sur l'ensemble des données et vérifier les logs
