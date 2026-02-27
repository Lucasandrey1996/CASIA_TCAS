# Filter_Techant.py

## Objectif

Script Python qui génère des fichiers de configuration (un par type de donnée) à partir des points de transmission CAD et de la liste des techant. Il applique des filtres prédéfinis pour associer chaque point SST (sous-station de chaleur) aux références techant correspondantes.

## Fichiers

| Rôle | Chemin | Description |
|------|--------|-------------|
| **Entrée** | `0_Raw/RefFiles/Point_transmission_CAD_20260202.xlsx` | Fichier Excel contenant les points de transmission (colonne `U_NO_EGID`) |
| **Entrée** | `0_Raw/liste_techant/20260219_techant.csv` | Liste des techant avec métadonnées |
| **Sortie** | `0_Raw/liste_techant/config_export_{suffix}_controlCAD.csv` | Un fichier par type : `config_export_TempRet_controlCAD.csv`, `config_export_PuisCpt_controlCAD.csv`, `config_export_PosVan_controlCAD.csv`, etc. |

## Filtres appliqués

Pour chaque `U_NO_EGID` du fichier Excel, le script recherche dans la liste techant les lignes où `ouvrage = "[U_NO_EGID] SST"` avec les critères suivants :

| Type | Champ | libelle_2 | Fréquence | Type | Suffix | Fichier généré |
|------|-------|-----------|-----------|------|--------|----------------|
| Température | CH1_2B4 SONDE DE TEMPERATURE | — | 15 MINUTES | Valeur moyenne | TempRet | config_export_TempRet_controlCAD.csv |
| Température | CH1_2B4 SONDE DE TEMPERATURE COMMUN | — | 15 MINUTES | Valeur moyenne | TempRet | config_export_TempRet_controlCAD.csv |
| Température | CH1_1C2 COMPTEUR | TEMPERATURE RETOUR | 15 MINUTES | Valeur moyenne | TempRet | config_export_TempRet_controlCAD.csv |
| Puissance | CH1_1C2 COMPTEUR | PUISSANCE | 1 MINUTE | Valeur moyenne | PuisCpt | config_export_PuisCpt_controlCAD.csv |
| Vanne | CH1_2Y7 VANNE | — | 15 MINUTES | Valeur maximum | PosVan | config_export_PosVan_controlCAD.csv |
| Vanne | CH2_4Y4 VANNE | — | 15 MINUTES | Valeur maximum | PosVan | config_export_PosVan_controlCAD.csv |
| Vanne | CH3_5Y4 VANNE | — | 15 MINUTES | Valeur maximum | PosVan | config_export_PosVan_controlCAD.csv |
| Vanne | CH4_6Y4 VANNE | — | 15 MINUTES | Valeur maximum | PosVan | config_export_PosVan_controlCAD.csv |

> Les champs VANNE acceptent les variantes (ex. `CH2_4Y4 VANNE 2 VOIES`).

## Format des fichiers de sortie

Chaque CSV utilise le séparateur `;` et contient 3 colonnes (Nom, Table, nbr). Les lignes sont regroupées par suffix (TempRet, PuisCpt, PosVan), ce qui produit un fichier par type de donnée.

| Colonne | Description | Exemple |
|---------|-------------|---------|
| Nom | `[U_NO_EGID]_[suffix]` | `1511120_TempRet` |
| Table | `techant[ref_techant]` | `techant5215` |
| nbr | Nombre fixe | `120` |

## Dépendances

- **pandas** : lecture Excel et CSV
- **openpyxl** : support des fichiers `.xlsx`
- **tqdm** : barre de progression (optionnel)

## Exécution

```bash
# Via Python (depuis la racine du projet)
.venv\Scripts\python.exe "2_Program\0_Data\Filter_Techant.py"

# Ou via le fichier batch
run_Filter_Techant.bat
```

## Logique

1. **Chargement** : lecture du fichier Excel et du CSV techant
2. **Index** : construction d’un index des correspondances techant en une seule passe
3. **Croisement** : pour chaque ligne Excel (U_NO_EGID), recherche des techant correspondants
4. **Dédoublonnage** : une seule ligne générée par couple (U_NO_EGID, filtre)
5. **Export** : regroupement par suffix et écriture d'un fichier CSV par type
