# ExpArchiV7.sh — Documentation

Script d'export et d'archivage de données depuis PostgreSQL. Fusion de **ExpArchiV5.1** (limite de lignes, temporisation) et des **modes 0 et 1** de **ExpArchiV6**.

> **Note** : Pour les identifiants SQL commençant par un chiffre (ex. `1511181_TempRet`), utiliser **ExpArchiV7.1** qui corrige l'erreur de syntaxe PostgreSQL.

---

## Synopsis

```bash
./ExpArchiV7.sh <config_file_path> <output_dir_path> [mode]
```

## Prérequis

- **Dépendances** : `psql` (client PostgreSQL) et `zip` doivent être installés.
- **PostgreSQL** : Connexion à la base `archivage` via les variables d'environnement (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` ou `~/.pgpass`).

## Paramètres

| Paramètre | Obligatoire | Description |
|-----------|-------------|--------------|
| `config_file_path` | Oui | Chemin vers le fichier CSV de configuration |
| `output_dir_path` | Oui | Répertoire de destination des exports |
| `mode` | Non | 0 ou 1 (valeur par défaut : 0). Voir [Modes](#modes). |

## Modes

| Mode | Description |
|------|-------------|
| **0** | Export limité à `nbr` lignes par table (comportement par défaut). |
| **1** | Export des tables **complètes** (ignore `nbr`). ⚠️ Temps d'exécution élevé. |

## Format du fichier de configuration

Fichier CSV avec séparateur `;`, première ligne = en-têtes :

```
Nom;Table;nbr
```

| Colonne | Description | Contrainte |
|---------|-------------|------------|
| `Nom` | Nom de la colonne de valeur dans l'export | Alphanumérique et underscore uniquement |
| `Table` | Nom de la table source dans la base `archivage` | Format obligatoire : `techantXXXXX` (XXXXX = entier strictement positif, ex. techant5278) |
| `nbr` | Nombre de lignes à exporter (entier > 0). Ignoré en mode 1. | Entier strictement positif |

## Comportement

1. **Vérifications initiales** :
   - Présence des commandes `psql` et `zip`
   - Arguments valides, existence du fichier de config et du répertoire de sortie
   - Permissions d'écriture sur le répertoire de sortie
   - Connexion à la base PostgreSQL `archivage`
2. **Validation des lignes de config** : Pour chaque ligne, contrôle de `nom` (regex alphanumérique/underscore), `table` (format `techantXXXXX`), `nbr` (entier > 0). Les lignes invalides sont ignorées avec un avertissement.
3. **Limite de lignes** : Maximum 5000 lignes de données traitées (au-delà : avertissement).
4. **Export** : Pour chaque ligne du fichier de configuration :
   - Mode 0 : `FETCH FIRST nbr ROWS ONLY`
   - Mode 1 : pas de limite (export complet)
   - Temporisation de 50 ms entre chaque requête PSQL pour limiter la charge sur la base.
5. **Compression** : Création de `export_prod_<YYYYMMDD>.zip`. Si aucun CSV n'a été produit, la compression est ignorée (avertissement).
6. **Nettoyage** : Suppression des CSV individuels après création du ZIP.

## Paramètres configurables (en tête de script)

| Variable | Valeur par défaut | Description |
|----------|-------------------|--------------|
| `CONFIG_MAX_LINES` | 5000 | Nombre maximum de lignes de config à traiter |
| `PSQL_DELAY_SEC` | 0.05 | Délai en secondes (50 ms) entre chaque requête PSQL |
| `PSQL_DB` | archivage | Nom de la base PostgreSQL cible |
| `VALID_ID_REGEX` | `^[a-zA-Z0-9_]+$` | Regex de validation pour la colonne `Nom` |
| `VALID_TABLE_REGEX` | `^techant[1-9][0-9]*$` | Regex de validation pour la colonne `Table` |

## Format des CSV exportés

- Séparateur : `;`
- En-têtes : oui
- Colonnes : `date` (DD/MM/YYYY HH24:MI), `<Nom>`, `inv`

## Origines des fonctionnalités

| Fonctionnalité | Source |
|----------------|--------|
| Limite 5000 lignes | ExpArchiV5.1 |
| Temporisation entre requêtes | ExpArchiV5.1 |
| Mode 0 (export limité) | ExpArchiV6 |
| Mode 1 (export complet) | ExpArchiV6 |
| Colonne valeur dynamique (`value AS ${nom}`) | ExpArchiV5 |
| Gestion des erreurs (`continue` sur ligne invalide) | ExpArchiV5.1 |
| Vérification dépendances (psql, zip) | V7 |
| Test connexion PostgreSQL | V7 |
| Validation permissions écriture | V7 |
| Validation regex (nom, table techantXXXXX) | V7 |
| Gestion ZIP sans CSV produits | V7 |

## Exemples

```bash
# Export limité (mode 0 par défaut)
./ExpArchiV7.sh config_export_controlCAD.csv ./exports

# Export complet de toutes les tables
./ExpArchiV7.sh config_export_controlCAD.csv ./exports 1

# Export complet, config dans le répertoire du script, sortie dans ./ExportData (exécuter depuis 0_Raw/liste_techant/)
./ExpArchiV7.sh config_export_controlCAD.csv ./ExportData 1
```
