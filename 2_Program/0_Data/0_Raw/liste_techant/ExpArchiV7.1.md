# ExpArchiV7.1.sh — Documentation

Script d'export et d'archivage de données depuis PostgreSQL. Version **V7.1** : corrige l'erreur SQL sur les identifiants débutant par un chiffre (ex. `1511181_TempRet`).

---

## Synopsis

```bash
./ExpArchiV7.1.sh <config_file_path> <output_dir_path> [mode]
```

## Modification V7 → V7.1

En PostgreSQL, les identifiants (noms de colonne) **commençant par un chiffre** doivent être entre guillemets doubles. Le script génère désormais `value AS "1511181_TempRet"` au lieu de `value AS 1511181_TempRet`, ce qui évite l'erreur de syntaxe.

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
| `Table` | Nom de la table source dans la base `archivage` | Format obligatoire : `techantXXXXX` |
| `nbr` | Nombre de lignes à exporter (entier > 0). Ignoré en mode 1. | Entier strictement positif |

## Comportement

1. **Vérifications initiales** : dépendances, arguments, existence des chemins, permissions, connexion PostgreSQL.
2. **Validation des lignes de config** : contrôle de `nom`, `table` (format `techantXXXXX`), `nbr`.
3. **Limite de lignes** : maximum 5000 lignes de données.
4. **Export** : Mode 0 (limité) ou 1 (complet), temporisation 50 ms entre requêtes, identifiants SQL entre guillemets pour les noms débutant par un chiffre.
5. **Compression** : création de `export_SSTCAD_<YYYYMMDD>.zip`.
6. **Nettoyage** : suppression des CSV individuels après compression.

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

## Exemples

```bash
# Export limité (mode 0 par défaut)
./ExpArchiV7.1.sh config_export_controlCAD.csv ./ExportData

# Export complet de toutes les tables
./ExpArchiV7.1.sh config_export_controlCAD.csv ./ExportData 1
```
