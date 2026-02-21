# ExpArchiV5.sh — Documentation

Script d'export et d'archivage de données depuis une base PostgreSQL vers des fichiers CSV, puis compression en ZIP.

## Synopsis

```bash
./ExpArchiV5.sh <config_file_path> <output_dir_path> [mode]
```

## Paramètres

| Paramètre | Obligatoire | Description |
|-----------|-------------|--------------|
| `config_file_path` | Oui | Chemin vers le fichier CSV de configuration |
| `output_dir_path` | Oui | Répertoire de destination des exports |
| `mode` | Non | Entier entre 0 et 3 (valeur par défaut : 0). *Non utilisé dans la logique actuelle.* |

## Prérequis

- **PostgreSQL** : client `psql` installé et accessible
- **Base de données** : connexion à la base `archivage` (configurée via variables d'environnement ou `.pgpass`)
- **Utilitaires** : `zip`, `find`, `tail`, `xargs`

## Format du fichier de configuration

Fichier CSV avec séparateur `;`, première ligne = en-têtes :

```
Nom;Table;nbr
```

| Colonne | Description |
|---------|-------------|
| `Nom` | Nom de la colonne de valeur dans l'export |
| `Table` | Nom de la table source dans la base `archivage` |
| `nbr` | Nombre de lignes à exporter (entier strictement positif) |

**Exemple :**

```
Nom;Table;nbr
1511115_TempRet;techant5278;120
1511115_PosVan;techant5284;120
T46596;techant22504;120
```

## Comportement

1. **Validation** : Vérification des arguments, existence du fichier de config et du répertoire de sortie.
2. **Export** : Pour chaque ligne du fichier de configuration :
   - Requête SQL : `SELECT date, value AS <Nom>, invalid AS inv FROM <Table> ORDER BY tod DESC FETCH FIRST <nbr> ROWS`
   - Export via `\copy` vers un fichier CSV : `<Table>_<YYYYMMDD>.csv`
3. **Compression** : Création d’une archive ZIP `export_prod_<YYYYMMDD>.zip` contenant tous les CSV générés.
4. **Nettoyage** : Suppression des fichiers CSV individuels après compression.

## Structure des tables sources

Les tables doivent contenir au minimum :

- `tod` : timestamp (utilisé pour le tri et la conversion en date)
- `value` : valeur exportée (renommée selon la colonne `Nom`)
- `invalid` : indicateur d’invalidité (exporté sous le nom `inv`)

## Fichiers générés

| Fichier | Description |
|---------|-------------|
| `<Table>_<YYYYMMDD>.csv` | Fichiers temporaires (supprimés après compression) |
| `export_prod_<YYYYMMDD>.zip` | Archive finale contenant tous les exports du jour |

## Format des CSV exportés

- Séparateur : `;`
- En-tête : oui
- Colonnes : `date` (format DD/MM/YYYY HH24:MI), `<Nom>`, `inv`

## Codes de sortie

| Code | Signification |
|------|---------------|
| 0 | Exécution réussie |
| 1 | Erreur d’arguments, fichier ou répertoire manquant |

## Exemples d’utilisation

```bash
# Export standard
./ExpArchiV5.sh config_export_controlCAD.csv /usr1/LYNX/client/sami8/FICHIER/DataCAD/

# Avec mode explicite
./ExpArchiV5.sh config_export_controlCAD.csv ./exports 2
```

## Gestion des erreurs

- Lignes mal formées (champs vides, `nbr` invalide) : avertissement, passage à la ligne suivante
- Échec d’export `psql` : message d’erreur, poursuite du traitement
- Échec de création du ZIP : message d’erreur, les CSV ne sont pas supprimés
