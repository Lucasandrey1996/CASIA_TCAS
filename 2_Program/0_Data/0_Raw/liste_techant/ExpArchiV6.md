# ExpArchiV6.sh — Documentation

Script générique d'export et d'archivage de données depuis une base PostgreSQL vers des fichiers CSV, avec compression et options de transfert distant.

**Projet** : Export des données de production LYNX — AFF-25-0887  
**Auteur** : Lucas Andrey  
**Date** : 17.10.2025

---

## Synopsis

```bash
./ExpArchiV6.sh <config_file_path> <output_dir_path> [mode]
```

## Paramètres

| Paramètre | Obligatoire | Description |
|-----------|-------------|--------------|
| `config_file_path` | Oui | Chemin vers le fichier CSV de configuration |
| `output_dir_path` | Oui | Répertoire de destination des exports |
| `mode` | Non | Entier entre 0 et 3 (valeur par défaut : 0). Voir [Modes de fonctionnement](#modes-de-fonctionnement). |

## Modes de fonctionnement

| Mode | Description |
|------|--------------|
| **0** | Par défaut. Export limité à `nbr_val` lignes par table. |
| **1** | Export des tables **complètes** (ignore `nbr_val`). ⚠️ Temps d'exécution élevé. |
| **2** | Envoi du ZIP vers `replica8:/usr1/LYNX/client/sami8/FICHIER/EXTRACTIONS/COMMON/PowerBI/` (à configurer). |
| **3** | Cumul des modes 1 et 2 : export complet + transfert distant. |

## Format du fichier de configuration

Fichier CSV avec séparateur `;`, première ligne = en-têtes :

```
nom;pkey_techant;nbr_val
```

| Colonne | Description |
|---------|--------------|
| `nom` | Identifiant / libellé (utilisé pour les logs) |
| `pkey_techant` | Nom de la table source dans la base `archivage` |
| `nbr_val` | Nombre de lignes à exporter (entier > 0). Ignoré en mode 1 ou 3. |

## Comportement

1. **Validation** : Arguments, existence du fichier de config et du répertoire de sortie.
2. **Export** : Pour chaque ligne du fichier de configuration :
   - En mode 0 ou 2 : `FETCH FIRST nbr_val ROWS ONLY`
   - En mode 1 ou 3 : pas de limite (export complet)
   - Export via `\copy` vers `<pkey_techant>_<YYYYMMDD>.csv`
3. **Compression** : Création de `export_prod_<YYYYMMDD>.zip`.
4. **Nettoyage** : Suppression des CSV individuels.
5. **Transfert** (modes 2 et 3) : Envoi du ZIP vers replica8 (actuellement commenté, à valider).

## Format des CSV exportés

- Séparateur : `;`
- En-tête : oui
- Colonnes : `date` (DD/MM/YYYY HH24:MI), `value`, `inv`

> **Note** : La colonne de valeur est toujours nommée `value` dans l'export (contrairement à V5 qui utilise le champ `Nom` du config).

## Fichiers générés

| Fichier | Description |
|---------|-------------|
| `<pkey_techant>_<YYYYMMDD>.csv` | Fichiers temporaires (supprimés après compression) |
| `export_prod_<YYYYMMDD>.zip` | Archive finale |

---

## Différences avec ExpArchiV5.sh

| Aspect | ExpArchiV5 | ExpArchiV6 |
|--------|------------|------------|
| **Paramètre `mode`** | Validé mais **jamais utilisé** | **Utilisé** : 4 modes (0, 1, 2, 3) |
| **Export limité** | Toujours limité à `nbr` lignes | Mode 0/2 : limité ; Mode 1/3 : **export complet** |
| **Nom colonne valeur** | `value AS ${nom}` — nom dynamique depuis le config | `value AS value` — toujours `value` |
| **Ligne invalide (`nbr`)** | `continue` — passe à la ligne suivante | `exit 1` — **arrêt du script** |
| **Transfert distant** | Aucun | Modes 2 et 3 : envoi vers replica8 (code commenté) |
| **En-tête script** | Minimal | En-tête projet/auteur/date détaillé |
| **Nom ZIP** | `export_prod_<timestamp>.zip` | Identique |
| **Format config** | `Nom;Table;nbr` | `nom;pkey_techant;nbr_val` (même structure) |

### Résumé des changements fonctionnels

1. **Modes opérationnels** : V6 exploite le paramètre `mode` (export complet, transfert distant).
2. **Robustesse** : V6 stoppe immédiatement en cas de `nbr_val` invalide au lieu de continuer.
3. **Uniformité des exports** : V6 utilise toujours la colonne `value` ; V5 personnalise le nom selon le config.
4. **Évolution prévue** : V6 prévoit un transfert SCP vers PowerBI (à activer/configurer).

### Point d'attention

Dans le script V6, les conditions `[[ MODE -eq 1 ]]` et `[[ MODE -eq 2 ]]` devraient utiliser `$MODE` pour référencer la variable. Sans le `$`, la variable n'est pas développée et le mode peut ne pas être appliqué correctement.
