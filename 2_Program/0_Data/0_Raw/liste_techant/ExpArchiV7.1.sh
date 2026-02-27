#!/bin/bash
# ************************************************************************************************************************
# * ExpArchiV7.1 : V7 + quoting des identifiants SQL (noms débutant par chiffre)
# ************************************************************************************************************************

# --- Script configuration ---
CONFIG_MAX_LINES=5000
PSQL_DELAY_SEC=0.05  # 50 ms entre chaque requête PSQL
PSQL_DB="archivage"
# Regex pour valider les identifiants SQL (nom de colonne)
VALID_ID_REGEX='^[a-zA-Z0-9_]+$'
# Regex pour valider le nom de table : techantXXXXX (XXXXX = entier strictement positif)
VALID_TABLE_REGEX='^techant[1-9][0-9]*$'

# Vérification des dépendances
for cmd in psql zip; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Error: '$cmd' is required but not installed."
    exit 1
  fi
done

# Check for a valid number of arguments
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "Usage: $0 <config_file_path> <output_dir_path> [mode]"
  echo "The 'mode' parameter is optional. 0 = limited export, 1 = full table export."
  exit 1
fi

# Assign command-line arguments to variables
CONFIG_FILE="$1"
OUTPUT_DIR="$2"
MODE=0  # Default value for 'mode'

# Check if a 'mode' argument was provided
if [ "$#" -eq 3 ]; then
  MODE="$3"
  if ! [[ "$MODE" =~ ^[0-9]+$ ]] || (( MODE < 0 )) || (( MODE > 1 )); then
    echo "Error: The 'mode' parameter must be 0 or 1."
    exit 1
  fi
fi

# Check if the configuration file exists
if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Error: unable to find '$CONFIG_FILE'."
  exit 1
fi

# Check if the output directory exists
if [[ ! -d "$OUTPUT_DIR" ]]; then
  echo "Error: The output directory '$OUTPUT_DIR' does not exist."
  exit 1
fi

# Vérification des permissions d'écriture
if ! touch "$OUTPUT_DIR/.write_test" 2>/dev/null; then
  echo "Error: No write permission on '$OUTPUT_DIR'."
  exit 1
fi
rm -f "$OUTPUT_DIR/.write_test"

# Test de connexion PostgreSQL
if ! psql -d "$PSQL_DB" -c "SELECT 1" &>/dev/null; then
  echo "Error: Cannot connect to PostgreSQL database '$PSQL_DB'. Check PGHOST, PGPORT, PGUSER, PGPASSWORD."
  exit 1
fi

# Check config file line count (header + data)
CONFIG_LINES=$(wc -l < "$CONFIG_FILE")
DATA_LINES=$((CONFIG_LINES - 1))
if [[ $DATA_LINES -gt $CONFIG_MAX_LINES ]]; then
  echo "Warning: Config has $DATA_LINES lines. Limiting to $CONFIG_MAX_LINES lines."
fi

# --- Script Logic ---

TIMESTAMP=$(date '+%Y%m%d')

echo "Start extracting configuration from $CONFIG_FILE (mode=$MODE, max $CONFIG_MAX_LINES lines, delay ${PSQL_DELAY_SEC}s between queries)."

# Loop to process each line of the configuration file (limited to CONFIG_MAX_LINES)
tail -n +2 "$CONFIG_FILE" | head -n "$CONFIG_MAX_LINES" | while IFS=';' read -r nom table nbr rest; do
  nom=$(echo "$nom" | xargs)
  table=$(echo "$table" | xargs)
  nbr=$(echo "$nbr" | xargs)

  if [[ -z "$nom" || -z "$table" || -z "$nbr" ]]; then
    echo "Warning: Empty field in '$CONFIG_FILE'."
    continue
  fi

  if ! [[ "$nbr" =~ ^[0-9]+$ ]] || (( nbr <= 0 )); then
    echo "Warning: Invalid nbr config ('$nbr') must be a positive integer."
    continue
  fi

  if ! [[ "$nom" =~ $VALID_ID_REGEX ]]; then
    echo "Warning: Invalid nom ('$nom'). Only alphanumeric and underscore allowed."
    continue
  fi
  if ! [[ "$table" =~ $VALID_TABLE_REGEX ]]; then
    echo "Warning: Invalid table ('$table'). Must match pattern techantXXXXX (XXXXX = positive integer)."
    continue
  fi

  OUTPUT_FILE="${OUTPUT_DIR}/${table}_${TIMESTAMP}.csv"

  if [[ $MODE -eq 1 ]]; then
    echo "Extracting (full): $table (nom: $nom)"
    psql -d "$PSQL_DB" -c "\copy (SELECT to_char(to_time(tod), 'DD/MM/YYYY HH24:MI') AS date, value AS \"${nom}\", invalid AS inv FROM ${table} ORDER BY tod DESC) TO '${OUTPUT_FILE}' WITH CSV DELIMITER ';' HEADER;"
  else
    echo "Extracting: $table (nom: $nom, nbr: $nbr)"
    psql -d "$PSQL_DB" -c "\copy (SELECT to_char(to_time(tod), 'DD/MM/YYYY HH24:MI') AS date, value AS \"${nom}\", invalid AS inv FROM ${table} ORDER BY tod DESC FETCH FIRST ${nbr} ROWS ONLY) TO '${OUTPUT_FILE}' WITH CSV DELIMITER ';' HEADER;"
  fi

  if [[ $? -eq 0 ]]; then
    echo "Export successful to '$OUTPUT_FILE'."
  else
    echo "Error: export failed for table '$table'."
  fi

  sleep "$PSQL_DELAY_SEC"
done

echo "All tables have been processed."

# --- Compression des fichiers ---
echo "Creating ZIP archive..."

ZIP_FILE="${OUTPUT_DIR}/export_SSTCAD_${TIMESTAMP}.zip"
CSV_COUNT=$(find "$OUTPUT_DIR" -maxdepth 1 -type f -name "*_${TIMESTAMP}.csv" 2>/dev/null | wc -l)

if [[ $CSV_COUNT -eq 0 ]]; then
  echo "Warning: No CSV files to compress. Skipping ZIP creation."
else
  find "$OUTPUT_DIR" -maxdepth 1 -type f -name "*_${TIMESTAMP}.csv" -exec zip -j "$ZIP_FILE" {} +
  if [[ $? -eq 0 ]]; then
    echo "ZIP file successfully created: '$ZIP_FILE' ($CSV_COUNT file(s))"
    echo "Cleaning up CSV files..."
    find "$OUTPUT_DIR" -maxdepth 1 -type f -name "*_${TIMESTAMP}.csv" -delete
    echo "Cleanup ended."
  else
    echo "Error during creation of the ZIP file."
  fi
fi

echo "End of the script."
