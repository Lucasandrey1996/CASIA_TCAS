##########!/bin/bash
# ************************************************************************************************************************
# * PROJECT : Export des donnée de production LYNX - AFF-25-0887
# ************************************************************************************************************************
# *
# * AUTHOR	   : Lucas Andrey
# * Date	   : 17.10.2025
# *
# ************************************************************************************************************************
# *
# * FUNCTION	   :	Export des donnés / Script générique
# *
# * DESCRIPTION	   :	Ce script à pour but de génériser l'export des données. Il fonctionne de la sorte:
# * 					- le premier argument est le chemin vers le fichier de config qui est un CSV et contient colonnes nom;pkey_techant;nbr_val
# * 					- Le script lis chacune des lignes et génère un fichier CSV unique à chaque ligne, dans le répertoire indiqué dans le deuxième argument.
# * 					- Le 3ème argument permet des variations de fonctionnement du script.
# * 
# * MODE		   :	Voici les modes possibles:
# * 					- 0 par défaut si il n'est pas renseigné
# * 					- 1 ne tient pas compte du "nbr_val" dans le fichier de config. il exporte les tables complètes !!! temps d'exécution !!!
# * 					- 2 permet d'envoyer le résultat dans replica8:/usr1/LYNX/client/sami8/FICHIER/EXTRACTIONS/COMMON/PowerBI/
# * 					- 3 cumul des modes 1 & 2
# *						
# * MODIFICATION   :
# *				17.10.2025 : Création
# *
# ************************************************************************************************************************ 
# --- Script configuration ---

# Check for a valid number of arguments
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "Usage: $0 <config_file_path> <output_dir_path> [mode]"
  echo "The 'mode' parameter is optional. It must be a positive integer between 0 and 3."
  exit 1
fi

# Assign command-line arguments to variables
CONFIG_FILE="$1" #"/usr1/LYNX/client/sami8/FICHIER/DataProd/0_config_tables.csv"
OUTPUT_DIR="$2" #"/usr1/LYNX/client/sami8/FICHIER/DataProd/"
MODE=0  # Default value for 'mode'

# Check if a 'mode' argument was provided
if [ "$#" -eq 3 ]; then
  MODE="$3"
  # Validate the 'mode' value
  if ! [[ "$MODE" =~ ^[0-9]+$ ]] || (( MODE < 0 )) || (( MODE > 3 )); then
    echo "Error: The 'mode' parameter must be a positive integer between 0 and 3."
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

# --- Script Logic ---

# Get the current timestamp for the output filenames
TIMESTAMP=$(date '+%Y%m%d')

echo "Start extracting configuration from $CONFIG_FILE."

# Loop to process each line of the configuration file
tail -n +2 "$CONFIG_FILE" | while IFS=';' read -r nom table nbr rest; do
  # Trim whitespace from variables
  nom=$(echo "$nom" | xargs)
  table=$(echo "$table" | xargs)
  nbr=$(echo "$nbr" | xargs)

  # Check for malformed lines
  if [[ -z "$nom" || -z "$table" || -z "$nbr" ]]; then
    echo "Warning: Empty field in '$CONFIG_FILE'."
    continue
  fi

  # Check if 'nbr' is a positive integer
  if ! [[ "$nbr" =~ ^[0-9]+$ ]] || (( nbr <= 0 )); then
    echo "Warning: Invalid nbr config must be a positive integer."
	echo "$nom"
    exit 1
  fi

  # Define the output file path for the current query
  OUTPUT_FILE="${OUTPUT_DIR}/${table}_${TIMESTAMP}.csv"

  echo "Extracting: $table (nom: $nom, nbr: $nbr)"

  # Use a here document to pass the SQL command to psql
  if [[ MODE -eq 1 ]] || [[ MODE -eq 3 ]]; then
    psql -c "\copy (SELECT to_char(to_time(tod), 'DD/MM/YYYY HH24:MI') AS date, value AS value, invalid AS inv FROM ${table} ORDER BY tod DESC) TO '${OUTPUT_FILE}' WITH CSV DELIMITER ';' HEADER;" archivage
  else
    psql -c "\copy (SELECT to_char(to_time(tod), 'DD/MM/YYYY HH24:MI') AS date, value AS value, invalid AS inv FROM ${table} ORDER BY tod DESC FETCH FIRST ${nbr} ROWS ONLY) TO '${OUTPUT_FILE}' WITH CSV DELIMITER ';' HEADER;" archivage
  fi
  #  psql -c "\copy (SELECT to_char(to_time(tod), 'DD/MM/YYYY HH24:MI') AS date, value AS value, invalid AS inv FROM ${table} ORDER BY tod DESC FETCH FIRST ${nbr} ROWS ONLY) TO '${OUTPUT_FILE}' WITH CSV DELIMITER ';' HEADER;" archivage
  
  
  # Check if the psql command was successful
  if [[ $? -eq 0 ]]; then
    echo "Export successful to '$OUTPUT_FILE'."
  else
    echo "Error: export failed for table '$table'."
  fi
done

echo "All tables have been processed."

# --- Compression des fichiers ---
echo "Creating ZIP archive..."

# Define the name of the ZIP file
ZIP_FILE="${OUTPUT_DIR}/export_prod_${TIMESTAMP}.zip"

# Find all generated CSV files and add them to the zip archive
find "$OUTPUT_DIR" -maxdepth 1 -type f -name "*_${TIMESTAMP}.csv" -exec zip -j "$ZIP_FILE" {} +

# Check if the zip command was successful
if [[ $? -eq 0 ]]; then
  echo "ZIP file successfully created: '$ZIP_FILE'"
  # Clean up the individual CSV files after compression
  echo "Cleaning up CSV files..."
  find "$OUTPUT_DIR" -maxdepth 1 -type f -name "*_${TIMESTAMP}.csv" -delete
  echo "Cleanup ended."
else
  echo "Error during creation of the ZIP file."
fi

# Send the fille for Export !!! A corriger/valider !!!
if [[ MODE -eq 2 ]] || [[ MODE -eq 3 ]]; then
  #scp -p ZIP_FILE replica8:/usr1/LYNX/client/sami8/FICHIER/EXTRACTIONS/COMMON/PowerBI/
  #sleep 5
  #rm -f ZIP_FILE
fi

echo "End of the script."