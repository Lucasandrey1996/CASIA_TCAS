# --- Script configuration ---

# Check for a valid number of arguments
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "Usage: $0 <config_file_path> <output_dir_path> [mode]"
  echo "The 'mode' parameter is optional. It must be a positive integer between 0 and 3."
  exit 1
fi

# Assign command-line arguments to variables
CONFIG_FILE="$1" #"/usr1/LYNX/client/sami8/FICHIER/DataCAD/config_export_controlCAD.csv"
OUTPUT_DIR="$2" #"/usr1/LYNX/client/sami8/FICHIER/DataCAD/"
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
    echo "Warning: Invalid nbr config ('$nbr') must be a positive integer."
    continue
  fi

  # Define the output file path for the current query
  OUTPUT_FILE="${OUTPUT_DIR}/${table}_${TIMESTAMP}.csv"

  echo "Extracting: $table (nom: $nom, nbr: $nbr)"

  # Use a here document to pass the SQL command to psql
  psql -c "\copy (SELECT to_char(to_time(tod), 'DD/MM/YYYY HH24:MI') AS date, value AS ${nom}, invalid AS inv FROM ${table} ORDER BY tod DESC FETCH FIRST ${nbr} ROWS ONLY) TO '${OUTPUT_FILE}' WITH CSV DELIMITER ';' HEADER;" archivage

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

echo "End of the script."