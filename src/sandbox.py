import logging

# Création du logger au début du main
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Création du handler console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Création du handler fichier
## création du doddiser log
logs_folder = output_folder / "logs"
logs_folder.mkdir(parents=True, exist_ok=True)

## Création du fichier log
file_handler = logging.FileHandler(logs_folder / "etl.log", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)

# Définition du format
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s, datefmt="%Y-%m-%d %H:%M:%S")
                              
# Attacher le format aux handlers
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Attacher les handlers au logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)
