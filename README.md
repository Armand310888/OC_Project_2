# OC_Project_2 – Script Python de traitement de données

## Description
Ce projet est réalisé dans le cadre de la formation *Développeur d’applications Python* chez OpenClassrooms.

Il s’agit d’un script Python implémentant une pipeline ETL (Extract - Transform - Load) simple permettant de :
- récupérer et extraire les données depuis une page web (HTML),
- effectuer des transformations simples,
- produire un résultat exploitable (fichiers CSV et fichiers JPG).

L’objectif principal est de mettre en pratique les bases:
- du langage Python: utilisation de listes, dictionnaires, bibliothèques, modules, fonctions, méthodes, boucles, structures conditionnelles, exceptions, etc.
- de la structuration d’un projet, de l'écriture de docstrings, des commentaires, du refactoring et de la  correction de bugs.
- de l’utilisation de Git et GitHub.

## Fonctionnalités:
- Extraction des données pour toutes les catégories de produits
- Génération d'un fichier CSV par catégorie, avec les données requises dans l'énoncé
- Téléchargement des images produits
- Validation de l'intégrité des images téléchargées via Pillow
- Journalisation des erreurs dans le CSV

## Résultats générés:
- output/data/ : fichiers CSV par catégorie
- output/images/ : images produits validées

## Robustesse:
Le script est conçu pour continuer son exécution même:
- en cas d'échec de téléchargement d'image. Les erreurs sont journalisées dans le CSV via les colonnes:
    - image_download_status
    - image_error
- en cas d'absence de description d'un produit

Le script affiche un décompte des lignes produits et des images exportées, pour vérification simple de sa bonne exécution.

## Choix techniques:
- Utilisation de BeautifullSoup pour le parsing HTML
- Utilisation de Pillow pour la validation d'intégrité des images

## Améliorations possible:
- gestion des exception
- robustesse en termes de ciblage des balises HTML (en cas de modification du code HTML)
- parallélisation des téléchargements d'images
- ajout de tests unitaires
- mise en place de logs structurés

## Prérequis
- Python 3.x
- Les dépendances listées dans `requirements.txt`

## Installation
1. Cloner le dépôt :
```bash
git clone <git@github.com:Armand310888/OC_Project_2.git>
```
2. Se placer dans le dossier du projet :
```sh
cd OC_Project_2
```
3. Installer les dépendances :
```sh
pip install -r requirements.txt
```

## Utilisation
```sh
python src/main.py
```

## Structure du projet
OC_Project_2
├── src
│   └── main.py
├── README.md
└── requirements.txt

## Auteur
Projet réalisé par Armand de la Porte des Vaux dans le cadre de la formation OpenClassrooms.