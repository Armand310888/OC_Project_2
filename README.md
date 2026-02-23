# OC_Project_2 – Script Python de traitement de données

## Description
Ce projet est réalisé dans le cadre de la formation *Développeur d’applications Python* chez OpenClassrooms.

Il s’agit d’un script Python implémentant une pipeline ETL (Extract - Transform - Load) simple permettant de :
- récupérer et extraire les données depuis un site web (HTML),
- effectuer des transformations simples de ces données pour les rendre exploitables,
- charger les données sous forme de fichiers CSV et JPG.

L’objectif principal est de mettre en pratique les bases:
- du langage Python: utilisation de listes, dictionnaires, bibliothèques, modules, fonctions, méthodes, classes, boucles, structures conditionnelles, etc.
- de la structuration d’un projet, de l'écriture des docstrings, des commentaires, du refactoring et de la correction de bugs.
- de l’utilisation de Git et GitHub.

## Fonctionnalités:
- Extraction des données pour toutes les catégories de produits
- Génération d'un fichier CSV par catégorie, avec les données requises dans l'énoncé
- Téléchargement des images produits
- Validation de l'intégrité des images téléchargées via Pillow
- Journalisation des erreurs dans le CSV et dans un fichier log

## Résultats générés:
- output/data/ : fichiers CSV par catégorie
- output/images/ : images produits validées
- output/logs/ : informations et erreurs relevées lors de l'exécution du script

## Robustesse:
Le script est conçu pour continuer son exécution même:
- en cas d'échec de téléchargement d'image. Les erreurs sont journalisées via le module logging (dans un fichier et sur la console) et dans le CSV  via les colonnes:
    - image_download_status
    - image_error
- en cas d'impossibilité à extraire l'une des données suivantes pour un produit, que ce soit parce que la donnée n'existe pas, ou que l'ETL ne parvienne plus à la retrouver (ex: en cas d'évolution de la structure du HTML): Product Description, UPC, Price (incl. tax), Pricer (excl. tax), Availability, Review_rating

En revanche, le script est conçu pour stopper son exécution en cas d'impossibilité à atteindre la page d'accueil du site web ou bien une page de catégorie de livres du site web.

Le script affiche un décompte des lignes produits et des images exportées, pour vérification simple de sa bonne exécution.

Les erreurs les plus courantes sont répertoriées dans le fichier log, ainsi que dans les fichiers CSV en ce qui concerne le téléchargement des images.

## Choix techniques:
- Utilisation de BeautifullSoup pour le parsing HTML
- Utilisation de Pillow pour la validation d'intégrité des images

## Améliorations possibles:
- gestion des exception
- robustesse en termes de ciblage des balises HTML (en cas de modification du code HTML)
- parallélisation des téléchargements d'images
- ajout de tests unitaires
- complétion et amélioration du système de journalisation
- intégrer des docstrings dans le script, associés à des fonctions par exemple.

## Prérequis
- Python 3.x > Python 3.7
- Les dépendances listées dans `requirements.txt`

## Installation
1. Cloner le dépôt :
```sh
git clone <https://github.com/Armand310888/OC_Project_2.git>
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
```sh
OC_Project_2
├── src
│   └── main.py
├── README.md
└── requirements.txt
```

## Auteur
Projet réalisé par Armand de la Porte des Vaux dans le cadre de la formation OpenClassrooms.