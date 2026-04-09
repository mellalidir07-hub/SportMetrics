# SportMetrics
# 🏀 Pipeline ELT & IA pour l'Analyse de Performance sportive pour un club de basketball professionnel

## 1. Description du Projet 📝

Dans le sport de haut niveau, la multiplication des sources de données (IoT cardiaque, statistiques, médicales) représente un capital inexploité. Ce projet transforme ce défi en un actif stratégique pour le club.

Ce projet relève ce défi en concevant et déployant une architecture ELT (*Extract, Load, Transform*) moderne, robuste et automatisée dans Google Cloud. L'objectif est de passer d'une gestion intuitive à une prise de décision s'appuyant sur des preuves factuelles pour les staffs technique et médical.

Nous avons fusionné des données hétérogènes (IoT, Stats) dans un Data Warehouse certifié et alimenté **cinq analyses prédicitives spécialisés**. Ces analyses anticipent la fatigue, préviennent les blessures et optimisent la composition d'équipe, le tout restitué de manière interactive dans **Power BI**.

## 2. Architecture Technique (Stack) 🛠️

Ce projet utilise une stack data moderne et scalable :

* **Ingestion (E) :** [n8n](etl_n8n/) (workflows automatisés).
* **Stockage (L) :** [Google Cloud BigQuery](https://cloud.google.com/bigquery).
* **Transformation (T) :** [dbt Cloud](dbt_project/) (Analytics Engineering certifié).
* **Orchestration :** [Apache Airflow](airflow_dags/) (Orchestration des dépendances).
* **IA & Machine Learning :** [Notebooks Python](notebooks_ml/) (5 analyses prédictives).
* **Visualisation (BI) :** [Microsoft Power BI](powerbi/) (Dashboards interactifs).

## 3. Structure du Dépôt 📁

Le dépôt est structuré de manière modulaire pour refléter les différentes briques de l'architecture ELT :

/SportMetrics/
├── /etl_n8n/           <-- Export JSON du workflow d'ingestion automatisé.
├── /dbt_project/       <-- Code source complet du projet dbt Cloud (modèles SQL, sources, schéma sémantique).
├── /airflow_dags/     <-- Fichier Python définissant le DAG d'orchestration (chaîne de dépendances).
├── /notebooks_ml/     <-- Notebooks Jupyter pour l'entraînement, le tuning et l'évaluation des 5 modèles d'IA.
├── /powerbi/           <-- Fichier de projet Power BI (.pbix).
├── .gitignore          <-- Fichier CRUCIAL excluant les fichiers inutiles ou sensibles.
└── README.md           <-- Ce fichier, vitrine du projet.


## 4. Résultats Clés & Impact Business 🚀

* Le modèle de **Prévention Blessures** identifie le **seuil critique de 20% de risque**, déclenché principalement par une augmentation brutale de la charge cardiaque (Ratio Charge Aiguë/Chronique > 1.5).
* Le modèle de **Impact Fatigue** & Dashboards BI révèlent que la gestion de la forme physique de l’équipe est défaillante, la fatigue due au temps de jeu (manque de rotation) et au sur-entrainement est un des facteurs clés de la baisse de performance en match.

---

**Auteur :** `Idir Mellal` - `Projet final de formation Data Analyst & IA, 2026` - `La Capsule`

---
