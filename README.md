# SentimentAnalysis
récupération et analyses des données depuis le réseau social twitter
utilisation des patrons de conception Singleton et AbstractFactory
la solution est composée de 3 projets:
-Kafka : pour la récupération des tweets et le stokage dans les brokers de kafka
-Spark : pour l'analyse des données provenant de kafka et leur stockage dans le hdfs ou l'Elasticsearch
-Visualisation de données avec Kibana.
