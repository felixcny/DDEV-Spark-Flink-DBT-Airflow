# Réponses aux questions analytiques

## Questions Spark Batch Taxi

### 1. Quelle est la distribution des durées de trajets ?

La durée des trajets a été calculée dans le pipeline PySpark à partir de la différence entre `tpep_dropoff_datetime` et `tpep_pickup_datetime`.

Les trajets ont ensuite été classés en plusieurs catégories :

* Trajets courts : moins de 10 minutes
* Trajets moyens : entre 10 et 30 minutes
* Trajets longs : plus de 30 minutes

Observation :
La majorité des trajets observés à NYC sont des trajets courts ou moyens. Les trajets longs sont moins fréquents mais génèrent généralement des montants totaux plus élevés.

Requête SQL :

```sql
SELECT
    CASE
        WHEN duree_trajet < 10 THEN 'Court'
        WHEN duree_trajet BETWEEN 10 AND 30 THEN 'Moyen'
        ELSE 'Long'
    END AS categorie_trajet,
    COUNT(*) AS nombre_trajets
FROM trip_enriched
GROUP BY categorie_trajet;
```

Reponse

| Catégorie | Nombre de trajets |
|---|---:|
| Court (< 10 min) | 1 284 890 |
| Moyen (10 à 30 min) | 1 998 878 |
| Long (> 30 min) | 441 121 |

Analyse :

Les trajets moyens représentent la catégorie dominante avec près de 2 millions de trajets. Les trajets courts sont également très fréquents, tandis que les trajets longs restent minoritaires. Cela confirme une utilisation principalement urbaine et locale du service de taxi à New York.
---

### 2. Les longs trajets reçoivent-ils plus de pourboires ?


Le pourcentage de pourboire a été calculé avec :

```sql
(tip_amount / fare_amount) * 100
```

Les trajets ont été regroupés selon plusieurs catégories de distance :

0 à 2 km
2 à 5 km
plus de 5 km

Requête SQL :

```sql
SELECT
    categorie_distance,
    ROUND(AVG(pourcentage_pourboire)::numeric, 2) AS avg_tip_percentage
FROM trip_enriched
GROUP BY categorie_distance;
```

Reponse 

categorie_distance | avg_tip_percentage 
--------------------+-------------------- 
0-2 km | 21.27
2-5 km | 15.91 
>5 km | 9.96 
(3 rows)

Analyse :

Les trajets courts présentent les pourcentages moyens de pourboire les plus élevés. À l’inverse, les trajets longs reçoivent des pourboires proportionnellement plus faibles. Cela peut s’expliquer par le coût déjà élevé des longues courses, qui réduit le pourcentage de pourboire donné par les clients.



---

### 3. Quelles sont les heures de prise en charge les plus chargées ?

Le modèle `trip_summary_per_hour` permet d’agréger le nombre total de trajets par heure de prise en charge.

Requête SQL :

```sql
SELECT
    pickup_hour,
    total_trips
FROM trip_summary_per_hour
ORDER BY total_trips DESC
LIMIT 10;
```
Reponse

 pickup_hour     | total_trips
---------------------+-------------
 2026-01-30 18:00:00 |       11739
 2026-01-30 19:00:00 |       11677
 2026-01-15 18:00:00 |       11323
 2026-01-29 18:00:00 |       10785
 2026-01-15 17:00:00 |       10555
 2026-01-29 21:00:00 |       10547
 2026-01-23 18:00:00 |       10413
 2026-01-23 19:00:00 |       10412
 2026-01-10 18:00:00 |       10356
 2026-01-10 19:00:00 |       10331
(10 rows)


Analyse :

Les pics de trajets sont principalement observés entre 17h et 19h. Ces horaires correspondent aux heures de pointe de fin de journée, lorsque les utilisateurs se déplacent après le travail. Les périodes du soir concentrent donc le plus grand volume de trajets taxi.


---

### 4. Existe-t-il une corrélation entre la distance du trajet et le pourcentage de pourboire ?


Une analyse de corrélation a été réalisée entre la distance des trajets (`trip_distance`) et le pourcentage de pourboire (`pourcentage_pourboire`).

Requête SQL :

```sql
SELECT
    ROUND(
        CORR(trip_distance, pourcentage_pourboire)::numeric,
        4
    ) AS correlation_distance_tip
FROM trip_enriched
WHERE trip_distance IS NOT NULL
  AND pourcentage_pourboire IS NOT NULL;
```

Reponse 

correlation_distance_tip
--------------------------
                  -0.0005
(1 row)

Analyse :

La corrélation obtenue est extrêmement proche de zéro. Cela indique qu’il n’existe pas de relation linéaire significative entre la distance parcourue et le pourcentage de pourboire donné par les clients. Le comportement des pourboires semble donc dépendre d’autres facteurs que la seule distance du trajet.


---

# Questions Spark Streaming / Flink

### 5. Quelle est la température moyenne lors des pics de trajets ?

Requête SQL :

```sql
SELECT
    heure_prisencharge,
    ROUND(AVG(temperature)::numeric, 2) AS avg_temperature,
    COUNT(*) AS total_trips
FROM trip_enriched
GROUP BY heure_prisencharge
ORDER BY total_trips DESC;
```

Reponse 

  heure_prisencharge | avg_temperature | total_trips
--------------------+-----------------+-------------
                 18 |                 |      265574
                 17 |                 |      252173
                 19 |                 |      234542
                 16 |           20.00 |      227893
                 15 |                 |      221677
                 20 |                 |      212883
                 21 |                 |      212544
                 14 |                 |      207507
                 13 |                 |      195708
                 22 |                 |      194801
                 12 |                 |      187435
                 11 |                 |      171729
                 10 |                 |      161419
                  9 |                 |      156894
                 23 |                 |      152622
                  8 |                 |      148281
                  7 |                 |      113463
                  0 |                 |      111867
                  1 |                 |       77821
                  6 |                 |       63721
                  2 |                 |       54105
                  3 |                 |       38645
                  5 |                 |       32912
                  4 |                 |       28673
(24 rows)

Analyse :


Les pics de trajets sont principalement observés entre 17h et 19h.
La seule donnée météo disponible après jointure concerne 16h, avec une température moyenne de 20°C. Cela montre que le pipeline météo fonctionne, mais que le jeu météo simulé ne couvre pas toutes les heures présentes dans les données taxi.

---



### 6. Quel est l’impact du vent ou de la pluie sur le nombre de trajets ?

Le modèle `trip_enriched` permet d’associer les trajets taxi à des catégories météo issues du pipeline streaming.

Requête SQL :

```sql
SELECT
    weather_category,
    COUNT(*) AS total_trips
FROM trip_enriched
GROUP BY weather_category;
```
Reponse



 weather_category | total_trips
------------------+-------------
 Clair            |      227893
                  |     3496996


Analyse :

Les données enrichies montrent actuellement une catégorie météo disponible principalement pour les trajets associés à la condition « Clair ». Une grande partie des trajets ne possède cependant pas encore de catégorie météo associée, ce qui indique une couverture météo partielle dans les données simulées utilisées pour le projet.

Le pipeline de jointure météo fonctionne néanmoins correctement pour les heures couvertes par les données météo disponibles.

---

# Questions dbt / Analyse

### 7. Quels comportements de trajets observe-t-on selon les types de météo ?

Le modèle `trip_enriched` permet d’analyser les durées moyennes de trajets selon les catégories météo disponibles.

Requête SQL :

```sql
SELECT
    weather_category,
    ROUND(AVG(duree_trajet)::numeric, 2) AS avg_duration,
    COUNT(*) AS total_trips
FROM trip_enriched
GROUP BY weather_category;
```
Reponse 

weather_category | avg_duration | total_trips
------------------+--------------+-------------
 Clair            |        19.47 |      227893
                  |        17.04 |     3496996


Analyse :

Les trajets associés à une météo claire présentent une durée moyenne légèrement plus élevée que les trajets sans données météo associées.

La majorité des trajets ne possède cependant pas encore d’information météo disponible, ce qui limite l’analyse détaillée des comportements selon les conditions climatiques.
---

### 8. À quelle heure observe-t-on le plus de clients à haute valeur ?

Le modèle `high_value_customers` identifie les groupes de passagers ayant :

- plus de 10 trajets
- plus de 300 dollars dépensés au total
- plus de 15 % de pourboire moyen

Requête SQL :

```sql
SELECT
    heure_prisencharge,
    COUNT(*) AS total_high_value_trips
FROM trip_enriched
WHERE passenger_count IN (
    SELECT passenger_count
    FROM high_value_customers
)
GROUP BY heure_prisencharge
ORDER BY total_high_value_trips DESC;
```

Reponse
 heure_prisencharge | total_high_value_trips
--------------------+------------------------
                 18 |                 190751
                 17 |                 190007
                 16 |                 180864
                 15 |                 178249
                 14 |                 166619
                 19 |                 163393
                 13 |                 155588
                 12 |                 149672
                 20 |                 148319
                 21 |                 144922
                 11 |                 135720
                 10 |                 125831
                 22 |                 124424
                  9 |                 114350
                  8 |                  95986
                 23 |                  89584
                  7 |                  68561
                  0 |                  61838
                  1 |                  40262
                  6 |                  34756
                  2 |                  27588
                  3 |                  18850
                  5 |                  17281
                  4 |                  13409
(24 rows)

Analyse :

Les trajets associés aux clients à haute valeur sont principalement concentrés entre 14h et 18h, avec un pic observé à 18h. Cette période correspond aux heures de forte activité urbaine et aux déplacements de fin de journée.
---

### 9. La météo influence-t-elle le comportement en matière de pourboires ?

Le modèle `trip_enriched` permet d’analyser le pourcentage moyen de pourboire selon les catégories météo disponibles.

Requête SQL :

```sql
SELECT
    weather_category,
    ROUND(AVG(pourcentage_pourboire)::numeric, 2) AS avg_tip_percentage
FROM trip_enriched
GROUP BY weather_category;
```
Reponse

weather_category | avg_tip_percentage
------------------+--------------------
 Clair            |              18.22
                  |              15.77

Analyse :

Les trajets associés à une météo claire présentent un pourcentage moyen de pourboire plus élevé que les trajets ne disposant pas d’information météo.

Même si les données météo restent partielles dans le jeu simulé utilisé pour le projet, les résultats montrent que les conditions météorologiques peuvent avoir une influence sur le comportement des clients en matière de pourboires.
---
