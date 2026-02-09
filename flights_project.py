"""
Projet UA3 - Flights (Spark)

Ce script réalise l'ensemble du pipeline :
  - Préparation et nettoyage des données (3.1)
  - Modélisation avec Spark DataFrames + RDD (3.2)
  - Transformations et actions (3.3)
  - Analyse (3.4)

Structure :
  - prepare_and_clean_data(spark) : lit le CSV brut, vérifie la qualité, applique
    les conversions et corrections, puis sauvegarde une version nettoyée (Parquet).
  - analyze_data(spark, df_clean) : lit les données nettoyées et réalise plusieurs
    analyses (retards moyens, effets du calendrier, répartition des causes, etc.)
    en combinant DataFrames et RDD (paires clé/valeur).
"""

from pyspark.sql import SparkSession, functions as F


# ======================================================================
# 3.1 PREPARATION DES DONNEES
# ======================================================================
def prepare_and_clean_data(spark):
    """
    Étape 3.1 : Préparation des données
      - Importation des données dans Spark (depuis HDFS)
      - Vérification de la qualité : dimensions, schéma, valeurs manquantes,
        incohérences simples
      - Nettoyage minimal : conversions, corrections, filtres
      - Sauvegarde d'un DataFrame nettoyé dans HDFS (Parquet)
    """

    # 1) IMPORTATION DES DONNÉES BRUTES DEPUIS HDFS
    input_path = "hdfs://hadoop-master:9000/user/root/projet_ua3/flights.csv"
    print(f"\n=== [3.1] Lecture du fichier brut : {input_path} ===")
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # --- 3.1.a — Vérification de base : dimensions, types, aperçu ---
    print("\n=== 3.1.a) DIMENSIONS DU DATASET BRUT ===")
    nb_lignes = df.count()
    nb_colonnes = len(df.columns)
    print(f"Nombre de lignes (brut)   : {nb_lignes}")
    print(f"Nombre de colonnes (brut) : {nb_colonnes}")

    print("\n=== 3.1.a) SCHÉMA (TYPES DES COLONNES) ===")
    df.printSchema()

    print("\n=== 3.1.a) APERÇU DES 5 PREMIÈRES LIGNES ===")
    df.show(5, truncate=False)

    # --- 3.1.b — Valeurs manquantes (qualité) ---
    print("\n=== 3.1.b) NOMBRE DE VALEURS MANQUANTES PAR COLONNE ===")
    null_counts = df.select([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ])
    null_counts.show(truncate=False)

    print("\n=== 3.1.b) POURCENTAGE DE VALEURS MANQUANTES PAR COLONNE ===")
    null_pct_exprs = [
        (F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / nb_lignes * 100).alias(c)
        for c in df.columns
    ]
    null_pct = df.select(null_pct_exprs)
    null_pct.show(truncate=False)

    # --- 3.1.c — Conversions et colonne dérivée (DEPARTURE_HOUR) ---
    print("\n=== 3.1.c) CONVERSIONS DE TYPES + COLONNE DEPARTURE_HOUR ===")
    # Conversions : YEAR, MONTH, DAY, etc. en entiers
    df_conv = (
        df
        .withColumn("YEAR", F.col("YEAR").cast("int"))
        .withColumn("MONTH", F.col("MONTH").cast("int"))
        .withColumn("DAY", F.col("DAY").cast("int"))
        .withColumn("DAY_OF_WEEK", F.col("DAY_OF_WEEK").cast("int"))
        .withColumn("DISTANCE", F.col("DISTANCE").cast("int"))
        .withColumn("DEPARTURE_DELAY", F.col("DEPARTURE_DELAY").cast("int"))
        .withColumn("ARRIVAL_DELAY", F.col("ARRIVAL_DELAY").cast("int"))
        .withColumn("ELAPSED_TIME", F.col("ELAPSED_TIME").cast("int"))
        .withColumn("CANCELLED", F.col("CANCELLED").cast("int"))
        .withColumn("DIVERTED", F.col("DIVERTED").cast("int"))
        # Colonne dérivée : heure de départ approximative à partir de HHMM (ex: 1420 -> 14)
        .withColumn(
            "DEPARTURE_HOUR",
            (F.col("SCHEDULED_DEPARTURE") / 100).cast("int")
        )
    )

    print("\nAperçu après conversions (types & DEPARTURE_HOUR) :")
    df_conv.select(
        "YEAR", "MONTH", "DAY", "DAY_OF_WEEK",
        "SCHEDULED_DEPARTURE", "DEPARTURE_HOUR",
        "DISTANCE", "DEPARTURE_DELAY", "ARRIVAL_DELAY",
        "ELAPSED_TIME", "CANCELLED", "DIVERTED"
    ).show(5, truncate=False)

    # --- 3.1.d — Vérification d'incohérences simples ---
    print("\n=== 3.1.d) INCOHÉRENCES : DISTANCE <= 0 ===")
    nb_dist_zero = df_conv.filter(F.col("DISTANCE") <= 0).count()
    print(f"Nombre de vols avec DISTANCE <= 0 : {nb_dist_zero}")

    print("\n=== 3.1.d) INCOHÉRENCES : ELAPSED_TIME <= 0 ===")
    nb_time_zero = df_conv.filter(F.col("ELAPSED_TIME") <= 0).count()
    print(f"Nombre de vols avec ELAPSED_TIME <= 0 : {nb_time_zero}")

    # --- 3.1.e — Nettoyage minimal (filtres + corrections) ---
    print("\n=== 3.1.e) APPLICATION DU NETTOYAGE MINIMAL ===")
    df_clean = (
        df_conv
        # ORIGIN et DESTINATION doivent exister
        .filter(
            F.col("ORIGIN_AIRPORT").isNotNull() &
            F.col("DESTINATION_AIRPORT").isNotNull()
        )
        # Distance valide
        .filter(F.col("DISTANCE").isNotNull() & (F.col("DISTANCE") > 0))
        # Temps de vol valide
        .filter(F.col("ELAPSED_TIME").isNotNull() & (F.col("ELAPSED_TIME") > 0))
        # Retards présents pour l'analyse
        .filter(
            F.col("DEPARTURE_DELAY").isNotNull() &
            F.col("ARRIVAL_DELAY").isNotNull()
        )
    )

    nb_lignes_clean = df_clean.count()
    print(f"Nombre de lignes APRÈS nettoyage : {nb_lignes_clean}")
    print(f"Lignes supprimées : {nb_lignes - nb_lignes_clean}")

    print("\nAperçu des données nettoyées :")
    df_clean.select(
        "YEAR", "MONTH", "DAY", "DAY_OF_WEEK",
        "ORIGIN_AIRPORT", "DESTINATION_AIRPORT",
        "DEPARTURE_HOUR", "DISTANCE",
        "DEPARTURE_DELAY", "ARRIVAL_DELAY", "ELAPSED_TIME"
    ).show(5, truncate=False)

    # --- 3.1.f — Sauvegarde du DataFrame propre ---
    output_path = "hdfs://hadoop-master:9000/user/root/projet_ua3/flights_clean.parquet"
    print(f"\nÉcriture du DataFrame nettoyé dans : {output_path}")

    df_clean.write.mode("overwrite").parquet(output_path)

    print("\n✅ [3.1] Sauvegarde terminée (flights_clean.parquet).")

    return df_clean


# ======================================================================
# 3.2 / 3.3 / 3.4 - MODELISATION, TRANSFORMATIONS, ACTIONS, ANALYSE
# ======================================================================
def analyze_data(spark, df_clean):
    """
    Étapes 3.2, 3.3 et 3.4 :
      - Utilisation de Spark DataFrames comme structure principale,
        avec des RDD (paires clé/valeur) pour illustrer map/flatMap/reduceByKey
      - 5+ transformations : map, flatMap, filter, groupBy, reduceByKey, join...
      - 3+ actions : count, show, take, collect, saveAsTextFile
      - 4 questions d'analyse :
         Q1 - Retard moyen d'arrivée par compagnie
         Q2 - Retard moyen par jour de la semaine
         Q3 - Retard moyen par heure de départ
         Q4 - Répartition des causes de retard (DataFrame + RDD)
    """

    print("\n=== [3.2/3.3/3.4] DÉBUT DE L'ANALYSE ===")

    # ACTION : count
    print("\nNombre de lignes dans le DataFrame nettoyé :", df_clean.count())

    # ------------------------------------------------------------------
    # Petit DataFrame de dimension pour illustrer un JOIN (TRANSFORMATION : join)
    # ------------------------------------------------------------------
    airlines_dim = df_clean.select("AIRLINE").distinct()
    print("\n=== DIMENSION AIRLINES (quelques lignes) ===")
    airlines_dim.show(5, truncate=False)

    df_joined = df_clean.join(airlines_dim, on="AIRLINE", how="inner")
    print("\n=== EXEMPLE DE JOIN (vols + dimension airlines) ===")
    df_joined.select("AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT").show(5, truncate=False)

    # ------------------------------------------------------------------
    # Q1 - Retard moyen d'arrivée par compagnie (DF + RDD)
    # ------------------------------------------------------------------
    print("\n=== Q1 - Retard moyen d'arrivée par compagnie (DataFrame) ===")
    df_delay_by_airline = (
        df_clean
        .groupBy("AIRLINE")  # TRANSFORMATION : groupBy
        .agg(
            F.mean("ARRIVAL_DELAY").alias("avg_arrival_delay"),
            F.count("*").alias("nb_vols")
        )  # TRANSFORMATION : agg
        .orderBy(F.desc("avg_arrival_delay"))  # TRANSFORMATION : orderBy
    )
    df_delay_by_airline.show(10, truncate=False)  # ACTION : show

    print("\n=== Q1 - Retard moyen d'arrivée par compagnie (RDD : map + reduceByKey) ===")
    rdd_airline_delay = (
        df_clean.select("AIRLINE", "ARRIVAL_DELAY")
        .rdd
        .map(lambda row: (row["AIRLINE"], (row["ARRIVAL_DELAY"], 1)))  # TRANSFORMATION : map
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))           # TRANSFORMATION : reduceByKey
        .mapValues(lambda x: x[0] / x[1])
    )

    # ACTION : take
    rdd_results = rdd_airline_delay.take(10)
    for airline, avg_delay in rdd_results:
        print(f"Compagnie {airline} -> retard moyen (min) = {avg_delay:.2f}")

    # Base pour les sorties RDD
    output_base = "hdfs://hadoop-master:9000/user/root/projet_ua3/output"
    rdd_airline_delay.map(
        lambda kv: f"{kv[0]}\t{kv[1]:.2f}"
    ).saveAsTextFile(output_base + "/q1_rdd_avg_delay_by_airline")

    # ------------------------------------------------------------------
    # Q2 - Retard moyen par jour de la semaine (DataFrame)
    # ------------------------------------------------------------------
    print("\n=== Q2 - Retard moyen par jour de la semaine (DataFrame) ===")

    # TRANSFORMATION : filter
    df_non_diverted = df_clean.filter(F.col("DIVERTED") == 0)

    df_delay_by_dow = (
        df_non_diverted
        .groupBy("DAY_OF_WEEK")  # TRANSFORMATION : groupBy
        .agg(
            F.mean("DEPARTURE_DELAY").alias("avg_dep_delay"),
            F.mean("ARRIVAL_DELAY").alias("avg_arr_delay"),
            F.count("*").alias("nb_vols")
        )
        .orderBy("DAY_OF_WEEK")
    )
    df_delay_by_dow.show(truncate=False)  # ACTION : show

    # ------------------------------------------------------------------
    # Q3 - Retard moyen par heure de départ (DataFrame)
    # ------------------------------------------------------------------
    print("\n=== Q3 - Retard moyen par heure de départ (DataFrame) ===")

    df_hour_delay = (
        df_clean
        .filter((F.col("DEPARTURE_HOUR") >= 0) & (F.col("DEPARTURE_HOUR") <= 23))  # TRANSFORMATION : filter
        .groupBy("DEPARTURE_HOUR")                                                # TRANSFORMATION : groupBy
        .agg(
            F.mean("DEPARTURE_DELAY").alias("avg_dep_delay"),
            F.mean("ARRIVAL_DELAY").alias("avg_arr_delay"),
            F.count("*").alias("nb_vols")
        )
        .orderBy("DEPARTURE_HOUR")
    )
    df_hour_delay.show(24, truncate=False)  # ACTION : show

    # ------------------------------------------------------------------
    # Q4 - Répartition des causes de retard (DataFrame + RDD)
    # ------------------------------------------------------------------
    print("\n=== Q4 - Répartition des causes de retard (DataFrame) ===")

    cause_cols = [
        "AIR_SYSTEM_DELAY",
        "SECURITY_DELAY",
        "AIRLINE_DELAY",
        "LATE_AIRCRAFT_DELAY",
        "WEATHER_DELAY"
    ]

    # --- VERSION DATAFRAME ---
    # On calcule la somme des minutes de retard pour chaque colonne
    causes_df = None
    for col_name in cause_cols:
        tmp = df_clean.select(
            F.lit(col_name).alias("cause_name"),
            F.sum(F.col(col_name)).alias("total_minutes")
        )
        causes_df = tmp if causes_df is None else causes_df.union(tmp)

    # On trie par nombre total de minutes de retard
    causes_df = causes_df.orderBy(F.desc("total_minutes"))

    print("\nTotal des minutes de retard par type de cause (DataFrame) :")
    causes_df.show(truncate=False)

    # --- VERSION RDD (MapReduce / clé-valeur) ---
    print("\n=== Q4 - Répartition des causes de retard (RDD : flatMap + reduceByKey) ===")

    rdd_causes = df_clean.select(cause_cols).rdd

    causes_kv = (
        rdd_causes.flatMap(
            lambda row: [
                (cause_name, row[cause_name])
                for cause_name in cause_cols
                if row[cause_name] is not None and row[cause_name] > 0
            ]
        )  # TRANSFORMATION : flatMap
        .map(lambda kv: (kv[0], kv[1]))         # TRANSFORMATION : map
        .reduceByKey(lambda a, b: a + b)        # TRANSFORMATION : reduceByKey
    )

    # ACTION : collect
    causes_totals = causes_kv.collect()
    print("\nTotal des minutes de retard par type de cause (RDD) :")
    for cause_name, total_minutes in causes_totals:
        print(f"{cause_name} -> {total_minutes} minutes")

    # Sauvegarde des résultats RDD dans HDFS
    causes_kv.map(
        lambda kv: f"{kv[0]}\t{kv[1]}"
    ).saveAsTextFile(output_base + "/q4_rdd_total_delay_causes")

    print("\n✅ [3.2/3.3/3.4] Analyses terminées.")



def main():
    spark = (
        SparkSession.builder
        .appName("UA3_Flights_Project")
        .getOrCreate()
    )

    # 🔇 On réduit les logs Spark pour mieux voir les résultats
    spark.sparkContext.setLogLevel("ERROR")

    # 3.1 : préparation et nettoyage
    df_clean = prepare_and_clean_data(spark)

    # 3.2 / 3.3 / 3.4 : modélisation + transformations + analyse
    analyze_data(spark, df_clean)

    spark.stop()



if __name__ == "__main__":
    main()
