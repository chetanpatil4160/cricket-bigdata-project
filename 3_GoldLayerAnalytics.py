# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Reading Data From Silver Layer
silver_df = spark.table("workspace.default.cricket_silver_current_matches")
display(silver_df)

# COMMAND ----------

gold_match_type_df = silver_df.groupBy("match_type").agg(count('*').alias("Total_match"))
display(gold_match_type_df)

# COMMAND ----------

gold_venue_df = silver_df.groupBy("venue").agg(count('*').alias("Total_match")).orderBy(col("Total_match").desc())
display(gold_venue_df)

# COMMAND ----------

team1_df = silver_df.select(col("team_1").alias("team"))
team2_df = silver_df.select(col("team_2").alias("team"))

all_team_df = team1_df.union(team2_df)

gold_team_df = all_team_df.groupBy("team").agg(count('*').alias("matches_played")).orderBy(col("matches_played").desc())
display(gold_team_df)

# COMMAND ----------


