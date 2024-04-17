# Databricks notebook source
# MAGIC %md
# MAGIC ### Initialize

# COMMAND ----------

path_gold_playerstats = 'dbfs:/capstone/gold/playerstats'
path_gold_teamstats = 'dbfs:/capstone/gold/teamstats'

# COMMAND ----------

from pyspark.sql.functions import *

# filter out players who played less than 2 mins per game
df1_ps = (spark.read.format("delta").load(path_gold_playerstats)).filter(col('mpg') > 2.00)

df1_ts = (spark.read.format("delta").load(path_gold_teamstats))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic Stats

# COMMAND ----------

# 2023 season
df2_ps = df1_ps.filter(col('game_season')== 2023)

df2_ps.describe('mpg').show()

df2_ps.describe('gp').show()

# COMMAND ----------

# import libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

%matplotlib inline

df1pd = df1_ps.toPandas()

corr = df1pd[['per', 'apg', 'bpg', 'mpg','ppg', 'rpg','spg','tpg']].corr()

# plot a correlation heatmap
sns.heatmap(corr, cmap='RdBu', fmt='.2f', square=True, linecolor='white', annot=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter Player Stats

# COMMAND ----------

# grab dataset one standard deviation away from mean
df3_ps = df2_ps.filter((df2_ps.gp >= 5) & (df2_ps.mpg >= 8.5))

display(df3_ps.select('team_full_name_current','player','per'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Players With Below-Median PER

# COMMAND ----------

medper = df3_ps.groupby("game_season").agg(median("per").alias('median_per')).collect()[0]['median_per']

display(medper)
# grab players that are less than median PER
df4_ps = df3_ps.filter(col('per')< medper)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframes for Visualizations

# COMMAND ----------

# Create Team Widget
teams = df4_ps.select("team_full_name_current").distinct().rdd.map(lambda row : row[0]).collect()

teams.sort()

dbutils.widgets.dropdown("Team", "Los Angeles Lakers", [str(x) for x in teams])

# COMMAND ----------

# Inefficient Players for the team
df5_ps = df4_ps.select('per','mpg','team_full_name_current','player').filter(col('team_full_name_current') == getArgument('Team'))

display(df5_ps)

# COMMAND ----------

# Create Player Widget
players = df5_ps.select("player").distinct().rdd.map(lambda row : row[0]).collect()

players += ['placeholder']

players.sort()

dbutils.widgets.dropdown("Player", "placeholder", [str(x) for x in players])

# Player Stat
df6_ps = df1_ps.filter(col('player') == getArgument('Player'))

display(df6_ps)


# COMMAND ----------

# PER for All Team Players
df7_ps = df1_ps.select('per','mpg','team_full_name_current','player').filter((col('team_full_name_current') == getArgument('Team')) & (col('game_season') == 2023))


display(df7_ps)

# COMMAND ----------

display(df1_ts.filter(((col('team_full_name') == getArgument('Team')) | (col('team_full_name')=='League Overall') | (col('team_full_name')=='League Top 5')) & (col('game_season')== 2023)))