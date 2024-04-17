# Databricks notebook source
# MAGIC %md
# MAGIC ### Initialize

# COMMAND ----------

path_silver = 'dbfs:/capstone/silver/'
path_gold = 'dbfs:/capstone/gold/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing

# COMMAND ----------

# dbutils.fs.rm(path_gold,recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Latest Dataset from Silver

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

delta_df = spark.read.format("delta").load(path_silver)

# Define a window specification to partition by source key and order by ingestion date
window_spec = Window.partitionBy("source_key").orderBy(desc("ingestion_date"))

# Use row_number() to assign a rank to each row within each source key
ranked_df = delta_df.withColumn("row_num", row_number().over(window_spec))

# Filter for rows where the rank is 1, indicating the maximum ingestion date
latest_df = ranked_df.filter("row_num = 1").drop("row_num")

# Show the result
# display(latest_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### List All Columns

# COMMAND ----------

delta_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Player Stats DataFrame

# COMMAND ----------

# Summary of a player's stats per season

df_playerstats = (latest_df
                   .groupBy(
                        col('game_season')
                        ,col('team_full_name_current')
                        ,col('player_id')
                        ,concat_ws(' ',col('player_first_name'),col('player_last_name')).alias('player')
                        ,concat_ws('|', col('player_id'),col('game_season')).alias('source_key')
                            )
                    .agg(
                        count('game_id').alias('gp')
                        ,(sum('ast')/count('game_id')).alias('apg')
                        ,(sum('blk')/count('game_id')).alias('bpg')
                        ,(sum('min')/count('game_id')).alias('mpg')
                        ,(sum('pts')/count('game_id')).alias('ppg')
                        ,(sum('reb')/count('game_id')).alias('rpg')
                        ,(sum('stl')/count('game_id')).alias('spg')
                        ,(sum('turnover')/count('game_id')).alias('tpg')
                        ,(sum('pf')/count('game_id')).alias('pfpg')
                        ,(sum('fgm')/count('game_id')).alias('fgmpg')
                        ,(sum('fga')/count('game_id')).alias('fgapg')
                        ,(sum('fgm')/sum('fga')).alias('fg_pct')
                        ,(sum('fg3m')/count('game_id')).alias('fg3mpg')
                        ,(sum('fg3a')/count('game_id')).alias('fg3apg')
                        ,(sum('fg3m')/sum('fg3a')).alias('fg3_pct')
                        ,(sum('ftm')/count('game_id')).alias('ftmpg')
                        ,(sum('fta')/count('game_id')).alias('ftapg')
                        ,(sum('ftm')/sum('fta')).alias('ft_pct')
                        ,(((sum('fgm') * 85.910) + (sum('stl') * 53.897) + (sum('fg3m') * 51.757) + (sum('ftm') * 46.845)
                        + (sum('blk') * 39.190) + (sum('oreb') * 39.190) + (sum('ast') * 34.677) + (sum('dreb') * 14.707)
                        - (sum('pf') * 17.174) - ((sum('fta')-sum('ftm')) * 20.091) - ((sum('fga')-sum('fgm')) * 39.190)
                        - (sum('turnover') * 53.897)
                        ) * (1/sum('min'))).alias('per')
                        ,(sum('pts')/(2 * (sum('fga')) + (0.44 * sum('fta')))).alias('ts')
                        ,(sum('ast')/sum('turnover')).alias('ato')
                        ,((sum('fgm') + (0.5 * sum('fg3m')))/sum('fga')).alias('efg')
                    )
                        
)

# display(df_playerstats)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Team Stats DataFrame

# COMMAND ----------

# Summary of a team's stats per season

df_teamstats_by_game = (latest_df
                   .groupBy(
                        col('game_season')
                        ,col('team_id')
                        ,col('team_full_name')
                        ,col('game_id')
                            )
                    .agg(
                        sum('ast').alias('ast')
                        ,sum('blk').alias('blk')
                        ,sum('pts').alias('pts')
                        ,sum('reb').alias('reb')
                        ,sum('stl').alias('stl')
                        ,sum('turnover').alias('turnover')
                        ,sum('pf').alias('pf')
                        ,sum('fgm').alias('fgm')
                        ,sum('fga').alias('fga')
                        ,sum('fg3m').alias('fg3m')
                        ,sum('fg3a').alias('fg3a')
                        ,sum('ftm').alias('ftm')
                        ,sum('fta').alias('fta')
                    )          
)

display(df_teamstats_by_game)


# COMMAND ----------

# Team Average Per Season
df_teamstats_1 = (df_teamstats_by_game
                .groupBy('game_season','team_full_name')
                .agg(avg('ast').alias('apg')
                    ,avg('blk').alias('bpg')
                    ,avg('pts').alias('ppg')
                    ,avg('reb').alias('rpg')
                    ,avg('stl').alias('spg')
                    ,avg('turnover').alias('tpg')
                    ,avg('pf').alias('pfpg')
                    ,avg('fgm').alias('fgmpg')
                    ,avg('fga').alias('fgapg')
                    ,avg('fg3m').alias('fg3mpg')
                    ,avg('fg3a').alias('fg3apg')
                    ,avg('ftm').alias('ftmpg')
                    ,avg('fta').alias('ftapg')
                    ,(sum('ftm')/sum('fta')).alias('ft_pct')
                    ,(sum('fg3m')/sum('fg3a')).alias('fg3_pct')
                    ,(sum('fgm')/sum('fga')).alias('fg_pct')
                    ,concat_ws('|', col('team_full_name'),col('game_season')).alias('source_key')
                    )
                )

# Overall League Average Per Season
df_leaguestats = (df_teamstats_by_game.drop('team_full_name').withColumn('team_full_name',lit('League Overall'))
                .groupBy('game_season','team_full_name')
                .agg(avg('ast').alias('apg')
                    ,avg('blk').alias('bpg')
                    ,avg('pts').alias('ppg')
                    ,avg('reb').alias('rpg')
                    ,avg('stl').alias('spg')
                    ,avg('turnover').alias('tpg')
                    ,avg('pf').alias('pfpg')
                    ,avg('fgm').alias('fgmpg')
                    ,avg('fga').alias('fgapg')
                    ,avg('fg3m').alias('fg3mpg')
                    ,avg('fg3a').alias('fg3apg')
                    ,avg('ftm').alias('ftmpg')
                    ,avg('fta').alias('ftapg')
                    ,(sum('ftm')/sum('fta')).alias('ft_pct')
                    ,(sum('fg3m')/sum('fg3a')).alias('fg3_pct')
                    ,(sum('fgm')/sum('fga')).alias('fg_pct')
                    ,concat_ws('|', col('team_full_name'),col('game_season')).alias('source_key')
                    )
                )

display(df_leaguestats.limit(5))

# COMMAND ----------

# League Top 5 Per Season

df_top5_1 = (delta_df.select('game_season','game_id','game_home_team_id','game_home_team_score','game_visitor_team_id','game_visitor_team_score')
             .distinct()
             )

df_top5_2 = df_top5_1.withColumn("team_id_winner", when(col("game_home_team_score") > col("game_visitor_team_score"), col('game_home_team_id')).otherwise(col('game_visitor_team_id')))

df_top5_3 = delta_df.select('team_id','team_full_name').distinct()

df_top5_4 = df_top5_2.join(df_top5_3, df_top5_2['team_id_winner'] == df_top5_3['team_id'], 'inner').select('game_season','game_id','team_id_winner','team_full_name')

df_top5_5 = (df_top5_4.groupBy('game_season','team_id_winner','team_full_name').agg(count('game_id').alias('wins')))


window_spec = Window.partitionBy('game_season').orderBy(col('wins').desc())

# Add a row number to each row based on the specified window specification
df_top5_6 = df_top5_5.withColumn('row_number', row_number().over(window_spec))

# Filter for rows where row number is less than or equal to 5 (top 5 counts)
df_top5_7 = df_top5_6.filter(col('row_number') <= 5).drop('row_number')

df_top5 = (df_top5_7.withColumnRenamed('game_season','game_season_top5')
            .withColumnRenamed('team_full_name','team_full_name_top5')
            )

# Show the result
df_top5.display()

# COMMAND ----------

df_teamstats_by_game_top5 = (df_teamstats_by_game
                             .join(df_top5
                                   ,(df_teamstats_by_game['game_season'] == df_top5['game_season_top5'])
                                   & (df_teamstats_by_game['team_full_name'] == df_top5['team_full_name_top5'])
                                   ,'inner')
                            .drop('game_season_top5')
                            .drop('team_full_name_top5')
                            .drop('team_id_winner')
                            .drop('wins')
                             )


df_top5stats = (df_teamstats_by_game_top5.drop('team_full_name').withColumn('team_full_name',lit('League Top 5'))
                .groupBy('game_season','team_full_name')
                .agg(avg('ast').alias('apg')
                    ,avg('blk').alias('bpg')
                    ,avg('pts').alias('ppg')
                    ,avg('reb').alias('rpg')
                    ,avg('stl').alias('spg')
                    ,avg('turnover').alias('tpg')
                    ,avg('pf').alias('pfpg')
                    ,avg('fgm').alias('fgmpg')
                    ,avg('fga').alias('fgapg')
                    ,avg('fg3m').alias('fg3mpg')
                    ,avg('fg3a').alias('fg3apg')
                    ,avg('ftm').alias('ftmpg')
                    ,avg('fta').alias('ftapg')
                    ,(sum('ftm')/sum('fta')).alias('ft_pct')
                    ,(sum('fg3m')/sum('fg3a')).alias('fg3_pct')
                    ,(sum('fgm')/sum('fga')).alias('fg_pct')
                    ,concat_ws('|', col('team_full_name'),col('game_season')).alias('source_key')
                    )
                )

# COMMAND ----------

# Combine Team Avg, League Overall Avg, League Top 5 Avg -- Per Season
df_teamstats = df_teamstats_1.union(df_leaguestats).union(df_top5stats)

# COMMAND ----------

display(df_teamstats)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Player Stats DF to Gold Delta

# COMMAND ----------

from delta.tables import *

path_gold_playerstats = path_gold + 'playerstats/'


isDeltaTable = DeltaTable.isDeltaTable(spark, path_gold_playerstats)

if(isDeltaTable):
    deltaTable = DeltaTable.forPath(spark, path_gold_playerstats)

    (deltaTable
    .alias("target")
    .merge(df_playerstats.alias("source"), "target.source_key = source.source_key") #source key is player & season
    .whenMatchedUpdateAll() 
    .whenNotMatchedInsertAll() 
    .execute())
else:
    df_playerstats.write.mode("overwrite").format("delta").partitionBy("game_season").save(path_gold_playerstats)

# COMMAND ----------

display(df_playerstats.groupBy(df_playerstats.source_key).count().filter("count > 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Team Stats DF to Gold Delta

# COMMAND ----------

from delta.tables import *

path_gold_teamstats = path_gold + 'teamstats/'


isDeltaTable = DeltaTable.isDeltaTable(spark, path_gold_teamstats)

if(isDeltaTable):
    deltaTable = DeltaTable.forPath(spark, path_gold_teamstats)

    (deltaTable
    .alias("target")
    .merge(df_teamstats.alias("source"), "target.source_key = source.source_key") #source key is team & season
    .whenMatchedUpdateAll() 
    .whenNotMatchedInsertAll() 
    .execute())
else:
    df_teamstats.write.mode("overwrite").format("delta").partitionBy("game_season").save(path_gold_teamstats)