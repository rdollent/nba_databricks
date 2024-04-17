# Databricks notebook source
# MAGIC %md
# MAGIC ### Initialize

# COMMAND ----------

path_bronze = 'dbfs:/capstone/bronze/'
path_silver = 'dbfs:/capstone/silver/'
path_bronze_processed = 'dbfs:/capstone/bronze/processed/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing

# COMMAND ----------

# dbutils.fs.rm(path_silver,recurse=True)

# COMMAND ----------

# yr = '2023'
# fname = yr + '-20231115.json'
# # fname = yr + '-20231211.json'
# matches = [fname]
# # matches = ['2022-20231128.json']
# # matches = ['2022-20231201.json']

# matches = ['player-20231216','.json']

# # matches = ['2022-', '.json']

# # move files
# for f in dbutils.fs.ls(path_bronze_processed):

#     if all(x in f.name for x in matches):
#         # Original file path
#         original_path = f.path

#         # New file path (rename)
#         new_path = original_path.replace("bronze/processed","bronze/")

#         # Rename the file
#         dbutils.fs.cp(original_path, new_path)

#         dbutils.fs.rm(original_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

try:
    dbutils.fs.ls(path_silver)
    print("directory exists!")
except Exception as e:
    # create directory
    dbutils.fs.mkdirs(path_silver)
    print("directory created!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load JSONs To DataFrames

# COMMAND ----------

from functools import reduce

bronze_files = dbutils.fs.ls(path_bronze)

matches_season = ['season','.json']

matches_player = ['player','.json']


json_filepath_season = []
json_filepath_player = []

for f in bronze_files:
    if all(x in f.name for x in matches_season):
        json_filepath_season += [f.path]
    
    if all(x in f.name for x in matches_player):
        json_filepath_player += [f.path]


df_season_list = [spark.read.json(filepath) for filepath in json_filepath_season]
# [df1,df2,df3]

df_season = reduce(lambda df1, df2: df1.union(df2), df_season_list)
df_player = spark.read.json(json_filepath_player)

display(df_season.limit(3))
display(df_player.limit(3))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform DataFrame

# COMMAND ----------

# from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import *

# https://stackoverflow.com/questions/34271398/flatten-nested-spark-dataframe

def flatten_structs(nested_df):
    stack = [((), nested_df)]

    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()
        
        # array_cols = [
        #     c[0]
        #     for c in df.dtypes
        #     if c[1][:5] == "array"
        # ]
        
        flat_cols = [
            col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]
        
        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))
        
    return nested_df.select(columns)


# COMMAND ----------

from pyspark.sql.types import IntegerType,DecimalType,DateType,LongType,StringType,BooleanType
from pyspark.sql.functions import col

# Define Schema On Write
def transform_df(input_df):
    return (
        input_df
            .withColumn("ast", col("ast").cast(IntegerType())) 
            .withColumn("blk", col("blk").cast(IntegerType())) 
            .withColumn("dreb", col("dreb").cast(IntegerType())) 
            .withColumn("fg3_pct", col("fg3_pct").cast(DecimalType(5,2))) 
            .withColumn("fg3a", col("fg3a").cast(IntegerType())) 
            .withColumn("fg3m", col("fg3m").cast(IntegerType())) 
            .withColumn("fg_pct", col("fg_pct").cast(DecimalType(5,2))) 
            .withColumn("fga", col("fga").cast(IntegerType())) 
            .withColumn("fgm", col("fgm").cast(IntegerType())) 
            .withColumn("ft_pct", col("ft_pct").cast(DecimalType(5,2))) 
            .withColumn("fta", col("fta").cast(IntegerType())) 
            .withColumn("ftm", col("ftm").cast(IntegerType())) 
            .withColumn("game_date", col("game_date").cast(StringType())) 
            .withColumn("game_home_team_id", col("game_home_team_id").cast(StringType())) 
            .withColumn("game_home_team_score", col("game_home_team_score").cast(IntegerType())) 
            .withColumn("game_id", col("game_id").cast(StringType())) 
            .withColumn("game_period", col("game_period").cast(IntegerType())) 
            .withColumn("game_postseason", col("game_postseason").cast(BooleanType())) 
            .withColumn("game_season", col("game_season").cast(IntegerType())) 
            .withColumn("game_status", col("game_status").cast(StringType())) 
            .withColumn("game_time", col("game_time").cast(StringType())) 
            .withColumn("game_visitor_team_id", col("game_visitor_team_id").cast(StringType())) 
            .withColumn("game_visitor_team_score", col("game_visitor_team_score").cast(IntegerType())) 
            .withColumn("id", col("id").cast(StringType()))
            .withColumn("min", col("min").cast(IntegerType())) 
            .withColumn("oreb", col("oreb").cast(IntegerType())) 
            .withColumn("pf", col("pf").cast(IntegerType())) 
            .withColumn("player_first_name", col("player_first_name").cast(StringType())) 
            .withColumn("player_id", col("player_id").cast(StringType())) 
            .withColumn("player_last_name", col("player_last_name").cast(StringType())) 
            .withColumn("pts", col("pts").cast(IntegerType())) 
            .withColumn("reb", col("reb").cast(IntegerType())) 
            .withColumn("stl", col("stl").cast(IntegerType())) 
            .withColumn("team_abbreviation", col("team_abbreviation").cast(StringType())) 
            .withColumn("team_city", col("team_city").cast(StringType())) 
            .withColumn("team_conference", col("team_conference").cast(StringType())) 
            .withColumn("team_division", col("team_division").cast(StringType())) 
            .withColumn("team_full_name", col("team_full_name").cast(StringType())) 
            .withColumn("team_id", col("team_id").cast(StringType())) 
            .withColumn("team_name", col("team_name").cast(StringType())) 
            .withColumn("turnover", col("turnover").cast(IntegerType()))
            .withColumn("team_full_name_current", col("team_full_name_current").cast(StringType()))
            .withColumn("source_key",concat_ws("|",col("player_id"),col("game_id")))
            .withColumn("ingestion_date",lit(from_utc_timestamp(current_timestamp(),"America/Edmonton")))
            .drop("player_height_feet") 
            .drop("player_height_inches") 
            .drop("player_position") 
            .drop("player_weight_pounds")
            .drop("player_team_id") 
   
    )

# COMMAND ----------

# Flatten DFs
df2_season = flatten_structs(df_season)
df2_player = flatten_structs(df_player)

# Add player's current team
df2_player = (df2_player
              .select('id','team_full_name')
              .withColumnRenamed('id','id_player')
              .withColumnRenamed('team_full_name','team_full_name_current')
              )

df2 = df2_season.join(df2_player, df2_season["player_id"] == df2_player["id_player"], "inner").drop('id_player')


# define dataframe schema
df3 = transform_df(df2)

# remove duplicates
# df4 = df3.dropDuplicates()
df4 = df3.distinct()

display(df4.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Uniqueness and Duplicates

# COMMAND ----------

# check if id is unique

isUnique = False

if(df4.select("id").distinct().count() == df4.count()):
    isUnique = True
else:
    raise ValueError("id is not unique!")

print(df4.count())

print(isUnique)

# Check for Duplicates
# https://stackoverflow.com/questions/50122955/check-for-duplicates-in-pyspark-dataframe
display(df4.groupBy(df4.columns).count().filter("count > 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Hash

# COMMAND ----------

hash_col = df4.columns

# remove Ingestion Date column from list of columns to be hashed
hash_col.remove("ingestion_date")

# Add a SHA-256 hash column
df5 = df4.withColumn("hashed", sha2(concat_ws("|",*[col(col_name).cast(StringType()) for col_name in hash_col]), 256))
# df5 = df4.withColumn("hash", md5(concat_ws("|", *hash_col)))

display(df5.limit(3))


# COMMAND ----------

# Check for Null Hash
display(df5.select("*").filter(col("hashed").isNull()).limit(100))

# Check for duplicate Hashes
display(df5.groupBy(df5.hashed).count().filter("count > 1"))

# COMMAND ----------

# Assign to Final DF
final_df = df5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save JSON as Delta

# COMMAND ----------

from delta.tables import *

isDeltaTable = DeltaTable.isDeltaTable(spark, path_silver)

if(isDeltaTable):
    deltaTable = DeltaTable.forPath(spark, path_silver)

    (deltaTable
    .alias("target")
    .merge(final_df.alias("source"), "target.hashed = source.hashed") 
    # .whenMatchedUpdateAll() 
    .whenNotMatchedInsertAll() 
    .execute())
else:
    final_df.write.mode("overwrite").format("delta").partitionBy("game_season").save(path_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Move to Bronze Processed

# COMMAND ----------

move_list = json_filepath_season + json_filepath_player

# move files
for f in move_list:

    # Original file path
    original_path = f

    # New file path (rename)
    new_path = original_path.replace("bronze","bronze/processed")

    # Rename the file
    dbutils.fs.cp(original_path, new_path)

    dbutils.fs.rm(original_path)