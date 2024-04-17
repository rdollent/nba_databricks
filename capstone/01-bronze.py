# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

pip install requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Directory

# COMMAND ----------

#  create directory

path_bronze = 'dbfs:/capstone/bronze/'

try:
    dbutils.fs.ls(path_bronze)
    print("directory exists!")
except Exception as e:
    # create directory
    dbutils.fs.mkdirs(path_bronze)
    print("directory created!")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Download Stats Data

# COMMAND ----------

# https://stackoverflow.com/questions/67234001/how-do-i-combine-several-json-api-responses-into-a-single-variable-object

import json
import time
import requests
from requests.exceptions import HTTPError
from datetime import datetime as dt
from sys import exit

# season_list = ['2019','2020','2021','2022','2023']

season_list = ['2023']

# limit 60 calls per minute
set_delay = 0.3

api_request_limit = 1

start_index = 0
base_url = f"https://www.balldontlie.io/api/v1/stats?per_page=100&seasons[]="


curr_data = {}

current_date = dt.now().strftime("%Y%m%d")

for season in season_list:

    player_data = [] # Needs to be a list.
    response = requests.get(base_url + season)
    res = response.json()
    total_pages = res['meta']['total_pages'] + 1

    display(res['meta'])

    season_url = f'{base_url}{season}&page='

    print(season + '-' + str(total_pages))

    for curr_start_index in range(start_index, total_pages, api_request_limit):

        api_url = season_url + str(curr_start_index)
        
        try:
            response = requests.get(api_url)
            # if successful, no exceptions
            response.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
        else:
            # print('Success!')
            curr_data = response.json()

            player_data = [*player_data, *curr_data['data']] 

        time.sleep(set_delay)
        print(f'Counter: {curr_start_index}. Delay: {set_delay}. Record Count: {len(player_data)}.')

    json_str = json.dumps(player_data)


    dbutils.fs.put(f"{path_bronze}nba-season{season}-{current_date}.json",json_str,overwrite=True)
    print(f'nba-season{season}-{current_date}.json written')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Download Player Data

# COMMAND ----------

# start_index = 0
base_url = "https://www.balldontlie.io/api/v1/players?per_page=100"

curr_data = {}

player_data = [] # Needs to be a list.
response = requests.get(base_url)
res = response.json()
total_pages = res['meta']['total_pages'] + 1

player_url = f'{base_url}&page='

print(str(total_pages))

for curr_start_index in range(start_index, total_pages, api_request_limit):

    api_url = player_url + str(curr_start_index)
    
    try:
        response = requests.get(api_url)
        # if successful, no exceptions
        response.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        # print('Success!')
        curr_data = response.json()

        player_data = [*player_data, *curr_data['data']] 

    time.sleep(set_delay)
    print(f'Counter: {curr_start_index}. Delay: {set_delay}. Record Count: {len(player_data)}.')

json_str = json.dumps(player_data)


dbutils.fs.put(f"{path_bronze}nba-player-{current_date}.json",json_str,overwrite=True)
print(f'nba-player-{current_date}.json written')


# COMMAND ----------

dbutils.fs.ls('dbfs:/capstone/bronze')

# COMMAND ----------

