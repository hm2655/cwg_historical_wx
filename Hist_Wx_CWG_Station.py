# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 17:21:07 2019

@author: harshit.mahajan
"""
import os
import pandas as pd
import time
print('Current OS: ' + os.name)
import jaydebeapi
from jpype import *
import numpy as np
import csv
import datetime as dt
###################################

df = pd.read_csv("weather.csv")
df.head()

###########################

records  = (df['station_id'], df['date'],df['Tmin'],df['Tmax'],df['Tavg'],df['Prcp'],df['Norm30'],df['Norm10'], df['Hdd'], df['Cdd'])
df.item()
start_time = time.time()
insert_query = """ insert into ef_dev.weather_data values (?,?,?,?,?,?,?,?,?,?)"""
records = df
curs.execute(insert_query, df)


print("  ---------------%s seconds -------- to copy data from staging to rawdata table" %(time.time() - start_time))

##############################

for index, rows in df.iterrows():
    curs.execute("insert into ef_dev.weather_data values (?,?,?,?,?,?,?,?,?,?)", 
             (rows['station_id'], rows['date'],rows['Tmin'],rows['Tmax'],rows['Tavg'],rows['Prcp'],rows['Norm30'],rows['Norm10'], rows['Hdd'], rows['Cdd']))
    
##############################

query = """select * from ef_dev.weather_view"""
df = pd.read_sql(query, conn)
latest_date = df.loc[0][1]
