#!/usr/bin/env python
# coding: utf-8

# In[17]:


import pytest
import weathermax
from unittest import TestCase
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from os import path

#Testing 
#test from directory with csv and parquet files.

#open files in excel note down row details for maximum temps
df1 = pd.read_csv(os.path.abspath('weather.20160201.csv'))
df2 = pd.read_csv(os.path.abspath('weather.20160301.csv'))

df_maxtemp = max(df1['ScreenTemperature'].max(),df2['ScreenTemperature'].max())

#initialize weather max class.
new_weather = weathermax1.WeatherMax(os.getcwd())

class Testfile(TestCase):
    
    #attribute test

    def test_directorypath():
        datapath = new_weather.directorypath
        assert path.exists(datapath) == True
    
    #parquet dataset schema test
    
    def test_builddataset():
        new_weather.builddataset()
        mnth2path = os.getcwd() + '\weather_results\ObsYear=2016\ObsMonth=2'
        mnth3path = os.getcwd() + '\weather_results\ObsYear=2016\ObsMonth=3'
        mnth2file = os.getcwd() + '\weather.20160201.parquet.snappy'
        assert path.isdir(mnth2path) == True
        assert path.isdir(mnth3path) == True
        assert path.isfile(mnth2file) == True
    
    #test parquet dataset to dataframes
    
    def test_pqresult(self):
        weather_data = pq.ParquetDataset('weather_results/')
        table = weather_data.read()
        weather_table_df = table.to_pandas()
        weather_result = weather_table_df.loc[weather_table_df['ScreenTemperature'].idxmax()]
        pqresult = weather_result['ScreenTemperature']
        assert pqresult == df_maxtemp
        
    #test class objects to static value
    
    def test_weathermax(self):
        classtemp = new_weather.weather_temp()
        assert classtemp == 15.8
        

# In[ ]:




