#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from os import path
import re

class WeatherMax():
    '''
    pass file directory path with forward slashes
    '''
    def __init__(self,directorypath):
        self.directorypath = directorypath
        self.df = None
        #self.weather_result
        #self.date 
        #self.time 
        #self.region 
        
        
    def csvtodataset(self):
        os.chdir(self.directorypath)
        
        #read csv files in directory

            
    def patable(self):
        for file in os.listdir():
            if file.endswith(".csv"):
                file1 = file
                df = pd.read_csv(os.path.abspath(file1))
                
                #select required columns and enrich data with partition attributes
                
                df = df[['ForecastSiteCode','ObservationTime','ObservationDate','ScreenTemperature','SiteName','Region']]
                df = df.sort_values(by=['ForecastSiteCode','ObservationDate','ObservationTime'])
                df = df.reset_index(drop=True)
                df['ObsYear'] = pd.DatetimeIndex(df['ObservationDate']).year
                df['ObsMonth'] = pd.DatetimeIndex(df['ObservationDate']).month
                df['ObsDay'] = pd.DatetimeIndex(df['ObservationDate']).day

                table = pa.Table.from_pandas(df)

                #create additional files for testing 
                
                file1 = file1.replace(".csv",".")
                file2 = file1 + 'parquet.snappy'
                pq.write_table(table, file2,compression='snappy')

                pq.write_to_dataset(table,root_path='weather_results',partition_cols=['ObsYear','ObsMonth','Region'])  
                
    #create folder schema
    def builddataset(self):
        self.patable()             
        
        
    def weathertable(self):
        weather_data = pq.ParquetDataset('weather_results/')
        table = weather_data.read()
        weather_table_df = table.to_pandas()
        return weather_table_df
  
    
    def weathermax_row(self):
        weather_table_df = self.weathertable()
        weather_result = weather_table_df.loc[weather_table_df['ScreenTemperature'].idxmax()]
        return weather_result
       
     #return max temp date
    def weather_date(self):
        weather_result = self.weathermax_row()
        date = weather_result['ObservationDate'][:9]
        return date
    
    #return max temp
    def weather_temp(self):
        weather_result = self.weathermax_row()
        temp = weather_result['ScreenTemperature']
        return temp
    
    #return max temp region
    def weather_region(self):
        weather_result = self.weathermax_row()
        region = weather_result['Region']
        return region
    
    #print questions and answers
    def prntresult(self):
        a = "Which date was the hottest day? " + str(self.weather_date()) +'\n'
        b = "What was the (max) temperature on that day? " + str(self.weather_temp()) +'\n'
        c = "In which region was the hottest day? " + str(self.weather_region())
        
        result = print(a,b,c)
        return result
