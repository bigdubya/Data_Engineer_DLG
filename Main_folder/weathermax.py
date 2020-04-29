#!/usr/bin/env python
# coding: utf-8

# In[1]:





# In[36]:


import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


class WeatherMax():
    '''
    pass file directory path with forward slashes
    '''
    def __init__(self,directorypath):
        self.directorypath = directorypath
        
    def csvtodataset(self):
        os.chdir(self.directorypath)
        
        #read csv files in directory
        
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
              
    def builddataset(self):
        self.csvtodataset()
        
    def weatherresults(self):
        weather_data = pq.ParquetDataset('weather_results/')
        table = weather_data.read()
        weather_table_df = table.to_pandas()
        weather_result = weather_table_df.loc[weather_table_df['ScreenTemperature'].idxmax()]
        temp = weather_result['ScreenTemperature']
        date = weather_result['ObservationDate'][:9]
        region = weather_result['Region']

        print("Which date was the hottest day? ",date)
        print("") 
        print("What was the temperature on that day? ",temp)
        print("")        
        print("In which region was the hottest day? ",region) 
        
    def prntresults(self):
        self.weatherresults()
        
    


# In[37]:





# In[40]:





# In[ ]:


WeatherMax()

