#!/usr/bin/env python
# coding: utf-8

# In[17]:


import pytest
from unittest import TestCase
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


#Testing


#open files in excel note down row details for maximum temps
df1 = pd.read_csv(os.path.abspath('weather.20160201.csv'))
df2 = pd.read_csv(os.path.abspath('weather.20160301.csv'))

maxtemp = max(df1['ScreenTemperature'].max(),df2['ScreenTemperature'].max())

class Testfile(TestCase):
    
    def testexpected(self):
        weather_data = pq.ParquetDataset('weather_results/')
        table = weather_data.read()
        weather_table_df = table.to_pandas()
        
        assert expected == maxtemp


# In[16]:



    


# In[14]:





# In[15]:





# In[ ]:




