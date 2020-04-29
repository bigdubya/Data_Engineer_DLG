# Data_Engineer_DLG
Python data engineering test 
Convert the weather data into parquet format. Set the raw group to appropriate value you see fit for this data. 
The converted data should be queryable to answer the following question. 
- Which date was the hottest day? 
- What was the temperature on that day? 
- In which region was the hottest day? 
Please provide the source code, tests, documentations and any assumptions you have made. 

Assumptions:

The hottest day is defined as the day that recorded the maximum single screen temperature across the 2 .csv files.
All temperatures with a value of -99 will be assumed to be outside the scope of max temperature selection - no change required.
Where region and country do not align region will be assumed to be correct - no change required.
Only a subset of fields will be required for comparison, analysis and queries.
It is expected that the underlying csv files will grow on a monthly basis and that comparisons and aggregations will span monthly intervals and be across regions - the data will be partitioned based on these assumptions with performance in mind. 

source code contained in .py file 
source, documentation and testing in .ipynb file

Test Cases
create dataframes from the 2 csv filter for max temps
check parquet dataset schema and metadata
check parquet file metadata and schema

Result

Which date was the hottest day?  2016-03-1
What was the temperature on that day?  15.8
In which region was the hottest day?  Highland & Eilean Siar
