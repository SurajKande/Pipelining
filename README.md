# This repository is based on intoduction to piplining ETL

## Contents of repository:

1.  [Introduction to computation frameworks](https://github.com/SurajKande/Pipelining/blob/master/Parallel_computation_frameworks.ipynb) : 

    * contains information on hadoop, hive, spark and a example of spark loading data from dataset. 

2.  [Data ingestion using Singer](https://github.com/SurajKande/Pipelining/blob/master/data_ingestion_with_Singer.ipynb) : 

    * contains information on Data lakes and Singer library and its implementation.

3.  [Data ingestion with pandas](https://github.com/SurajKande/Pipelining/blob/master/streamlined_data_ingestion_with_pandas.ipynb) : 

    * contains information on loading using pandas i.e importing data from flat-files, excel-files, database and JSON.

4.  [Intoduction to ETL](https://github.com/SurajKande/Pipelining/blob/master/ETL.ipynb) : 

    * contains information on ETL

5.  [Introduction to pyspark](https://github.com/SurajKande/Pipelining/blob/master/introduction_to_pyspark.ipynb) : 

    * contains information on pyspark with an example
    
6.  [casestudy recommended system](https://github.com/SurajKande/Pipelining/blob/master/ETL_casestudy_recommended_system.ipynb)

7.  [A simple ETL implementation](https://github.com/SurajKande/Pipelining/blob/master/simple_ETL_data_pipeline.ipynb) :
  
    * Implemented a ETL pipeline by extracting data from data source and loading the data into database after data transformation 

7.  [ETL implementation using Airflow](https://github.com/SurajKande/Pipelining/blob/master/etl-airflow.py) :
  
    * Implemented a ETL-pipeline using the airflow scheduler in ec2 server (http://13.233.123.149:8080/admin/ : to view the airflow dashboard hosted from server)
    
    * the code is scheduled to extract and load the data into sqlite database after applying transformations on the data and to run a ML model on the extracted data and save the ML model as pickle file
    
    * the ML model used in this example is a present in the custom pypi server i have hosted from ec2 server (http://13.235.50.20:8080/ ) how to use the model (https://github.com/SurajKande/python_packaging/tree/master/classification_package) 
