import sqlite3
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime

import numpy as np
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from genpactclassification import decision_tree_classifier as dt

import pickle

class Pipeline():
    def __init__(self):
        self.population = None
        self.unemployment = None

    def extract(self):
        
        url_popul_est = 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/metro/totals/cbsa-est2019-alldata.csv'
        url_unemployment = 'https://www.ers.usda.gov/webdocs/DataFiles/48747/Unemployment.xls?v=318.5'

        self.population = pd.read_csv(url_popul_est, encoding='ISO-8859-1')
        self.unemployment = pd.read_excel(url_unemployment, skiprows=7)

    def transform(self):
        # formatting Population dataset

        # keep the relevant columns only i.e. the columns that contain year-population-estimate and index names
        pop_idx = ['CBSA', 'MDIV', 'STCOU', 'NAME', 'LSAD']
        pop_cols = [c for c in self.population.columns if c.startswith('POPEST')]
        population = self.population[pop_idx + pop_cols].copy()

        # melt, "unpivot" the yearly rate values (from wide format 'columns' to long format 'rows')
        self.population = population.melt(id_vars=pop_idx,
                                          value_vars=pop_cols,
                                          var_name='YEAR',
                                          value_name='POPULATION_EST')
        
        self.population['YEAR'] = self.population['YEAR'].apply(lambda x: x[-4:]) 


        # formatting Unemployment dataset

        # keep the relevant columns only i.e. unemployment-rate-year and names
        unemp_idx = ['FIPStxt', 'Stabr', 'area_name']
        unemp_cols = [c for c in self.unemployment.columns if c.startswith('Unemployment_rate')]
        unemployment = self.unemployment[unemp_idx + unemp_cols].copy()

        # melt, "unpivot" the yearly rate values (from wide format 'columns' to long format 'rows')
        self.unemployment = unemployment.melt(id_vars=unemp_idx,
                                              value_vars=unemp_cols,
                                              var_name='Year',
                                              value_name='Unemployment_rate')
        
        self.unemployment['Year'] = self.unemployment['Year'].apply(lambda x: x[-4:])


    def load(self):
        db = DB()
        self.population.to_sql('population', db.conn, if_exists='append', index=False)
        self.unemployment.to_sql('unemployment', db.conn, if_exists='append', index=False)

class DB(object):

    def __init__(self, db_file='/home/ec2-user/db.sqlite'):
        self.conn = sqlite3.connect(db_file)
        self.cur = self.conn.cursor()
        self.__init_db()

    def __del__(self):
        self.conn.commit()
        self.conn.close()

    def __init_db(self):
        table1 = f"""CREATE TABLE IF NOT EXISTS population1(
              CBSA INTEGER,
              MDIV REAL,
              STCOU INTEGER,
              NAME TEXT,
              LSAD TEXT,
              YEAR INTEGER,
              POPULATION_EST INTEGER
                );"""

        table2 = f"""CREATE TABLE IF NOT EXISTS unemployment1(
            FIPStxt INTEGER,
            Stabr TEXT,
            area_name TEXT,
            Year INTEGER,
            unemployment_rate REAL
            );"""

        self.cur.execute(table1)
        self.cur.execute(table2)

###############################

def etl():
  pipeline = Pipeline()

  pipeline.extract()
  
  pipeline.transform()
  
  pipeline.load()

def runModel():
    conn = sqlite3.connect('/home/ec2-user/db.sqlite')        # connecting to sqlite database
    sql1 = """
    SELECT NAME, YEAR, POPULATION_EST
    FROM population
    WHERE STCOU is NULL;
    """

    df1 = pd.read_sql(sql1, conn)

    sql2 = """
    SELECT Stabr, area_name, unemployment_rate
    FROM unemployment
    """
    df2 = pd.read_sql(sql2,conn)

    df1["STATE"]=df1["NAME"].str.split(",").str[1].str[-2:]
    df2["STATE"] = df2["Stabr"]

    merged_dataframe = pd.merge(left= df1.groupby("STATE")["POPULATION_EST"].mean().reset_index() , right = df2.groupby("STATE")["Unemployment_rate"].mean().reset_index(), left_on = 'STATE', right_on = 'STATE')

    threshold = merged_dataframe['Unemployment_rate'].describe([0.2,0.4,0.75]).values[4:8]
    
    def return_unemployment_zone(x, tr=threshold):
        if ( x <= tr[1]):
            return "low"
        elif ( x > tr[1] and x <= tr[2] ):
            return "medium"
        elif ( x > tr[2] and x <= tr[3]) :
            return "high"
        else:
            return "critical"
    
    merged_dataframe["zone"] = merged_dataframe['Unemployment_rate'].apply(return_unemployment_zone)

    le = preprocessing.LabelEncoder()
    le = le.fit_transform(merged_dataframe['zone'])
    merged_dataframe['zone'] = le

    X = merged_dataframe.loc[:,'POPULATION_EST':'zone'].values
    y = merged_dataframe.loc[:,'zone'].values
    X_train, X_test, y_train, y_test = train_test_split(X,y,random_state=0)

    model,train_accuracy,test_accuracy = dt.decision_tree_classifier(X_train,X_test,y_train,y_test)

    dt_string = datetime.now().strftime("%m-%d-%Y-%H-%M-%S")
    pkl_filename = "model"+"_"+dt_string+".pkl"
    with open("/home/ec2-user/airflow/ml_models/"+pkl_filename, 'wb') as file:
        pickle.dump(model, file)
    print("model saved at {}".format("/home/ec2-user/airflow/ml_models/"+pkl_filename))


# defining dags

default_args = {
  'start_date': datetime(2020,6,11),
  'retries': 3,
}

etl_dag = DAG("population_unemployment_etl_pipeline",
          default_args = default_args,
          schedule_interval=None)


etl_task = PythonOperator(task_id="etl_task",
                          python_callable=etl,
                          dag = etl_dag)

run_model = PythonOperator(task_id="run_ML_model",
                           python_callable=runModel,
                           dag=etl_dag
                           )

'''email_task = EmailOperator(
    to='user@example.com',
    task_id='email_task',
    subject='airflow update',
    html_content="task completed model saved at /home/ec2-user/airflow/ml_models",
    dag=etl_dag)'''
                           
etl_task >> run_model 
