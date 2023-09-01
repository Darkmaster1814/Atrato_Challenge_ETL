#Define the start datefrom 
from datetime import datetime;

#import the DAG object
from airflow.models import DAG;

#pandas and numpy just in case
import pandas as pd
import numpy as np

#Decorator for use SQL and python in Python context
from astro import sql as aql
from astro.files import File #Import files in DAG
from astro.sql.table import Table# Import Tables

#VARIABLE TO dataset name of tables an files
SNOWFLAKE_CONN_ID="snowflake_default"
SNOWFLAKE_CUSTOMER="customer"
SNOWFLAKE_TRANSACTION="transaction"
SNOWFLAKE_GROUPED_TRANS="transaction_filtered"
SNOWFLAKE_JOINED="joined_table"
SNOWFLAKE_ALERT="alert"

#Funtions transformations for DAG
@aql.transform
def group_trans(input_table: Table):
    return "SELECT Cid,TransDate,Commerce,SUM(Amount) AS TOTALAMOUNT FROM {{input_table}} GROUP BY Cid, TransDate, Commerce"

@aql.transform
def join_customer_trans(group_trans_table:Table,customer:Table):
    return"SELECT A.Cid, A.Name, A.Age,B.Commerce, B.TransDate, (1-(A.Credit-B.TOTALAMOUNT)/A.Credit)*100 AS CREDITUSED FROM {{customer}} A JOIN {{group_trans_table}} B ON A.Cid=B.Cid GROUP BY A.Cid, A.Name, A.age,B.Commerce, B.TransDate, A.Credit,B.TOTALAMOUNT"

@aql.transform
def alert_table(join_table:Table):
    return "SELECT Cid,Name,Age,Commerce,TransDate,CreditUsed FROM {{join_table}} WHERE CreditUsed>=85"

#Defining the DAG with name of DAG start time and if it is scheduled daily
with DAG(dag_id='atrato_fraud',start_date=datetime(2023,1,1),schedule='@daily',catchup=False):
    #load the dataset in a table 
    
    #Connect the tables in Snowflake
    transaction=Table(
        name=SNOWFLAKE_TRANSACTION,
        conn_id=SNOWFLAKE_CONN_ID
    )
    customer=Table(
        name=SNOWFLAKE_CUSTOMER,
        conn_id=SNOWFLAKE_CONN_ID
    )
    alert=Table(
        name=SNOWFLAKE_ALERT,
        conn_id=SNOWFLAKE_CONN_ID
    )
    
    #Transformations
    load_data=group_trans(transaction)
    joined_data=join_customer_trans(load_data,customer)
    alert_data=alert_table(joined_data)
    