from datetime import datetime, date, timedelta
import os
import pandas as pd

import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


BUCKET_NAME = "test_airflow_infomanager"
PROJECT_ID = "iungo-infomanager"
DATASET = "tickers_ds"
TABLE = "tickers"
TICKERS_LIST = "tickers_list.csv"
TICKERS_LIST_FILE_PATH = os.getcwd() + os.sep + "dags" + os.sep + "tickers_exercise" + os.sep + TICKERS_LIST
SECRET_FILE = os.getcwd() + os.sep + "dags" + os.sep + "credentials" + os.sep + "iungo-infomanager.json"

                                            # daily
@dag(dag_id='ab_tickers', schedule_interval='@once', start_date=datetime(2022, 1, 1), tags=['iungo', 'gcs', 'bucket', 'tickers'], catchup=False)
def taskflow():

    @task
    def build_bq_file(filepath: str = TICKERS_LIST_FILE_PATH, start=(datetime.now() - timedelta(1)).strftime('%Y-%m-%d'), end=date.today()):
        from pandas_datareader import data as pdr
        import yfinance as yf

        # to fill dataset
        start = "2017-01-01"
        end = "2021-04-09"

        tickers = list(pd.read_csv(filepath, sep=";"))
        
        dataset_bq = pd.DataFrame()
        for ticker in tickers:
            yf.pdr_override()
            data = (pdr.get_data_yahoo(ticker, start=start, end=end))
            
            if not data.empty:
                data["Ticker"] = ticker
                dataset_bq = pd.concat([dataset_bq, data])
        
        if not dataset_bq.empty:
            dataset_bq.columns = dataset_bq.columns.str.replace(' ', '_')
            dataset_bq.reset_index(inplace=True)
        
        return dataset_bq.to_json()
        
        
    @task
    def to_bq(df: pd.DataFrame):
        credential = Credentials.from_service_account_file(SECRET_FILE)
        df = pd.read_json(df)

        df.to_gbq(f"{DATASET}.{TABLE}", if_exists='append', progress_bar=True, credentials=credential)

    # Uncomment for deleting duplicates
    delete_duplicate = BigQueryInsertJobOperator(
        task_id="delete_duplicate",
        configuration={
            "query": {
                "query": f"CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.{TABLE}`"
                        "AS "
                        f"SELECT DISTINCT * FROM `{PROJECT_ID}.{DATASET}.{TABLE}`",
                "useLegacySql": False,
            }
        },
    )
    
    to_bq(build_bq_file()) >> delete_duplicate

    
graph = taskflow()
