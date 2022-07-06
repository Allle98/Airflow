import os
from socket import timeout
import pandas as pd
import pandas_gbq
import datetime
import pendulum
import json

from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator


default_args = {
    "owner": "ab",
    "start_date": pendulum.datetime(2022, 5, 2),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=60),
    "email": ["test@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}


WORKING_DIRECTORY = os.getcwd() + os.sep + "dags" + os.sep + "sample1" + os.sep
INPUT_FILE =  WORKING_DIRECTORY + "input.json"


PROJECT_ID = "iungo-infomanager"
DATASET = "sample1"
TABLE = "DB_tot"

SECRET_FILE = os.getcwd() + os.sep + "dags" + os.sep + "credentials" + os.sep + "iungo-infomanager.json"

@dag(dag_id="sample1_pipeline", schedule_interval="@once", default_args=default_args, catchup=False, tags=['iungo', 'sample'])
def taskflow():

   
    def read_input(file_path: str = INPUT_FILE):
        with open(file_path) as f:
            d = json.load(f)
            print(d)
            piva = d["piva"]
            anni = d["anni"]

            return piva, anni

    
    @task
    def read_bg(piva, anni):
        credential = Credentials.from_service_account_file(SECRET_FILE)
        
        sql = f"""SELECT * 
                  FROM {DATASET}.{TABLE}
                  WHERE anno IN {tuple(anni)} AND partita_iva_ IN {tuple(piva)}"""

        df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID, credentials=credential)

        df.columns = [
            "anno",
            "ID",
            "Ragione Sociale",
            "Ricavi delle vendite",
            "CREDITI VERSO SOCI",
            "TOTALE IMMOB. IMMATERIALI",
            "TOTALE IMMOB. MATERIALI",
            "Terreni e fabbricati",
            "TOTALE IMMOB. FINANZIARIE",
            "Partecipazioni e titoli immobilizzati - IP",
            "TOT CREDITI a breve tra immob. finanz. - IP",
            "Azioni proprie",
            "TOTALE RIMANENZE",
            "TOTALE CREDITI",
            "Clienti totale - IP",
            "Cred. vs Clienti oltre",
            "Crediti oltre (in attivo circolante) - IP",
            "TOTALE ATTIVITA' FINANZIARIE",
            "TOT. DISPON. LIQUIDE",
            "Totale Attività",
            "Patrimonio Netto",
            "Capitale sociale",
            "TOTALE FONDI RISCHI",
            "TRATTAMENTO DI FINE RAPPORTO",
            "TOTALE DEBITI",
            "Total debiti oltre l'esercizio",
            "Banche entro l'esercizio",
            "Banche oltre l'esercizio",
            "debiti finanziari a breve - IP",
            "debiti finanziari oltre - IP",
            "Fornitori e acconti totale - IP",
            "Fornitori e acconti oltre - IP",
            "RATEI E RISCONTI",
            "TOT. VAL. DELLA PRODUZIONE",
            "Altri ricavi",
            "Materie prime e consumo",
            "Servizi",
            "Godimento beni di terzi",
            "Totale costi del personale",
            "TOT Ammortamenti e svalut",
            "Variazione materie",
            "Accantonamenti per rischi",
            "Altri accantonamenti",
            "Oneri diversi di gestione",
            "PROVENTI FINANZIARI - IP",
            "TOT Oneri finanziari",
            "Utili e perdite su cambi",
            "TOTALE RETTIFICHE ATT. FINANZ",
            "TOTALE PROVENTI/ONERI STRAORDINARI",
            "Totale Imposte sul reddito correnti, differite e anticipate",
            "UTILE/PERDITA DI ESERCIZIO di pert. di TERZI",
            "UTILE/PERDITA DI ESERCIZIO di pert. del GRUPPO",
            "Dipendenti",
            "ATECO 2007 Codice",
            "Indirizzo",
            "Codice Postale",
            "Comune",
            "Provincia",
            "Regione",
            "Numero CCIAA",
            "Codice Fiscale",
            "Partita IVA",
            "CREDITI A OLTRE",
            "DEBITI A OLTRE",
            "Testo nota",
            "ATECO 2007 Descrizione",
            "Immobilizzazioni materiali destinate alla vendita",
        ]

        dict_anni = {}
        for anno in anni:
            dfx = (df.loc[df['anno'] == anno])
            dfx.reset_index(inplace=True)
            print(dfx)

            dict_anni[anno] = dfx.to_json()
        
        #print(dict_anni)

        return dict_anni

    @task
    def display_dataframe(dict_anni):
        for k in dict_anni:
            print(pd.read_json(dict_anni[k])["anno"])

    # @task
    # def build_dict(piva, anni):
    #     credential = Credentials.from_service_account_file(SECRET_FILE)

    #     df_dict()
    #     for anno in anni:
    #         sql = f"""SELECT * 
    #               FROM {DATASET}.{TABLE}
    #               WHERE anno {anno} AND partita_iva_ IN {tuple(piva)}"""

    #         df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID, credentials=credential)


    piva, anni = read_input()
    display_dataframe(read_bg(piva, anni))

# =================== DAG =======================

pipeline = taskflow()

# =================== END =======================
