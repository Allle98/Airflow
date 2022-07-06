import os
import sys
from socket import timeout
from typing_extensions import Self
import pandas as pd
import pandas_gbq
import datetime
import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from google.oauth2.service_account import Credentials


# ===============================================
default_args = {
                "owner": "AB",
                "start_date": pendulum.datetime(2022, 5, 2),
                "retries": 1,
                "retry_delay": datetime.timedelta(minutes=60),
                "email": ["test@gmail.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                'provide_context': True,
                }

# =================== VARIABLE =======================

PROJECT_ID = "iungo-infomanager"
DATASET = "sample1"
TABLE = "DB_tot"

SECRET_FILE = os.getcwd() + os.sep + "dags" + os.sep + "credentials" + os.sep + "iungo-infomanager.json"
# =================== DAGS =======================

# pipeline setup
@dag(
    dag_id="dag_to_trigger", 
    schedule_interval="@once",#'@daily',
    default_args=default_args,
    catchup=False,
    tags=['iungo', 'streamlit']
)
def main():
#with DAG( dag_id="dag_to_trigger", schedule_interval="@once", default_args=default_args, catchup=False, tags=['iungo', 'streamlit']) as dag:
 
    @task
    def read_piva(**kwargs):    
        piva = kwargs['dag_run'].conf.get('piva')
        print(piva)
        print(type(piva))
        return piva

   
    @task
    def read_bg(piva):
        credential = Credentials.from_service_account_file(SECRET_FILE)
        
        if  len(piva) > 1:
            sql = f"""SELECT * 
                    FROM {DATASET}.{TABLE}
                    WHERE partita_iva_ IN {tuple(piva)}"""
        else:
            sql = f"""SELECT * 
                FROM {DATASET}.{TABLE}
                WHERE partita_iva_ = {piva[0]}"""


        print(sql)
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

        print(df["Ragione Sociale"])
        
        return df.to_json()

    #read_piva = PythonOperator(task_id="read_piva", python_callable=read_piva)

    read_bg(read_piva())

# =================== DAG =======================

taskflow = main()

# =================== END =======================
