from datetime import datetime, timedelta
import json
import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from typing import Dict, Any
from pathlib import Path

from sat_utils.sat_downloader import SATDownloader

# Configuración de rutas usando pathlib (más robusto)
PATH_CONFIG = Path(__file__).parent.parent.parent / "config" / "download_xml" / "dags_config.json"
SAT_CREDENTIALS_DIR = Path(__file__).parent.parent / "temp" / "sat_credentials"
XML_OUTPUT_DIR = Path(__file__).parent.parent / "temp" / "xml"

# Cargar configuración
with open(PATH_CONFIG) as f:
    configs = json.load(f)

def create_sat_download_dag(config,id,rfc:str, tags:list = ["Descarga Masiva"]):
    """Función que define y retorna un DAG (sin registrarlo aún)"""
    @dag(
        dag_id=id,
        schedule=config["schedule"],
        start_date=datetime(2023, 1, 1),
        tags=tags,
        catchup=False  # Evita ejecuciones históricas
    )
    def dag_template():
        @task
        def log_sat(rfc:str):
            sat_downloader = SATDownloader(
                rfc = rfc,
                cert_file = f'{rfc}.cer',
                key_file = f'{rfc}.key',
                password_file = f'password.txt',
            )
            data_login_sat = sat_downloader._login_sat()
            return data_login_sat

        @task
        def generar_key(data):
            print(f"Token: {data['AutenticaResult']}")
            print(f"Válido desde: {data['Created']}")
            print(f"Válido hasta: {data['Expires']}")

        data_login_sat = log_sat(rfc)
        data_login_sat >> generar_key(data_login_sat)

    return dag_template()

# Registrar DAGs dinámicamente
for config in configs:
    if config.get("type") == "sat_download":
        for cliente in config["sat_rfcs"]:
            tags=["Descarga Masiva"]
            tags.append(cliente["rfc"])
            dag_id = str(config["dag_id"])+"_"+str(cliente["rfc"])
            rfc = cliente["rfc"]
            globals()[dag_id] = create_sat_download_dag(config,dag_id,rfc,tags)