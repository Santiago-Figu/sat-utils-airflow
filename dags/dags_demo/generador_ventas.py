from datetime import datetime
import json
import os
from airflow.decorators import dag, task

from sat_utils.test import get_claves

path_config = os.path.join(os.getcwd(), "config", "demo", "dags_config.json")
with open(path_config) as f:
    configs = json.load(f)

def create_dag(config):
    """Función que define y retorna un DAG (sin registrarlo aún)"""
    @dag(
        dag_id=config["dag_id"],
        schedule=config["schedule"],
        start_date=datetime(2023, 1, 1),
        catchup=False  # Evita ejecuciones históricas
    )
    def dag_template():
        @task
        def calcular_ventas(region: str, umbral: int):
            return f"Ventas en {region}: ${umbral * 1.1}"

        @task
        def generar_key(**kwargs):
            get_claves()

        ventas = calcular_ventas(**config["parametros"])
        ventas >> generar_key()

    return dag_template()

# Registra los DAGs una sola vez (fuera del bucle)
for config in configs:
    globals()[config["dag_id"]] = create_dag(config)