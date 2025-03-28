from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# Definición del DAG con decorador
@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["ejemplo", "decoradores"],
)
def ejemplo_dag_decoradores():
    # Tarea 1: Usando decorador @task (Python)
    @task
    def extraer_datos():
        import pandas as pd
        datos = {"id": [1, 2], "valor": [10, 20]}
        df = pd.DataFrame(datos)
        print("Datos extraídos:")
        print(df)
        return df.to_json()

    # Tarea 2: Decorador + parámetros
    @task(retries=2)
    def transformar_datos(datos_json: str):
        import pandas as pd
        df = pd.read_json(datos_json)
        df["valor_transformado"] = df["valor"] * 2
        print("Datos transformados:")
        print(df)
        return df.to_json()

    # Tarea 3: Operador tradicional (Bash)
    cargar = BashOperator(
        task_id="cargar_datos",
        bash_command='echo "Cargando datos: {{ ti.xcom_pull(task_ids=\'transformar_datos\') }}"',
    )

    # Flujo definido con dependencias automáticas
    datos_extraidos = extraer_datos()
    datos_transformados = transformar_datos(datos_extraidos)
    datos_transformados >> cargar

# Genera el DAG decoradores
dag_generado = ejemplo_dag_decoradores()