from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'tu_usuario',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    'ejemplo_flujo_datos',
    default_args=default_args,
    description='Un DAG de ejemplo para procesamiento de datos',
    schedule_interval=timedelta(days=1),  # Ejecución diaria
    start_date=days_ago(2),  # Comienza a ejecutarse desde 2 días atrás
    tags=['ejemplo', 'datos'],
) as dag:

    # Tarea inicial (inicio del flujo)
    inicio = EmptyOperator(task_id='inicio')

    # Tarea 1: Descargar datos (simulada)
    descargar = BashOperator(
        task_id='descargar_datos',
        bash_command='echo "Descargando datos de https://ejemplo.com/datos.csv..." && sleep 5'
    )

    # Tarea 2: Procesar datos (Python)
    def procesar_datos(**kwargs):
        import pandas as pd
        data = {'col1': [1, 2], 'col2': [3, 4]}
        df = pd.DataFrame(data)
        print(f"Datos procesados:\n{df}")
        return df.to_json()  # Retorno que puede ser usado por downstream tasks

    procesar = PythonOperator(
        task_id='procesar_datos',
        python_callable=procesar_datos,
        provide_context=True
    )

    # Tarea 3: Notificar (Branching)
    def decidir_notificacion(**kwargs):
        ti = kwargs['ti']
        datos_json = ti.xcom_pull(task_ids='procesar_datos')
        print(f"Datos recibidos del paso anterior: {datos_json}")
        return 'notificar_exito'  # Cambia esto para probar diferentes caminos

    decision = PythonOperator(
        task_id='decidir_notificacion',
        python_callable=decidir_notificacion,
        provide_context=True
    )

    # Tareas paralelas
    notificar_exito = BashOperator(
        task_id='notificar_exito',
        bash_command='echo "Proceso exitoso! Los datos están listos."'
    )

    notificar_error = BashOperator(
        task_id='notificar_error',
        bash_command='echo "Hubo un error en el procesamiento!"'
    )

    # Tarea final
    fin = EmptyOperator(task_id='fin')

    # Estructura del flujo
    inicio >> descargar >> procesar >> decision
    decision >> [notificar_exito, notificar_error] >> fin