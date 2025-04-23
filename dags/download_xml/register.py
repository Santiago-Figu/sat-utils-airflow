# Python Standard Library
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

# Dependencias externas
from sqlalchemy.exc import SQLAlchemyError

# Airflow Core
from airflow.models import DagModel
from airflow import settings

# Módulos personalizados
from config.log.logger import Logger as LogConfig
# from dags.download_xml.task import create_sat_download_dag, load_config
# from dags.download_xml.task_without_decorators import create_sat_download_dag, load_config
from dags.download_xml.task_retries import create_sat_download_dag, load_config

logger = LogConfig(file_name="download_xml").get_logger()

logger.debug(f"Archivo DAG cargado: {__file__} a las {datetime.now()}")


def register_dags_dynamic():
    """
    Regista los DAGS a Nivel de Base de datos para evitar duplicidad de Ejecuciones
    1. Nota: Si se registra un Schedule y se ejecuta manualmente sin cumplir el Schedule, puede generar una ejecución doble del DAG,
    dado a que ejecuta la manual y la del Schedule, tener cuidado
    """
    logger.debug("Iniciando registro de DAGs")
    
    try:
        # función contenida en cada DAG para obtener su configuración asociada
        configs = load_config()

        # Para realizar Mockeado en pruebas de pytest
        if hasattr(settings, 'Session') and not isinstance(settings.Session, MagicMock):
            # 1. Obtener los DAGs existentes en la base de datos
            session = settings.Session()
            existing_dags = {dag.dag_id for dag in session.query(DagModel.dag_id).all()}
        else:
            session = None
            existing_dags = set()

        registered_dags = []

        for config in configs:
            if config.get("type") == "sat_download":
                for cliente in config["sat_rfcs"]:
                    dag_id = f"{config['dag_id']}_{cliente['rfc']}"
                    tags = ["Descarga Masiva", cliente["rfc"]]
                    
                    # 2. Crear instancia del DAG con configuración personalizada desde el archivo config
                    dag_instance = create_sat_download_dag(config = config, dag_id = dag_id, info_cliente = cliente, tags= tags)
                    
                    # 3. Registrar Instancia del DAG en globals (requerido por Airflow)
                    globals()[dag_id] = dag_instance
                    registered_dags.append(dag_id)
                    
                    # 4. Solo registrar en DB si no existe para evitar duplicados
                    if  session and dag_id not in existing_dags:
                        try:
                            # 5. Crear entrada en DagModel si es nuevo
                            dag_model = DagModel(
                                dag_id=dag_id,
                                is_active=True,
                                fileloc=__file__
                            )
                            session.add(dag_model)
                            session.commit()
                            logger.info(f"Nuevo DAG registrado en DB: {dag_id}")
                        except SQLAlchemyError as e:
                            session.rollback()
                            logger.error(f"Error al registrar DAG en DB: {str(e)}")
                    else:
                        logger.debug(f"DAG existente cargado: {dag_id}")

        logger.debug(f"Proceso completado. DAGs registrados: {', '.join(registered_dags)}")

    except Exception as e:
        logger.error(f"Error crítico durante el registro: {str(e)}")
        if session:
            session.rollback()
        raise
    finally:
        if session:
            session.close()
            logger.debug("Sesión de base de datos cerrada")

register_dags_dynamic()

# Variable.delete("mi_dag_manual_executed")