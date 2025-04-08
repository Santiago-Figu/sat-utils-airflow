from datetime import datetime, timedelta
import json
import os
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context  # Para la función
from typing import Dict, Any
from pathlib import Path
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagModel
from airflow import settings
from airflow.utils.email import send_email
import requests
from sqlalchemy.exc import SQLAlchemyError

from sat_utils.sat_downloader import SATDownloader

from config.log.logger import Logger as LogConfig

logger = LogConfig(file_name="download_xml").get_logger()

logger.debug(f"Archivo DAG cargado: {__file__} a las {datetime.now()}")

# Configuración de rutas usando pathlib
SAT_CREDENTIALS_DIR = Path(__file__).parent.parent / "temp" / "sat_credentials"
XML_OUTPUT_DIR = Path(__file__).parent.parent / "temp" / "xml"

# Cargar configuración
# ToDo: moverlo a utils y que reciba el path a cargar, posteriormente esto sera remplazado por la lectura de bucket o base de datos
def load_config():
    config_path = Path(__file__).parent.parent.parent / "config" / "download_xml" / "dags_config.json"
    with open(config_path) as f:
        return json.load(f)
    
configs = load_config()

def validate_token_response(response: Dict[str, Any]) -> bool:
    """Valida que la respuesta del token SAT sea correcta
    
    Args:
        response: Diccionario con la respuesta del servicio de autenticación del SAT
            Debe contener:
            - 'AutenticaResult' (str): Token JWT
            - 'Created' (datetime): Fecha-hora de creación
            - 'Expires' (datetime): Fecha-hora de expiración
    
    Returns:
        bool: True si la respuesta es válida, False en caso contrario
    """
    # 1. Validar campos obligatorios
    required_fields = ['AutenticaResult', 'Created', 'Expires']
    if not all(field in response for field in required_fields):
        return False
    
    # 2. Validar tipo y contenido del token
    autentica_result = response['AutenticaResult']
    if not autentica_result or not isinstance(autentica_result, str):
        return False
    
    # Verificar estructura básica token JWT
    jwt_parts = autentica_result.split('.')
    if len(jwt_parts) != 3:
        return False
    
    # 3. Validar formato de fechas datetime
    created = response['Created']
    expires = response['Expires']
    
    if not isinstance(created, datetime) or not isinstance(expires, datetime):
        return False
    
    # 4. Validar que la fecha de expiración sea posterior a la de creación
    if expires <= created:
        return False
    
    # 5. Validar que el token no esté expirado
    # current_time = datetime.now()
    # if expires < current_time:
    #     return False
    
    return True

def send_failure_notification(context, rfc):
    """Envía notificación por email (igual que tu lógica original)"""
    try:
        subject = f"Fallo en generación de token SAT para {rfc}"
        html_content = f"""
        <h1>Fallo en generación de token SAT</h1>
        <p>RFC: {rfc}</p>
        <p>Error: {str(context.get('exception'))}</p>
        <p>Intento: {context['task_instance'].try_number}/10</p>
        """
        
        send_email(
            to=["tu_equipo@example.com"],
            subject=subject,
            html_content=html_content
        )
    except Exception as email_error:
        logger.error(f"Error al enviar correo: {email_error}")

def create_sat_download_dag(config, dag_id, rfc: str, tags: list = ["Descarga Masiva"]):
    """Función que define y retorna un DAG"""
    @dag(
        dag_id=dag_id,
        schedule=config["schedule"],
        start_date=datetime(2023, 1, 1),
        tags=tags,
        catchup=False,
        default_args={
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 10,
            'retry_delay': timedelta(minutes=1),
            'retry_exponential_backoff': True,
            'max_retry_delay': timedelta(minutes=5),
            'execution_timeout': timedelta(minutes=10),
        }
    )
    def generated_dag():
        # Variable para implementar Circuit Breaker pattern
        circuit_breaker_active = False
        
        @task(retries=10,retry_delay=timedelta(seconds=30))  # Desactivamos reintentos en la task para manejar manualmente
        def log_sat(rfc: str):
            nonlocal circuit_breaker_active
            
            # Verificar si el circuit breaker está activo
            if circuit_breaker_active:
                raise AirflowException("Circuit Breaker activo - Servicio SAT no disponible")
                
            sat_downloader = SATDownloader(
                rfc=rfc,
                cert_file=f'{rfc}.cer',
                key_file=f'{rfc}.key',
                password_file=f'password.txt',
            )
            
            try:
                # Agregamos timeout a la conexión (10 segundos conexión, 30 segundos lectura)
                data_login_sat = sat_downloader._login_sat()
                
                if not validate_token_response(data_login_sat):
                    logger.info(f"data:{data_login_sat}")
                    raise AirflowException("Token generado no es válido")
                    
                # Si llegamos aquí, resetear circuit breaker si estaba activo
                circuit_breaker_active = False
                return data_login_sat
                
            except requests.exceptions.Timeout:
                # Nota: Deberías modificar SATDownloader para soportar timeout
                raise AirflowException("Timeout al conectar con el SAT")
            except requests.exceptions.ConnectionError as e:
                # Activar circuit breaker por error de conexión
                circuit_breaker_active = True
                raise AirflowException(f"Error de conexión con SAT - Circuit Breaker activado {e}")
            except Exception as e:
                raise AirflowException(f"Error al generar token: {str(e)}")

        @task
        def generar_key(data: Dict[str, Any], rfc:str):
            if not validate_token_response(data):
                raise AirflowException("Token inválido recibido para procesar")
                
            logger.info(f"Token generado correctamente para {rfc}: {data['AutenticaResult']}")
            logger.info(f"Válido desde: {data['Created']}")
            logger.info(f"Válido hasta: {data['Expires']}")
            
            # Métrica para monitoreo (podrías enviar esto a Prometheus, Datadog, etc.)
            logger.info(f"METRIC: token_generation_success timestamp={datetime.now().isoformat()}")
            
            return {
                "status": "success",
                "token": data['AutenticaResult'],
                "valid_from": data['Created'],
                "valid_to": data['Expires']
            }

        @task(
            trigger_rule=TriggerRule.ONE_FAILED,
            retries=0
        )
        def handle_failure():
            # Obtener el contexto de ejecución
            context = get_current_context()
            
            # Extraer información importante
            task_instance = context['task_instance']
            exception = context.get('exception', "Error desconocido")
            dag_run = context['dag_run']
            
            logger.warning(f"Falló la tarea {task_instance.task_id}")
            logger.warning(f"Intento {task_instance.try_number} de {task_instance.max_tries}")
            logger.warning(f"Error: {str(exception)}")
            
            
            if task_instance.try_number >= task_instance.max_tries:
                # send_failure_notification(context, rfc)
                logger.warning("Pendiente de enviar correo")
            
            return "failure_handled"

       

        # Configuración del flujo con manejo de errores
        data_login_sat = log_sat(rfc)
        processed_data = generar_key(data_login_sat, rfc)
        error_handler = handle_failure()
        
        # Establecemos las dependencias
        data_login_sat >> processed_data
        data_login_sat >> error_handler
        processed_data >> error_handler

    return generated_dag()

def register_dags_safely():
    """Registra DAGs verificando contra la base de datos para evitar duplicidad"""
    logger.debug("Iniciando registro seguro de DAGs")
    
    try:
        configs = load_config()
        session = settings.Session()
        
        # 1. Obtener DAGs existentes en la base de datos
        existing_dags = {dag.dag_id for dag in session.query(DagModel.dag_id).all()}
        
        # 2. Registrar DAGs en globals() siempre, pero solo crear en DB si no existen
        registered_dags = []
        
        for config in configs:
            if config.get("type") == "sat_download":
                for cliente in config["sat_rfcs"]:
                    dag_id = f"{config['dag_id']}_{cliente['rfc']}"
                    tags = ["Descarga Masiva", cliente["rfc"]]
                    
                    # Siempre crear el objeto DAG en globals()
                    dag_instance = create_sat_download_dag(
                        config, 
                        dag_id, 
                        cliente["rfc"], 
                        tags
                    )
                    
                    if dag_id not in globals():
                        logger.debug(f"registrando dag {dag_id} en globals")
                        globals()[dag_id] = dag_instance
                    
                    if dag_id not in existing_dags:
                        logger.info(f"Nuevo DAG creado: {dag_id}")
                    else:
                        logger.debug(f"DAG existente cargado: {dag_id}")
        
        logger.debug(f"DAGs registrados correctamente: {', '.join(registered_dags)}")
        
    except SQLAlchemyError as e:
        logger.error(f"Error de base de datos al registrar DAGs: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado al registrar DAGs: {str(e)}")
        raise
    finally:
        session.close()
        logger.debug("Sesión de base de datos cerrada")

# Registrar los DAGs
register_dags_safely()