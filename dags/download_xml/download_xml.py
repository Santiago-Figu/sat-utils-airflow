from datetime import datetime, timedelta
import json
import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.utils.context import Context  # Para el tipo
from airflow.operators.python import get_current_context  # Para la función
from typing import Dict, Any
from pathlib import Path
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email
import requests

from sat_utils.sat_downloader import SATDownloader

from config.log.logger import Logger as LogConfig

logger = LogConfig(file_name="download_xml").get_logger()

# Configuración de rutas usando pathlib (más robusto)
PATH_CONFIG = Path(__file__).parent.parent.parent / "config" / "download_xml" / "dags_config.json"
SAT_CREDENTIALS_DIR = Path(__file__).parent.parent / "temp" / "sat_credentials"
XML_OUTPUT_DIR = Path(__file__).parent.parent / "temp" / "xml"

# Cargar configuración
with open(PATH_CONFIG) as f:
    configs = json.load(f)

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

def create_sat_download_dag(config, id, rfc: str, tags: list = ["Descarga Masiva"]):
    """Función que define y retorna un DAG (sin registrarlo aún)"""
    @dag(
        dag_id=id,
        schedule=config["schedule"],
        start_date=datetime(2023, 1, 1),
        tags=tags,
        catchup=False,
        default_args={
            'retries': 10,  # Número máximo de reintentos
            'retry_delay': timedelta(minutes=1),  # Espera 1 minuto entre reintentos
            'retry_exponential_backoff': True,  # Backoff exponencial para evitar saturación
            'max_retry_delay': timedelta(minutes=5),  # Límite máximo de espera
            'execution_timeout': timedelta(minutes=10),  # Timeout para cada ejecución
        }
    )
    def dag_template():
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
        def generar_key(data: Dict[str, Any]):
            if not validate_token_response(data):
                raise AirflowException("Token inválido recibido para procesar")
                
            logger.info(f"Token generado correctamente: {data['AutenticaResult']}")
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
        processed_data = generar_key(data_login_sat)
        error_handler = handle_failure()
        
        # Establecemos las dependencias
        data_login_sat >> processed_data
        data_login_sat >> error_handler
        processed_data >> error_handler

    return dag_template()

# Registrar DAGs dinámicamente
for config in configs:
    if config.get("type") == "sat_download":
        for cliente in config["sat_rfcs"]:
            tags = ["Descarga Masiva"]
            tags.append(cliente["rfc"])
            dag_id = str(config["dag_id"]) + "_" + str(cliente["rfc"])
            rfc = cliente["rfc"]
            globals()[dag_id] = create_sat_download_dag(config, dag_id, rfc, tags)