from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG
# Python Standard Library
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import Dict, Any

# Dependencias externas
import requests

# Airflow Core
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email
from airflow.utils.state import State

# Módulos personalizados
from sat_utils.sat_downloader import SATDownloader
from config.log.logger import Logger as LogConfig

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

logger = LogConfig(file_name="download_xml").get_logger()

def get_dag_logger(dag_id):
    return LogConfig(file_name=f"download_xml_{dag_id}").get_logger()

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



def create_sat_download_dag(config, dag_id, info_cliente: dict, tags: list = ["Descarga Masiva"]):
    """Función que define y retorna un DAG simplificado"""
    logger_dag = get_dag_logger(dag_id)
    
    @dag(
        dag_id=dag_id,
        schedule=config["schedule"],
        start_date=datetime(2023, 1, 1),
        tags=tags,
        catchup=False,
        default_args={
            'owner': 'airflow',
            'depends_on_past': True,
            'retries': 0,
            'retry_delay': timedelta(minutes=1),
            'retry_exponential_backoff': True,
            'max_retry_delay': timedelta(minutes=5),
            'execution_timeout': timedelta(minutes=10),
            'wait_for_downstream': True,
        },
        max_active_runs=1,
        max_active_tasks=1,
        is_paused_upon_creation=True
    )
    def generated_dag():
        circuit_breaker_active = False

        # 1. Definición de tareas (manteniendo tus funciones originales)
        @task(task_id='execution_controller')
        def execution_controller():
            # ... (tu implementación actual)
            pass

        @task(task_id='verify_single_execution')
        def check_single_execution():
            # ... (tu implementación actual)
            pass

        @task(retries=10, retry_delay=timedelta(seconds=30))
        def log_sat(rfc: str, reset_circuit: bool = False):
            nonlocal circuit_breaker_active

            if reset_circuit:
                circuit_breaker_active = False
            
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
                data_login_sat = sat_downloader._login_sat()
                if not validate_token_response(data_login_sat):
                    logger_dag.info(f"data:{data_login_sat}")
                    raise AirflowException("Token generado no es válido")
                    
                # Resetear circuit breaker si estaba activo
                circuit_breaker_active = False
                return data_login_sat
                
            except requests.exceptions.Timeout:
                # Todo: Modificar SATDownloader para soportar timeout
                raise AirflowException("Timeout al conectar con el SAT")
            except requests.exceptions.ConnectionError as e:
                # Activar circuit breaker por error de conexión
                circuit_breaker_active = True
                raise AirflowException(f"Error de conexión con SAT - Circuit Breaker activado {e}")
            except Exception as e:
                raise AirflowException(f"Error al generar token: {str(e)}")

        @task
        def generar_key(data: Dict[str, Any], rfc: str):
            # ... (tu implementación actual)
            pass

        @task(retries=0)
        def cfdi_request(data: Dict[str, Any], info_cliente: Dict):
            logger_dag.info(f"Realizando petición al SAT para descarga de CFDI de {info_cliente['rfc']}")
            sat_downloader = SATDownloader(
                rfc=str(info_cliente["rfc"]),
                cert_file=f'{info_cliente["rfc"]}.cer',
                key_file=f'{info_cliente["rfc"]}.key',
                password_file=f'password.txt',
            )
            try:
                id_solicitud = sat_downloader._solicitar_descarga(token_auth=data['AutenticaResult'], FechaInicial=info_cliente["fecha_inicial"], FechaFinal = info_cliente["fecha_inicial"], TipoSolicitud= info_cliente["tipo_solicitud"])
                logger_dag.info(f"id_solicitud obtenido: {id_solicitud}")
                return id_solicitud
            except Exception as e:
                logger_dag.error(f'Error al realizar la solicitud de descarga: {str(e)}')
                raise AirflowException(f"Error en cfdi_request: {str(e)}")

        @task(retries=0, trigger_rule=TriggerRule.ALL_SUCCESS)
        def validate_request_and_token(id_solicitud: str, data_login_sat: Dict[str, Any], rfc: str):
            context = get_current_context()
            task_instance = context['task_instance']
            
            # Verificar expiración del token
            current_time = datetime.now()
            expires_time = data_login_sat['Expires']
            token_expired = current_time >= expires_time
            
            logger_dag.info(f"Validando id_solicitud: {id_solicitud}")
            logger_dag.info(f"Token expira en: {expires_time} (ahora: {current_time})")
            
            # Si el token está expirado o a punto de expirar (menos de 1 minuto de vida)
            if token_expired or (expires_time - current_time) < timedelta(minutes=1):
                logger_dag.warning("Token SAT expirado o cerca de expirar, se requiere nuevo login")
                # Forzar reintento de log_sat lanzando una excepción
                raise AirflowException("Token expirado - Se requiere nuevo login")
            
            # Si no tenemos id_solicitud
            if id_solicitud is None:
                logger_dag.warning("No se obtuvo id_solicitud, reintentando cfdi_request")
                # Forzar reintento de cfdi_request lanzando una excepción
                raise AirflowException("id_solicitud no obtenido - Reintentando cfdi_request")
            
            # Si todo está bien
            logger_dag.info("Validación exitosa, continuando con el flujo")
            return {
                "id_solicitud": id_solicitud,
                "token_status": "valid",
                "token_expires": expires_time
            }

        @task
        def check_cfdi_request(data: Dict[str, Any], info_cliente: Dict, id_solicitud: str):
            logger_dag.info(f"Realizando proceso de revisión de petición {id_solicitud}")

        @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
        def handle_failure():
            # Obtener el contexto de ejecución
            context = get_current_context()
            
            # Extraer información del task que fallo
            task_instance = context['task_instance']
            exception = context.get('exception', "Error desconocido")
            dag_run = context['dag_run']
            
            logger_dag.warning(f"Falló la tarea {task_instance.task_id}")
            logger_dag.warning(f"Intento {task_instance.try_number} de {task_instance.max_tries}")
            logger_dag.warning(f"Error: {str(exception)}")
            
            if task_instance.try_number >= task_instance.max_tries:
                logger_dag.warning("Pendiente de enviar correo de notificación")
            return "failure_handled"

        # 2. Pipeline principal simplificado
        @task
        def main_workflow(info_cliente: dict):
            """Orquesta el flujo principal de trabajo"""
            # Control de ejecución
            controller_result = execution_controller()
            verification_result = check_single_execution()
            
            # Autenticación y solicitud
            auth_data = log_sat(info_cliente["rfc"])
            token_info = generar_key(auth_data, info_cliente["rfc"])
            request_id = cfdi_request(auth_data, info_cliente)
            
            # Validación
            validation = validate_request_and_token(request_id, auth_data, info_cliente["rfc"])
            
            # Verificación final
            check_result = check_cfdi_request(auth_data, info_cliente, request_id)
            
            return check_result

        # 3. Ejecutar el pipeline
        workflow_result = main_workflow(info_cliente)
        
        # 4. Configurar manejo de errores
        @task
        def monitor_errors():
            """Monitorea el estado del workflow"""
            context = get_current_context()
            ti = context['task_instance']
            
            # Verificar si alguna tarea falló
            if ti.xcom_pull(task_ids='main_workflow') is None:
                handle_failure()
                return "error_handled"
            return "workflow_completed_successfully"

        # 5. Establecer dependencias
        workflow_result >> monitor_errors()

    return generated_dag()