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
from airflow.models import DagModel, DagRun, Variable
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
    """Función que define y retorna un DAG"""
    logger_dag = get_dag_logger(dag_id)
    
    @dag(
        dag_id=dag_id,
        schedule=config["schedule"],
        start_date=datetime(2024, 1, 1),
        # start_date=None,
        tags=tags,
        catchup=False,
        default_args={
            'owner': 'airflow',
            'depends_on_past': False, # evita ejecuciones paralelas
            'retries': 0,
            # 'retry_delay': timedelta(minutes=1),
            'retry_exponential_backoff': True,
            # 'max_retry_delay': timedelta(minutes=5),
            'execution_timeout': timedelta(minutes=10),
            'wait_for_downstream': False, # si se activa no permite más ejecuciones hasta que todas se completen no importa si son manuales o programadas
        },
        max_active_runs=1, # especifica solo una ejecución a la vez
        max_active_tasks=1, # Evita la superposición de ejecuciones
        is_paused_upon_creation=True #False Evita que se pause el dag al ser creado
    )
    def generated_dag():
        #v variable de prueba para el pytest, permite simular un error de conexión con el SAT
        circuit_breaker_active = False

        ###############################################################################################
        @task(task_id='execution_controller')
        def execution_controller():
            context = get_current_context()
            dag_run = context['dag_run']
            session = Session()
            
            try:
                # Verificar si es una ejecución manual
                if dag_run.run_type == 'manual':
                    # Activar el DAG para futuras ejecuciones programadas
                    dag_model = session.query(DagModel).filter(
                        DagModel.dag_id == dag_id
                    ).first()
                    dag_model.is_paused = False
                    session.commit()
                    
                    # Registro de ejecuciones manuales para controlar futuras ejecuciones programadas
                    Variable.set(key=f"{dag_id}_manual_priority", value=True)
                    return "manual_execution_priority"
                
                # Para ejecuciones programadas
                if not Variable.get(f"{dag_id}_manual_priority", default_var=False):
                    # Si es la primera ejecución programada verificar si hay una manual en progreso
                    manual_running = session.query(DagRun).filter(
                        DagRun.dag_id == dag_id,
                        DagRun.run_type == 'manual',
                        DagRun.state.in_([State.RUNNING, State.QUEUED])
                    ).count()
                    
                    if manual_running > 0:
                        # logger_dag.warning("Ejecución manual en progreso - Cancelando ejecución programada inicial")
                        raise AirflowException("Ejecución manual en progreso - Cancelando ejecución programada inicial")
                return "scheduled_execution_allowed"
            finally:
                session.close()
        ###############################################################################################
        @task(task_id='verify_single_execution')
        def check_single_execution():
            """Task de verificación de ejecuciones de DAGS duplicados"""
            context = get_current_context()
            dag_run = context['dag_run']
            # Obtener todas las ejecuciones activas
            active_runs = DagRun.find(
                dag_id=dag_id,
                state='running'
            )
            
            # Si hay más de una ejecución activa, genera un AirflowException
            if len(active_runs) > 1:
                current_run_id = dag_run.run_id
                other_runs = [r.run_id for r in active_runs if r.run_id != current_run_id]
                raise AirflowException(
                    f"Ejecución duplicada detectada. Ejecuciones activas: {', '.join(other_runs)}"
                )
            return "Sin Ejecuciones duplicadas, continuando flujo"
        ###############################################################################################
        @task(
            trigger_rule=TriggerRule.ONE_FAILED,
            retries=0
        )
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
        ###############################################################################################
        @task(task_id='log_sat', retries=0)
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
            
        ###############################################################################################
        @task(retries=0)
        def validate_token(data_login_sat: Dict[str, Any], rfc: str):
            """Tarea de validación con modo de prueba controlado"""
            context = get_current_context()
            current_time = datetime.now()
            expires_time = data_login_sat['Expires']
            token = data_login_sat['AutenticaResult']

            # Variables de prueba para probar el reintento por token expirado
            TEST_MODE = True  # Cambiar a False en producción
            FORCE_EXPIRE_FIRST_CHECK = True

            if TEST_MODE:
                # Registro del intento de validación para restablecer valores en la segunda ronda de validación y evitar ciclos infinitos
                task_instance = context['task_instance']
                validation_count = task_instance.xcom_pull(
                    key='validation_count',
                    default=0
                ) + 1
                task_instance.xcom_push(key='validation_count', value=validation_count)

                # generación de error por expiración en el primer check
                if FORCE_EXPIRE_FIRST_CHECK and validation_count == 1:
                    logger_dag.warning("MODO PRUEBA - Forzando token expirado (primer check)")
                    return {
                        "status": "expired",
                        "message": "Token expirado - PRUEBA",
                        "expires": current_time - timedelta(minutes=5),  # Fecha pasada
                        "token": token
                    }

            # flujo normal de validación
            if current_time >= expires_time or (expires_time - current_time) < timedelta(minutes=1):
                logger_dag.warning("Token expirado o cerca de expirar (validación real)")
                return {
                    "status": "expired",
                    "message": "Token requires refresh",
                    "expires": expires_time,
                    "token": token
                }

            return {
                "status": "valid",
                "message": "Token is valid",
                "expires": expires_time,
                "token": token
            }
        ###############################################################################################
        @task.branch(retries=0)
        def decide_token_refresh(validation_result: Dict[str, Any], rfc: str):
            """Task Branch para decidir si refrescar el token o continuar"""
            if validation_result['status'] == 'expired':
                logger_dag.warning("Token expirado - Decidiendo refrescar")
                return "log_sat"
            logger_dag.info("Token válido - Continuando flujo normal")
            return "cfdi_request"
        ###############################################################################################
        @task(retries=1, retry_delay=timedelta(seconds=10))
        def cfdi_request(validation_data: Dict[str, Any], info_cliente: Dict):
            """Versión modificada que solo recibe el token válido"""
            context = get_current_context()
            logger_dag.info(f"Realizando petición al SAT para descarga de CFDI de {info_cliente['rfc']}")
            
            sat_downloader = SATDownloader(
                rfc=str(info_cliente["rfc"]),
                cert_file=f'{info_cliente["rfc"]}.cer',
                key_file=f'{info_cliente["rfc"]}.key',
                password_file=f'password.txt',
            )
            
            try:
                id_solicitud = sat_downloader._solicitar_descarga(
                    token_auth=validation_data["token"],
                    FechaInicial=info_cliente["fecha_inicial"],
                    FechaFinal=info_cliente["fecha_inicial"],
                    TipoSolicitud=info_cliente["tipo_solicitud"]
                )
                
                # if not id_solicitud:
                #     raise AirflowException("id_solicitud no obtenido")
                
                logger_dag.info(f"id_solicitud obtenido: {id_solicitud}")
                return id_solicitud
                
            except Exception as e:
                logger_dag.error(f'Error al realizar la solicitud de descarga: {str(e)}')
                raise AirflowException(f"Error en cfdi_request: {str(e)}")
        ###############################################################################################
        @task(retries=0)
        def validate_request(id_solicitud: str, rfc: str):
            """Valida solo el id_solicitud (sin validación de token)"""
            if not id_solicitud:
                logger_dag.warning("No se obtuvo id_solicitud")
                raise AirflowException("id_solicitud no obtenido")
            
            logger_dag.info("Validación de solicitud exitosa")
            return id_solicitud
        ###############################################################################################
        @task(task_id='check_cfdi_request')
        def check_cfdi_request(info_cliente: Dict, id_solicitud: str):
            logger_dag.info(f"Realizando proceso de revisión de petición {id_solicitud}")
        ###############################################################################################

        # Flujo principal
        controller = execution_controller()
        verification = check_single_execution()
        initial_login = log_sat(info_cliente["rfc"])
        token_validation = validate_token(initial_login, info_cliente["rfc"])
        refresh_decision = decide_token_refresh(token_validation, info_cliente["rfc"])

        # flujo (Rama) alterno para generar token expirado
        refreshed_login = log_sat.override(task_id='refresh_token')(info_cliente["rfc"], reset_circuit=True)
        refreshed_validation = validate_token(refreshed_login, info_cliente["rfc"])

        request_with_valid_token = cfdi_request.override(task_id='cfdi_request')(
            validation_data=token_validation, # diccionario completo para que sea reconocido por Xcom
            info_cliente=info_cliente
        )

        request_with_refreshed_token = cfdi_request.override(task_id='cfdi_request_refreshed')(
            validation_data=refreshed_validation, # diccionario completo para que sea reconocido por Xcom
            info_cliente=info_cliente
        )

        # Validaciones
        validation_normal = validate_request(request_with_valid_token, info_cliente["rfc"])
        validation_refreshed = validate_request.override(task_id='validate_refreshed_request')(request_with_refreshed_token, info_cliente["rfc"])

        # Check requests
        check_normal = check_cfdi_request(info_cliente, validation_normal)
        check_refreshed = check_cfdi_request.override(task_id='check_refreshed_request')(info_cliente, validation_refreshed)

        error_handler = handle_failure()

        # Establecer dependencias del flujo normal
        controller >> verification >> initial_login >> token_validation >> refresh_decision

        # Conexión de ramas alternas
        refresh_decision >> [request_with_valid_token, refreshed_login]
        refreshed_login >> refreshed_validation >> request_with_refreshed_token >> validation_refreshed >> check_refreshed >> error_handler
        request_with_valid_token >> validation_normal >> check_normal >> error_handler

    return generated_dag()