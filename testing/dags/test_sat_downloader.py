
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from airflow.exceptions import AirflowException
from airflow.models import DagBag, TaskInstance
from pathlib import Path
import sys

import requests

# # Configurar el path para importar tu DAG
# sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Importar tu módulo
from dags.download_xml.download_xml import create_sat_download_dag
RFC = "EWE1709045U0"

@pytest.fixture
def test_dag():
    """Fixture que crea un DAG de prueba"""
    test_config = {
        "schedule": None,
        "type": "test",
        "dag_id": "test_dag"
    }
    return create_sat_download_dag(test_config, "test_dag_circuit", RFC)

def test_circuit_breaker_activation(test_dag, caplog, score):
    """Prueba completa de activación del Circuit Breaker"""
    import logging
    # Obtener la tarea
    task = test_dag.get_task("log_sat")
    
    # Mockear SATDownloader para simular error de conexión
    with patch('dags.download_xml.download_xml.SATDownloader') as mock_sat:
        mock_instance = mock_sat.return_value
        mock_instance._login_sat.side_effect = requests.exceptions.ConnectionError("Error simulado")
        
        # Mock del contexto de ejecución
        context = {
            "task_instance": MagicMock(spec=TaskInstance),
            "dag_run": MagicMock(),
            "params": {}
        }
        context["task_instance"].try_number = 1
        context["task_instance"].max_tries = 10
        
        # 1. Primer intento - debe activar el Circuit Breaker
        with pytest.raises(AirflowException) as exc_info:
            logging.info("PRIMER INTENTO: Se lanzó la excepción ConnectionError")
            score.add_points(5, 20)
            task.execute(context)
            
        logging.warning(f"exc_info.value: {exc_info.value}")
        assert "Error de conexión con SAT" in str(exc_info.value), "Mensaje de excepción no coincide, validar"
        
        # 2. Segundo intento - debe fallar inmediatamente por Circuit Breaker
        with pytest.raises(AirflowException) as exc_info:
            logging.info("SEGUNDO INTENTO: Se lanzó la excepción 'Circuit Breaker activo - Servicio SAT no disponible'")
            score.add_points(10, 20)
            task.execute(context)
            
        
        assert "Circuit Breaker activo" in str(exc_info.value), "Mensaje de Circuit Breaker no coincide, validar"
        logging.info("ASSERT 2 PASÓ: Circuit Breaker se mantuvo activo")
        score.get_total_points()

# def test_circuit_breaker_reset():
#     """Prueba que el Circuit Breaker se resetea después de una conexión exitosa"""
#     # Configuración de prueba
#     test_config = {
#         "schedule": None,
#         "type": "test",
#         "dag_id": "test_dag"
#     }
#     test_rfc = RFC
    
#     # Crear el DAG de prueba
#     dag = create_sat_download_dag(test_config, "test_dag_circuit_reset", test_rfc)
#     log_sat_task = dag.get_task("log_sat")
#     context = {
#         "task_instance": MagicMock(),
#         "dag_run": MagicMock(),
#         "params": {}
#     }
    
#     with patch('sat_utils.sat_downloader.SATDownloader') as mock_sat:
#         mock_instance = mock_sat.return_value
        
#         # Simular primero un error de conexión
#         mock_instance._login_sat.side_effect = ConnectionError("Error inicial")
#         with pytest.raises(AirflowException):
#             log_sat_task.execute(context)
        
#         # Luego simular una respuesta exitosa
#         mock_instance._login_sat.side_effect = None
#         mock_instance._login_sat.return_value = {
#             'AutenticaResult': 'token_valido',
#             'Created': datetime.now(),
#             'Expires': datetime.now()
#         }
        
#         # Esta ejecución debería resetear el Circuit Breaker
#         result = log_sat_task.execute(context)
#         assert 'token_valido' in result['AutenticaResult']
        
#         # Verificar que ahora acepta nuevas conexiones
#         mock_instance._login_sat.side_effect = ConnectionError("Nuevo error")
#         with pytest.raises(AirflowException, match="Circuit Breaker activado"):
#             log_sat_task.execute(context)