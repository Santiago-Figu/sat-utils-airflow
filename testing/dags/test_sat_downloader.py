
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, UTC
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance

import requests

# # Configurar el path para importar tu DAG
# sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def test_circuit_breaker_activation(dag_factory, configure_logging, score):
    """Prueba completa de activación del Circuit Breaker"""

    configure_logging.info("Ejecutando prueba test_circuit_breaker_activation")
    # Obtener la tarea
    test_dag = dag_factory(dag_id="test_circuit_breaker")
    task = test_dag.get_task("log_sat")
    
    # Mockear SATDownloader para simular error de conexión
    with patch('dags.download_xml.task.SATDownloader') as mock_sat:
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
            configure_logging.info("PRIMER INTENTO: Se lanzó la excepción ConnectionError")
            score.add_points(5, 20)
            task.execute(context)
            
        configure_logging.warning(f"exc_info.value: {exc_info.value}")
        assert "Error de conexión con SAT" in str(exc_info.value), "Mensaje de excepción no coincide, validar"
        
        # 2. Segundo intento - debe fallar inmediatamente por Circuit Breaker
        with pytest.raises(AirflowException) as exc_info:
            configure_logging.info("SEGUNDO INTENTO: Se lanzó la excepción 'Circuit Breaker activo - Servicio SAT no disponible'")
            score.add_points(10, 20)
            task.execute(context)
            
        
        assert "Circuit Breaker activo" in str(exc_info.value), "Mensaje de Circuit Breaker no coincide, validar"
        configure_logging.info("ASSERT 2 PASÓ: Circuit Breaker se mantuvo activo")
        score.add_points(5, 20)
        score.get_total_points()

def test_circuit_breaker_reset(dag_factory, configure_logging, score):
    """Prueba que el Circuit Breaker se resetea correctamente"""
 
    configure_logging.info("Ejecutando prueba test_circuit_breaker_reset")
    test_dag = dag_factory(dag_id="test_circuit_breaker_reset", rfc="EWE1709045U0")
    log_sat_task = test_dag.get_task("log_sat")
    log_sat_task.op_kwargs = {'reset_circuit': True}
    
    context = {
        "task_instance": MagicMock(),
        "dag_run": MagicMock(),
        "params": {}
    }
    context["task_instance"].try_number = 1
    context["task_instance"].max_tries = 10
    
    with patch('dags.download_xml.task.SATDownloader') as mock_sat:
        mock_instance = mock_sat.return_value
        
        # 1. Simular error inicial para activar Circuit Breaker
        mock_instance._login_sat.side_effect = requests.exceptions.ConnectionError("Error simulado")
        with pytest.raises(AirflowException, match="Circuit Breaker activado"):
            log_sat_task.execute(context)
        
        # 2. Simular éxito para resetear Circuit Breaker
        mock_instance._login_sat.side_effect = None
        mock_instance._login_sat.return_value = {
            'AutenticaResult': "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.KMUFsIDTnFmyG3nMiGM6H9FNFUROf3wh7SmqJp-QV30",
            'Created': datetime.now(UTC),
            'Expires': datetime.now(UTC)+ timedelta(seconds=300)
        }
        
        # Esta ejecución debe pasar (Circuit Breaker reseteado)
        result = log_sat_task.execute(context)
        assert result['AutenticaResult']
        
        # 3. Verificar que acepta nuevos errores (no debe estar bloqueado)
        mock_instance._login_sat.side_effect = requests.exceptions.ConnectionError("Nuevo error")
        with pytest.raises(AirflowException, match="Circuit Breaker activado"):
            log_sat_task.execute(context)