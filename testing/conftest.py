from unittest.mock import patch
import pytest
import logging
import warnings
from _pytest.logging import LogCaptureFixture
import airflow
from config.log.logger import Logger

# Configuración básica
@pytest.fixture()
def configure_logging():
    logger = Logger(file_name="testing", debug= True).get_logger()
    return logger

# Fixture de score CORREGIDO
class Score:
    def __init__(self):
        self.total = 0
        self.earned = 0
    
    def add_points(self, earned, total):
        self.earned += earned
        self.total = total
        print(f"✓ Puntos añadidos: {earned}/{total}")
    
    def get_total_points(self):
        print(f"Total: {self.earned}/{self.total}")

@pytest.fixture(scope="session")
def mock_airflow_db():
    """Fixture para mockear completamente la base de datos de Airflow"""
    with patch('airflow.settings.Session'), \
         patch('airflow.models.DagBag'), \
         patch('airflow.models.DagModel'), \
         patch('airflow.utils.db.create_default_connections'):
        yield

@pytest.fixture
def dag_factory(mock_airflow_db):
    """Fixture factory para crear DAGs con parámetros personalizados"""
    from dags.download_xml.register import create_sat_download_dag
    
    def _create_dag(config_id="test_id", dag_id="test_dag_circuit", rfc="TEST12345678"):
        test_config = {
            "schedule": None,
            "type": "test",
            "dag_id": config_id
        }

        dag = create_sat_download_dag(test_config, dag_id, rfc)
        
        # Acceder a la función original para manipular su estado
        log_sat_func = dag.get_task("log_sat").python_callable
        
        # Resetear el Circuit Breaker antes de cada prueba
        if hasattr(log_sat_func, '__closure__'):
            for cell in log_sat_func.__closure__:
                if isinstance(cell.cell_contents, bool):
                    # Encontrar y resetear la variable circuit_breaker_active
                    cell.cell_contents = False
        
        return dag
    
    return _create_dag


@pytest.fixture(scope="session")
def airflow_db():
    from airflow.utils import db
    db.resetdb()
    yield
    db.resetdb()

@pytest.fixture
def score():
    """Llama a la clase Score para llevar puntuación"""
    return Score()

# Fixture simple de prueba
@pytest.fixture
def simple_fixture():
    print("\n¡FIXTURE FUNCIONANDO!")
    return 42

# Configuración de caplog
@pytest.fixture
def caplog(caplog: LogCaptureFixture):
    caplog.set_level(logging.INFO)
    return caplog

# Suprimir warnings
warnings.filterwarnings(
    "ignore",
    message="Airflow currently can be run on POSIX-compliant Operating Systems",
    category=RuntimeWarning
)