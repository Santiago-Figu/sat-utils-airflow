import pytest
import logging
import warnings
from _pytest.logging import LogCaptureFixture

# Configuración básica
@pytest.fixture(autouse=True)
def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )

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

@pytest.fixture
def score():
    """Fixture mejorado para llevar puntuación"""
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