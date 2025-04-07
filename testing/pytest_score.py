import pytest

class ScorePlugin:
    def __init__(self):
        self.scores = {}
    
    def pytest_runtest_logreport(self, report):
        if report.when == "call" and hasattr(report, 'score'):
            test_name = report.nodeid.split("::")[-1]
            self.scores[test_name] = report.score
    
    def pytest_sessionfinish(self, session):
        print("\n\nResumen de Puntuación:")
        total = sum(s['total'] for s in self.scores.values())
        earned = sum(s['earned'] for s in self.scores.values())
        
        for test, score in self.scores.items():
            print(f"{test}: {score['earned']}/{score['total']}")
        
        print(f"\nTOTAL: {earned}/{total} ({earned/total*100:.1f}%)")

def pytest_configure(config):
    config.pluginmanager.register(ScorePlugin())