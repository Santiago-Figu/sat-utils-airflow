def test_log_output(caplog, score):
    import logging
    logging.info("Este es un mensaje de prueba INFO")
    logging.warning("Este es un mensaje WARNING")
    
    # Verificación y puntuación
    if "mensaje de prueba" in caplog.text:
        score.add_points(20, 20)
    else:
        score.add_points(0, 20)
    
    assert "mensaje de prueba" in caplog.text

def test_simple_fixture(simple_fixture, score):
    assert simple_fixture == 42
    score.add_points(10, 10)  # Puntos por esta prueba