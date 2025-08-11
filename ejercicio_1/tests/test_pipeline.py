import os
from src.pipeline import main

def test_pipeline_runs(monkeypatch):
    # Variables de entorno fake
    monkeypatch.setenv("PG_USER", "test")
    monkeypatch.setenv("PG_PWD", "test")
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.setenv("PG_DB", "test_db")

    # Ejecutar pipeline (con fuentes de muestra)
    main()
    assert True  # Si no lanza excepción, pasa