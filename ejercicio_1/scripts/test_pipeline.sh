#!/bin/bash
set -e
export $(grep -v '^#' .env.test | xargs)

echo "🧪 Corriendo pruebas de pipeline con datos de muestra..."
pytest tests/ --maxfail=1 --disable-warnings -q