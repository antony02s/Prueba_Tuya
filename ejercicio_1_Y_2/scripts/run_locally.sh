#!/bin/bash
set -e  # Detener en el primer error
export $(grep -v '^#' .env | xargs)  # Cargar variables de entorno

echo "🚀 Ejecutando pipeline local..."
python3 src/pipeline.py
echo "✅ Pipeline finalizado"