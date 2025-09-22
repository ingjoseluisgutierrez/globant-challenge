# Globant Data Engineering Challenge

Paso 1: API mínima con FastAPI.

Endpoints iniciales:
- GET /       -> salud básica
- GET /health -> chequeo rápido

Cómo correr:
1. python -m venv venv
2. source venv/bin/activate  (o .\venv\Scripts\Activate.ps1 en Windows)
3. pip install -r requirements.txt
4. uvicorn main:app --reload --host 0.0.0.0 --port 8000
