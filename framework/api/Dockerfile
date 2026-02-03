FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código del API
COPY app.py .

# ✅ Copia artifacts del modelo dentro del contenedor
# OJO: este path es relativo al CONTEXTO de build (framework/api)
COPY ../mlops/artifacts/ ./mlops_artifacts/

ENV MODEL_PATH=/app/mlops_artifacts/model.pkl

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
