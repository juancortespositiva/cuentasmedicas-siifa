FROM python:3.12-slim

# Evita problemas de buffer y mejora logs
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .

# Puerto obligatorio en Cloud Run
ENV PORT=8080

# Ejecutar con gunicorn (PRODUCCIÓN)
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 main:app
