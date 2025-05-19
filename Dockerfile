# Imagen base de Python 3.10
FROM python:3.10-slim

# Definir el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar el archivo de dependencias al contenedor
COPY requirements.txt /app/requirements.txt

# Instalar las dependencias
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copiar el código fuente y otros archivos necesarios al contenedor
COPY src /app/src
COPY utils /app/utils
COPY .env /app/.env

# Configurar el PYTHONPATH
ENV PYTHONPATH=/app/src

# Establecer el comando por defecto para ejecutar el contenedor
CMD ["python3", "/app/src/llamadas_api.py"]
