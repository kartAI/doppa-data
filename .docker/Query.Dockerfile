FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/
RUN apt-get update && apt-get install -y \
    libexpat1 \
    libgdal-dev \
    && pip install --no-cache-dir gdal==$(gdal-config --version) \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app