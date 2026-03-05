FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/
RUN apt-get update && apt-get install -y libexpat1 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app

EXPOSE 8000

CMD ["uvicorn", "src.presentation.endpoints.tile_server:app", "--host", "0.0.0.0", "--port", "8000"]
