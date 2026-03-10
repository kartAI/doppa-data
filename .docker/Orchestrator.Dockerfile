FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y curl ca-certificates gnupg && \
    curl -sL https://aka.ms/InstallAzureCLIDeb | bash && \
    rm -rf /var/lib/apt/lists/*

COPY . .
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "-m", "main"]