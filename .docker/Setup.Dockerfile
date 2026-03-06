FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/
RUN apt-get update && apt-get install -y \
    libexpat1 \
    git \
    g++ \
    make \
    libsqlite3-dev \
    zlib1g-dev \
    && git clone https://github.com/felt/tippecanoe.git /tmp/tippecanoe \
    && cd /tmp/tippecanoe \
    && make -j$(nproc) \
    && make install \
    && cd / \
    && rm -rf /tmp/tippecanoe \
    && apt-get purge -y git g++ make \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app