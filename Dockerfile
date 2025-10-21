# ============================
#  ALTEO VM Dockerfile
# ============================

FROM python:3.11-slim

# Környezeti beállítások
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /usr/src/app

# Rendszerfüggőségek
RUN apt-get update && apt-get install -y \
    supervisor \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Követelmények
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Alkalmazásfájlok
COPY . .

# Log könyvtár
RUN mkdir -p /usr/src/app/logs

# Supervisor indítása
CMD ["supervisord", "-c", "/usr/src/app/supervisord.conf"]
