# Python 3.11 tabanlı hafif imaj
FROM python:3.11-slim

# Çalışma dizinini ayarla
WORKDIR /app

# Gerekli paketler için sistem bağımlılıklarını yükle
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Python bağımlılıklarını yükle
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama dosyalarını kopyala
COPY . .

# Railway, PORT ortam değişkenini otomatik atar
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]