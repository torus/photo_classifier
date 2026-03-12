FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libheif-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install Pillow pillow-heif

CMD ["python", "photo_classifier.py"]
