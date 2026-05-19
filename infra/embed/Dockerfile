FROM python:3.11-slim

WORKDIR /app

# Install dependencies before copying code for better layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY serve.py .

EXPOSE 8080
ENV EMBED_MODEL=sentence-transformers/all-MiniLM-L6-v2
ENV EMBED_MAX_TEXTS=256

CMD ["uvicorn", "serve:app", "--host", "0.0.0.0", "--port", "8080"]
