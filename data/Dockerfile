FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data/data_generator.py main.py

EXPOSE 8080

# ✅ Run cron-style job
CMD ["python", "data_generator.py"]

