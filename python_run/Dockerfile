FROM python:3.9

WORKDIR .

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest_to_db.py .

CMD ["python", "ingest_to_db.py"]