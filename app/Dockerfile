FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./main.py /app/main.py

WORKDIR /app

CMD ["uvicorn", "main:app", "--host", "20.195.164.120", "--port", "8000"]