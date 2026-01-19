FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir --default-timeout=120 pyarrow \
 && pip install --no-cache-dir --default-timeout=120 -r requirements.txt


COPY project1.py .

CMD ["python", "project1.py"]
