FROM python:3.9

WORKDIR /app

COPY ingest.py ingest.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt
RUN apt install wget

ENTRYPOINT ["python", "ingest.py"]

