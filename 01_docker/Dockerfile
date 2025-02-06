FROM python:latest

RUN apt-get update && apt-get install -y wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY loadNYdata.py loadNYdata.py

ENTRYPOINT [ "python", "loadNYdata.py" ]