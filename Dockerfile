FROM ubuntu:latest
LABEL authors="rocky"

ENTRYPOINT ["top", "-b"]
FROM python:3.11-bullseye
WORKDIR /app

COPY requirements.tx requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

CMD ["./stock_market_ETL/dagster dev"]