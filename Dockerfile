FROM python:3.9.5-alpine3.13

WORKDIR . 

COPY . .

CMD ["python", "process_transactions.py"]
