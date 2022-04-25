FROM python:3.9.1

WORKDIR /app

COPY . .

RUN pip3 install -r requirements.txt

CMD ["python3", "address/process_txns_for_famous_address.py"]