FROM python:3.6.5-slim-stretch

WORKDIR /root/server

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

CMD python app.py