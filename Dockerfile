FROM python:3.6

RUN pip install pip==9.0.1
ADD . /app
WORKDIR /app
RUN pip install -r requirements.txt --no-cache-dir
ENV PYTHONPATH=/app

