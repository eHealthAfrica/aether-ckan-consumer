FROM python:2.7-alpine

RUN apk update && \
    apk add --no-cache \
        bash \
        gcc \
        libffi-dev \
        linux-headers \
        musl-dev \
        openssl-dev

ADD . /code

WORKDIR code

RUN pip install -r requirements.txt

CMD ["python", "consumer/main.py"]
