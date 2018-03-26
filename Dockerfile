FROM python:2.7-alpine

LABEL maintainer="Keitaro Inc <info@keitaro.com>"

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

CMD ["python", "consumer/main.py"]
