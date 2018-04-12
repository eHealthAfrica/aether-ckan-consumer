FROM python:2.7-alpine

RUN apk update && \
    apk add --no-cache \
        bash \
        gcc \
        libffi-dev \
        linux-headers \
        musl-dev \
        openssl-dev

WORKDIR /srv/app

# This avoids reinstalling Python packages each time the image is rebuilt
ADD ./requirements.txt /srv/app/requirements.txt

RUN pip install -r requirements.txt

ADD . /srv/app

CMD ["python", "-m", "consumer.main"]
