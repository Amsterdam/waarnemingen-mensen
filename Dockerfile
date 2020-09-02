FROM amsterdam/python:3.8-buster as app
MAINTAINER datapunt@amsterdam.nl

ENV PYTHONUNBUFFERED 1
ARG AUTHORIZATION_TOKEN
ARG GET_AUTHORIZATION_TOKEN
ARG SECRET_KEY

EXPOSE 8000

RUN mkdir -p /static && chown datapunt /static

WORKDIR /src/

COPY requirements.txt /src/
RUN pip install -r requirements.txt

COPY src /src/
COPY deploy /deploy/

USER datapunt
RUN python manage.py collectstatic --no-input

CMD uwsgi

# Tests
FROM app as tests

WORKDIR /src/
USER root
COPY requirements_dev.txt /src/
RUN pip install -r requirements_dev.txt

WORKDIR /tests
COPY tests /tests

ENV COVERAGE_FILE=/tmp/.coverage
ENV PYTHONPATH=/src

CMD ["pytest"]
