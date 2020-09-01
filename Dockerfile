FROM amsterdam/python:3.8-buster as app
MAINTAINER datapunt@amsterdam.nl

ENV PYTHONUNBUFFERED 1
ARG AUTHORIZATION_TOKEN
ARG GET_AUTHORIZATION_TOKEN
ARG SECRET_KEY

EXPOSE 8000

RUN mkdir -p /static && chown datapunt /static

COPY requirements.txt /app/
RUN pip install -r requirements.txt

# TODO: Remove requirements_dev in favor of a multistage dockerfile
COPY requirements_dev.txt /app/
RUN pip install -r requirements_dev.txt

COPY src /app/
COPY deploy /deploy/

WORKDIR /app

USER datapunt

ENV DJANGO_SETTINGS_MODULE=main.settings

RUN python manage.py collectstatic --no-input

CMD uwsgi


# Tests
FROM app as tests
WORKDIR /tests
COPY tests /tests

ENV COVERAGE_FILE=/tmp/.coverage
ENV PYTHONPATH=/app

CMD ["pytest"]
