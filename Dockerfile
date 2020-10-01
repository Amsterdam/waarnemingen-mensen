FROM amsterdam/python:3.8-buster as app
MAINTAINER datapunt@amsterdam.nl

WORKDIR /app_install
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY deploy /deploy

WORKDIR /src
COPY src .

ARG SECRET_KEY=not-used
ARG AUTHORIZATION_TOKEN=not-used
ARG GET_AUTHORIZATION_TOKEN=not-used
RUN python manage.py collectstatic --no-input

USER datapunt

CMD ["/deploy/docker-run.sh"]

# stage 2, dev
FROM app as dev

USER root
WORKDIR /app_install
ADD requirements_dev.txt requirements_dev.txt
RUN pip install -r requirements_dev.txt

WORKDIR /src
USER datapunt

# Any process that requires to write in the home dir
# we write to /tmp since we have no home dir
ENV HOME /tmp

CMD ["./manage.py", "runserver", "0.0.0.0:8000"]

# stage 3, tests
FROM dev as tests

USER datapunt
WORKDIR /tests
ADD tests .

ENV COVERAGE_FILE=/tmp/.coverage
ENV PYTHONPATH=/src

CMD ["pytest"]
