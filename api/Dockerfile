FROM amsterdam/python

ENV PYTHONUNBUFFERED 1

EXPOSE 8000

ARG https_proxy=http://10.240.2.1:8080/
ENV https_proxy=$https_proxy

RUN mkdir -p /static && chown datapunt /static

COPY requirements.txt /app/
RUN pip install --cert /usr/local/share/ca-certificates/extras/adp_rootca.crt -r requirements.txt

COPY src /app/
COPY deploy /deploy/

WORKDIR /app

USER datapunt

RUN export DJANGO_SETTINGS_MODULE=containers.settings

RUN python manage.py collectstatic --no-input

CMD uwsgi
