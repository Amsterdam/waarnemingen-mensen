FROM amsterdam/python

ENV PYTHONUNBUFFERED 1

EXPOSE 8000

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
