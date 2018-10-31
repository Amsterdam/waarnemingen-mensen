# iotsignals
Recieve IOT data from various systems and store it for later analysis.


Start the docker database and run the download en upload scripts.
```
docker-compose build
docker-compose up database api
```

Now you should be able to access http://127.0.0.1:8001/iotsignals/


#### Local development ####

Create a local environment and activate it:
```
virtualenv --python=$(which python3) venv
source venv/bin/activate
```

Start development database


```
	docker-compose up database
```

load production data:


```
	docker-compose exec database update-db.sh iotsignals
```

```
pip install -r requirements.txt
```
