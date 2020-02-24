# Waarnemingen Mensen

Naming conventions:
    - People Measurement: Monitoring crowds of people. Measured with camera that keep count of  people walking by.

Receive data from counting cameras and store it for later analysis.


Start the docker database and run the download en upload scripts.

    docker-compose build
    docker-compose up api


Now you should be able to access http://127.0.0.1:8001/


#### Local development ####

Create a virtualenv and activate it. Then start the development database

	docker-compose up database
	
Fill test data in database.

    docker-compose run api python manage.py migrate

or in your virtual environment:

	python manage.py migrate. (tip add database to your /etc/hosts pointing at 127.0.0.1)

Please schedule api/deploy/docker-migrate.sh script to run once a day. It will create the database partitions.

Then add the requirements:

    pip install -r requirements.txt


# Stress testing with locust
We've got a simple locust test script which fires a bunch of requests. It is automatically started by the locust 
container.

It can also be run manually from the root folder using:

    locust --host=http://127.0.0.1:8001

and starting it from the browser http://127.0.0.1:8089. 

Or run it headless:

    locust --host=http://127.0.0.1:8001 --no-web -c 250 -r 25 --run-time 30s
