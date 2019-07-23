# iotsignals

Naming conventions:
    - iotsignals: Umbrella term for this repository since the data is retrieved through IoT devices (e.g: cameras)
    - Passages: Monitoring cars. Measured with cameras that keep count of passing cars.
    - People Measurement: Monitoring crowds of people. Measured with camera that keep count of  people walking by.

Recieve IOT data from various systems and store it for later analysis.


Start the docker database and run the download en upload scripts.

    docker-compose build
    docker-compose up database api


Now you should be able to access http://127.0.0.1:8001/iotsignals/


#### Local development ####

Create a local environment and activate it:

    virtualenv --python=$(which python3) venv
    source venv/bin/activate


Start development database

	docker-compose up database
	
Fill test data in database.

    docker-compose run api python manage.py migrate

or in your virtual environment:

	python manage.py migrate. (tip add database to your /etc/hosts pointing at 127.0.0.1)

Please schedule api/deploy/docker-migrate.sh script to run once a day. It will create the database partitions.

Then add the requirements:

    pip install -r requirements.txt
