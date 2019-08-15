FROM ubuntu:18.04

EXPOSE 8089
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install locust
