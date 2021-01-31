# ASVSP

A college project for big data subject. It uses this dataset: https://www.kaggle.com/sobhanmoosavi/us-accidents?select=US_Accidents_June20.csv for analyzing car accidents in America between 2016 - 2020. Detailed explenation about the format of the data and meaning of all 49 attributes can be found on this link: https://smoosavi.org/datasets/us_accidents. In this file only the attributes that were relevent for the processing will be mentioned and explained. 

### Goals of Batch data processing
The idea of batch data processing was to group accidents by severity and show which one happen the most, group them by weather and show in which weather accidents happen the most, group them by the time of the day. We've also shown number of accidents that happended by state and by year. 

### Goals of Real Time Processing

The idea with this processing was to have a real time update ( for example one hour ) about streets and the number of accidents and distance occupied by those accidents in that street. This info can be used by people to know in which streets the accidents happened and how much trafic jam do those accidents cause so they can know if they should avoid them or not.

### How to run it

First you need to get the dataset. Position yourself in /batch/data folder and unpack the zip file. Do the same for /kafka/producer-docker/producer/src/main/resources location ( there will again be a zip file which needs to be uncompressed ).

You need to have docker and docker-compose installed to run this. For more information about installing this visit https://docs.docker.com/get-docker/ and https://docs.docker.com/compose/install/. 

### Batch

Postion yourself in /batch/docker-spark/ folder and run ``` docker-compose up```. After the containers have started, run ```docker exec -it spark-master bash```.
You should now see a shell. To run the example run the following command: ```spark/bin/spark-submit /home/code/test.py```. You can stop all the containers by running ```docker-compose down```.

### Real time
Position yourself in /kafka directory and run ```docker-compose up```. After everything has started the producer should start producing data to ```car-accidents-topic``` topic. Kafka stream app will read that data, transform it and produce result to the ```car-accidents-r1-topic```. To see the result it produces you can use kafka-console-consumer ( this requires kafka to be installed on your local machine ). To see the data run the ```kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic car-accidents-r1-topic --property print.key=true --property key.separator="-" --from-beginning```. You can stop the containers by running ```docker-compose down```
