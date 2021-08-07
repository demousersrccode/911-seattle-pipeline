
# 911-Seattle-Pipeline

An ETL pipeline that streams in Seatle's 911 emergency calls in real-time using KafkaStream and consuming the data stream using PySpark followed by loading the data 
to an ElasticSearch Index; 

Exploratory analysis can then be performed on the indexed data using Kibana.

This is an illustration of the pipeline architecture.

![alt text](https://github.com/BrightEmah123/911-seattle-pipeline/blob/main/img/Architecture-911-Seattle.png?raw=true)


## Pre-requisites
* API TOKEN - to read api response from data.seattle.gov
    * This can be retrieved by signing up [here](https://data.sfgov.org/) (the open data portal)

* [Docker-Desktop](https://docs.docker.com/get-docker/) is required for this project since this is all containerized



## Usage
* Clone the repo and ```cd``` to the project.

* Store the API token gotten from the open data portal to an ```.env``` config file; there is a sample txt file (```env.txt```) found on the ```dags/``` folder - this file can easily be converted to the ```.env``` file.

* Open the terminal and cd to the project then run this command to initiate the docker containers.
    ```bash
    docker-compose up
    ```
* Running that command starts up all containers; it might take a while at the first run because all the dependecies will need to be installed on each containers

* If everything goes well, open a browser and visit the airflow dashboard to spin-up the dags handling the ETL pipeline
  the url for the dashboard should be [localhost](http://localhost:8080/admin/).
    ![alt text](https://github.com/BrightEmah123/911-seattle-pipeline/blob/main/img/Airflow_Home.png?raw=true)

* To startup the pipeline; first toggle on the ```data_stream_DAG``` DAG - this allows for kafka to initiate streaming the api response to a Topic.
    ![alt text](https://github.com/BrightEmah123/911-seattle-pipeline/blob/main/img/Airflow_Kafka_Stream.png?raw=true)

* After doing that, the status of the ```data_stream_DAG``` should switch to ```running```; once that happens toggle the second DAG ```spark_consumer_DAG``` to consume the data stored on the Kafka Topic  which will in turn process it to Elasticsearch.
     ![alt text](https://github.com/BrightEmah123/911-seattle-pipeline/blob/main/img/Airflow_Spark_Consume.png?raw=true)

* If everything goes well, the elastic index ```calls``` would probably be created/populated with the response data; the next step will be to define the index on [kibana](http://localhost:5601/app/kibana#/management/kibana/index_patterns)
    ![alt text](https://github.com/BrightEmah123/911-seattle-pipeline/blob/main/img/Create_Index_Pattern.png?raw=true)

* KQL (Kibana Query Language) can then be worked on the index - [KQL](http://localhost:5601/app/kibana#/discover)
    ![alt text](https://github.com/BrightEmah123/911-seattle-pipeline/blob/main/img/Kibana_Discover.png?raw=true)
    >> An example of querying an index can be seen below - here we try filtering the index data where the precinct name is ```North```

    ![alt text](https://github.com/BrightEmah123/911-seattle-pipeline/blob/main/img/KQL.png?raw=true)

* Exploratory Analysis on the 911 calls can also be done and presented on the Kibana Dashboard



## Appendix

This project is open to contributions and additions from the community; feel free to open an issue if you experience any problem


## Authors

- [@bright_emah](https://www.github.com/BrightEmah123)

  