## Week 6 Homework 

In this homework, there will be two sections, the first session focus on theoretical questions related to Kafka 
and streaming concepts and the second session asks to create a small streaming application using preferred 
programming language (Python or Java).

</br></br>

### Question 1: 
**Please select the statements that are correct **

### Answer

All statements are correct. 

- Kafka Node is responsible to store topics
- Zookeeper is removed form Kafka cluster starting from version 4.0
- Retention configuration ensures the messages not get lost over specific period of time.
- Group-Id ensures the messages are distributed to associated consumers

</br></br>

### Question 2: 

**Please select the Kafka concepts that support reliability and availability**

### Answer

This is described in video 6.6 Kafka Configuration.

- Topic Replication
- Ack All

</br></br>

### Question 3: 

**Please select the Kafka concepts that support scaling**  


### Answer
This is described in video 6.6 Kafka Configuration.

- Topic Paritioning
- Consumer Group Id

</br></br>

### Question 4: 

**Please select the attributes that are good candidates for partitioning key. 
Consider cardinality of the field you have selected and scaling aspects of your application**  

### Answer

[Cardinality](https://en.wikipedia.org/wiki/Cardinality): In mathematics, the cardinality of a set is a measure of the number of elements of the set. 

In other words, number of unique numbers.


Among the options, payment_type, vendor_id and passenger_count have a natural limited number of unique values. 

Among those three vendor_id has the lowest cardinality and passenger_count the highest.

**- payment_type**
**- vendor_id**
**- passenger_count**


</br></br>

### Question 5: 

**Which configurations below should be provided for Kafka Consumer but not needed for Kafka Producer**

- Deserializer Configuration
- Topics Subscription
- Bootstrap Server
- Group-Id
- Offset
- Cluster Key and Cluster-Secret


### Answer

- Deserializer Configuration
- Topics Subscription
- Group-Id
- Offset

Kafka The definite guide

**Deserializer Configureation - only consumer**

[p. 88] Kafka producers require serializers to convert objects into byte arrays that are then sent to Kafka. Similarly, Kafka consumers require deserializers to convert byte arrays recieved from Kafka into Java objects.

**Topics Subscription - only consumer**


[p. 63] Applications that need to read data from Kafka use a KafkaConsumer to subscribe to Kafka topics and receive messages from these topics.

**Bootstrap server - both**

[p. 44]  bootstrap.servers

List of host:port pairs of brokers that the producer will use to establish initial connection to the Kafka cluster. This list doesn’t need to include all brokers, since the producer will get more information after the initial connection. But it is recommended to include at least two, so in case one broker goes down, the producer will still be able to connect to the cluster.

**group id - only consumer**

[p. 126] ...in order to configure your consumer for a desired reliability behavior.
The first is group.id, as explained in great detail in Chapter 4. The basic idea is that if two consumers have the same group ID and subscribe to the same topic, each will be assigned a subset of the partitions in the topic and will therefore only read a subset of the messages individually (but all the messages will be read by the group as a whole). If you need a consumer to see, on its own, every single message in the topics it is subscribed to—it will need a unique group.id.

**offset - only consumer**

[p. 75] As discussed before, one of Kafka’s unique
characteristics is that it does not track acknowledgments from consumers the way many JMS queues do. Instead, it allows consumers to use Kafka to track their position (offset) in each partition.

**Cluster Key and Cluster-Secret - both**

Both producer and consumer connect to cluster so I assume both need these.


### Question 6:

Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets.
Please use the datasets [fhv_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) 
and [green_tripdata_2019-01.csv.gz](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green)

PS: If you encounter memory related issue, you can use the smaller portion of these two datasets as well, 
it is not necessary to find exact number in the  question.

Your code should include following
1. Producer that reads csv files and publish rides in corresponding kafka topics (such as rides_green, rides_fhv)
2. Pyspark-streaming-application that reads two kafka topics
   and writes both of them in topic rides_all and apply aggregations to find most popular pickup location.

### Answer
WORK IN PROGRES

Follow the setup (requirements, docker, kafka, spark) in this [link](https://github.com/larsskaret/data-engineering-zoomcamp/tree/main/week_6_stream_processing/python/docker)

### Running Streaming Script

spark-submit script ensures installation of necessary jars before running the streaming.py

```bash
./spark-submit.sh streaming.py 
```