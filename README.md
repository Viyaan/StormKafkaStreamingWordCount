# StormKafkaStreamingWordCount

Application recieves live streaming data from kafka. and counts the number of words in each record.

## Getting Started

KafkaSpoutTopology has the below components

storm.kafka.KafkaSpout - > Storm API to recieve messages from Kafka and emits to WordSpitBolt

WordSpitBolt -->  Splits the number of words in each record and emits each word form the incoming record to WordCountBolt

WordCountBolt-->  Counts the number of words. And keeps the count of each word in Internal MAP.


### Prerequisites

Install and Run Zookeeper and Kafka
Create Topic

### Installing


Start Zookeeper:
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

Start Kafka:
.\bin\windows\kafka-server-start.bat .\config\server.properties


Create topic
.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitter-topic


End with an example of getting some data out of the system or using it for a little demo

## Running the tests





## Dependencies


	<dependency>
            <groupId>org.apache.storm</groupId>
            <artifactId>storm-core</artifactId>
            <version>0.9.4</version>
        </dependency>
        <dependency>
            <groupId>org.apache.storm</groupId>
            <artifactId>storm-kafka</artifactId>
            <version>0.9.3</version>
    </dependency>

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management
* [Storm](http://storm.apache.org/) - Apache Storm is a free and open source distributed realtime computation system.

## Contributing


## Versioning



## Authors

* **Viyaan Jhiingade** - *Initial work* - [Viyaan](https://github.com/Viyaan)



## License



## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc



