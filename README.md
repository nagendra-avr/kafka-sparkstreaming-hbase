# Kafka-SparkStreaming-HBase Application

A Spark streaming application that receives the live streaming from Kafka, process and stores back into Hbase

## Usage:

To get Started:

 * download or clone the project
 * run mvn clean install
 * run the below commands

## Commands:

* java -jar target/kafka-spark-streaming-hbase-example-0.1-SNAPSHOT-jar-with-dependencies.jar com.kafka.sparkstreaming.kafka.MeterSignalsProducer

* /usr/lib/spark/bin/spark-submit --class com.kafka.sparkstreaming.driver.FaultDetectionDriver --master local[3] target/kafka-spark-streaming-hbase-example-0.1-SNAPSHOT-jar-with-dependencies.jar

