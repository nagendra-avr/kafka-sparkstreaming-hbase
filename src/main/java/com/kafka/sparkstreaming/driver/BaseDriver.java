package com.kafka.sparkstreaming.driver;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import javax.xml.datatype.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

/**
 * Created by nagi on 4/8/16.
 */
public  class BaseDriver implements IDriver{

    private Map<String,String> metadata;
    private JavaStreamingContext jssc;

    /**
     * Simple constructor
     */
    public BaseDriver(){

    }

    /**
     * constructor with metadata initialization
     * @param metadata
     */
    public BaseDriver(Map<String,String> metadata) {
        initialize(metadata);
    }


    public void initialize(Map<String,String> metadata) {

        this.metadata = metadata;

        final SparkConf sparkConf = new SparkConf();
        if(metadata.get("master") != null) {
            sparkConf.setMaster(metadata.get("master"));
        }
        if(metadata.get("appName") != null) {
            sparkConf.setAppName(metadata.get("appName"));
        }
        int batchsize=1000;
        if(metadata.get("batchsize") != null) {
            batchsize = Integer.parseInt(metadata.get("batchsize"));
        }
        if(metadata.get("spark.ui.port") != null) {
            sparkConf.set("spark.ui.port",metadata.get("spark.ui.port"));
        }
        /*if(metadata.get("spark.driver.allowMultipleContexts") != null) {
            sparkConf.set("spark.driver.allowMultipleContexts",metadata.get("spark.driver.allowMultipleContexts"));
        }*/
        jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchsize));

        if(metadata.get("checkpoint") != null) {
            jssc.checkpoint(metadata.get("checkpoint"));
        }

    }

    public JavaPairInputDStream<String, String> getKafkaDirectStream() {

        final HashMap<String,Integer> topics = new HashMap<String,Integer>();
        topics.put("meter_signals_topic",2);

        final HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", metadata.get("kafka.brokers"));
        kafkaParams.put("zookeeper.connect", metadata.get("kafka.zookeepers"));
        kafkaParams.put("group.id", metadata.get("kafka.consumer_group"));

        System.out.println("metadata.get(\"kafka.brokers\")" +metadata.get("kafka.brokers"));
        System.out.println("");
        System.out.println("metadata.get(\"kafka.zookeepers\")" + metadata.get("kafka.zookeepers"));
        System.out.println("metadata.get(\"kafka.consumer_group\")" + metadata.get("kafka.consumer_group") );
        System.out.println("metadata.get(\"kafka.topic\")"+metadata.get("kafka.topic"));
        JavaPairInputDStream<String, String> stream=null;
        // Create direct kafka stream with brokers and topics

        //JavaPairReceiverInputDStream<String, String> dstreamData = KafkaUtils.createStream(jssc, "localhost:2181", "meter_signals_topic", topics);

        try {
            stream = KafkaUtils.createStream(
                    jssc,
                    String.class,
                    String.class,
                    StringDecoder.class,
                    StringDecoder.class,
                    kafkaParams,
                    topics,
                    StorageLevel.MEMORY_ONLY_SER()
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stream;
    }

    public void start() {
        jssc.start();
    }

    public void stop() {
        jssc.awaitTermination();
    }

}
