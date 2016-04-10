package com.kafka.sparkstreaming.driver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;


import com.kafka.sparkstreaming.functions.HbasePutConvertFunction;
import com.kafka.sparkstreaming.functions.HbaseSaveFunction;
import com.kafka.sparkstreaming.model.Meter;
import kafka.serializer.StringDecoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;


/**
 * The driver class to start the streaming of Meters
 * @author Nagendra Amalakanta
 *
 */
public class FaultDetectionDriver extends BaseDriver {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	final static Logger LOG = LoggerFactory.getLogger(FaultDetectionDriver.class);

	//static JavaStreamingContext ssc;
	
	public FaultDetectionDriver(Map<String, String> metadata) {
		super(metadata);
	}
	
	public static void main(String[] args) {

		try {
			//Load the properties into hashmap
			Map<String,String> metadata = new HashMap<String,String>();

			String configFilePath =  System.getenv("CONFIG_FILE");

			Properties prop = null;
			if(configFilePath == null) {
				prop = loadProperties();
			} else {
				prop = loadPropertiesFromExternal(configFilePath);
			}
			metadata.put("master", loadProperties().getProperty("spark.master"));
			metadata.put("appName", loadProperties().getProperty("spark.appName"));
			metadata.put("batchsize", loadProperties().getProperty("batch.size"));
			metadata.put("kafka.zookeepers", loadProperties().getProperty("kafka.zookeepers"));
			metadata.put("kafka.consumer_group", loadProperties().getProperty("kafka.consumer_group"));
			metadata.put("kafka.topic", loadProperties().getProperty("kafka.topic"));
			metadata.put("noofthreads", loadProperties().getProperty("noofthreads"));
			metadata.put("kafka.brokers", loadProperties().getProperty("kafka.brokers"));
			metadata.put("spark.ui.port", loadProperties().getProperty("spark.ui.port"));
			metadata.put("spark.driver.allowMultipleContexts", loadProperties().getProperty("spark.driver.allowMultipleContexts"));

			final IDriver driver = new FaultDetectionDriver(metadata);

			JavaPairInputDStream<String, String> kafkaStream = driver.getKafkaDirectStream();

			kafkaStream.print();

			JavaDStream<Meter> meterStream = kafkaStream.map(new Function<Tuple2<String, String>, Meter>() {

                @Override
                public Meter call(Tuple2<String, String> meterData) throws Exception {
                    Meter meter = new Meter();
                    String[] meters = meterData._2.split(",");
                    meter.setMeterID(meters[0]);
                    meter.setMeterStatus(meters[1]);
                    meter.setMeterTime(meters[2]);
                    return meter;
                }
            });

            //Convert meter dstream to pair of immutable bytes and put objects
           JavaPairDStream<ImmutableBytesWritable, Put> meterPairStream =  meterStream.mapToPair(new HbasePutConvertFunction());


            //Save to Hbase
            meterPairStream.foreachRDD(new HbaseSaveFunction());

			driver.start();
			driver.stop();

		} catch (Exception e) {
			LOG.error(e.getMessage());
		}

	}

	
	public static Properties loadProperties() {
		
		Properties props = new Properties();
		InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties");
		if(null != inputStream) {
			try {
				props.load(inputStream);
			} catch (IOException e) {
				LOG.error(String.format("Config.properties file not found in the classpath"));
			}
		}
		
		return props;
		
	}
	
	public static Properties loadPropertiesFromExternal(String filePath) {

		Properties props = new Properties();
		InputStream inputStream;
		try {
			inputStream = new FileInputStream(new File(filePath));
			props.load(inputStream);
		} catch (FileNotFoundException e1) {
			LOG.error(String.format("%s not found. Please provide the correct path",filePath));
		} catch (IOException e) {
			LOG.error(String.format("Failed to load the properties file %s with reason %s",filePath,e.getMessage()));
		}

		return props;
	}
}
