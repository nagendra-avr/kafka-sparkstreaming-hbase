package com.kafka.sparkstreaming.driver;

import org.apache.spark.streaming.api.java.JavaPairInputDStream;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * @author Nagendra Amalakanta
 *
 */
public interface IDriver extends Serializable{
	
	/**
	 * Initialize the spark driver with the configurations
	 */
	public void initialize(Map<String, String> metadata);
	
	/**
	 * Start the spark context in the driver
	 */
	public void start();
	
	/**
	 * stop the spark context in the driver
	 */
	public void stop();

	/**
	 *
	 * @return
	 */
	public JavaPairInputDStream<String, String> getKafkaDirectStream();

}
