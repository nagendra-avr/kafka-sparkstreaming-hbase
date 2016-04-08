package com.kafka.sparkstreaming.kafka;

/**
 * Created by Nagendra on 4/9/16.
 */
public interface IProducer {

    /**
     * To open the connection
     */
    void open();

    /**
     * Send the data to the topic
     * @param data the data to be send
     * @param topic the topic to which data will be send
     */
    void send(final String data, final String topic);

    /**
     * Close the connection
     */
    void close();

}
