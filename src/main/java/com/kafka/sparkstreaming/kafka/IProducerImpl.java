package com.kafka.sparkstreaming.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Nagendra on 4/9/16.
 */
public class IProducerImpl implements IProducer {

    private static final Logger LOG = LoggerFactory.getLogger(IProducerImpl.class);

    private String brokers;

    private Producer<Object, Object> producer;

    public IProducerImpl() {
        Properties prop =  loadProperties();
        brokers = prop.getProperty("kafka.brokers");
        open();
    }
    @Override
    public void open() {

        if(null == producer) {
            final Properties props = new Properties();
            props.put("metadata.broker.list", brokers);
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("request.required.acks", "1");
            props.put("producer.type", "sync");
            // props.put("compression.codec", "none");

            producer = new Producer<Object, Object>(new ProducerConfig(props));
        }
    }

    @Override
    public void send(String data, String topic) {

        if(null != producer) {
            producer.send(new KeyedMessage<Object, Object>(topic, data));
        }
    }

    @Override
    public void close() {
        if(null != producer) {
            producer.close();
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
}
