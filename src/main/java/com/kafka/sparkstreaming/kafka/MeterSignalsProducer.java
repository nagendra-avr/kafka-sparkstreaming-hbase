package com.kafka.sparkstreaming.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import com.kafka.sparkstreaming.kafka.IProducer;
import com.kafka.sparkstreaming.kafka.IProducerImpl;
import org.xembly.Directives;
import org.xembly.ImpossibleModificationException;
import org.xembly.Xembler;



import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Author; Nagendra
 */
public class MeterSignalsProducer {

	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println("Usage: <topicName> <messagesPerSec>");
			System.exit(1);
		}

		final int messagesPerSec = Integer.parseInt(args[1]);
		final String topic = args[0];

		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				int j = 1;
				while (j <= 10) {

					String times = new java.text.SimpleDateFormat(
							"dd/MM/yyyy HH:mm:ss:SSSSS")
							.format(new java.util.Date());
					System.out.println(times);
					int id = new Random().nextInt(messagesPerSec + 1);
					String meterId = "M" + String.format("%05d", id);

					int status = new Random().nextInt(2);
					String status_m = "";
					if (status == 1) {
						status_m = "ON";
					} else {
						status_m = "OFF";
					}

					String data = meterId+","+status_m + ","+ times;
					IProducer kafkaProducer = new IProducerImpl();
					kafkaProducer.send(data,topic);
					System.out.println(meterId+","+status_m + ","+ times);

					j = j + 1;

				}
			}

		};
		Timer timer = new Timer();

		timer.schedule(task,new	Date(),	1000);

	}
}
