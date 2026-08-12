package com.example.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducer {
	private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);

	//KafkaProducer<String, String> producer;
	public static   void  main(String[] args) throws InterruptedException {
		String topicName = "simple-topic";

		Properties props = new Properties();

		/*
		 * broker set
		 * - bootstrap server
		 * - key.serializer.class
		 * - value.serializer.class
		 * */
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.50:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// KafkaProducer Object creation
		//KafkaProducer<String, String> kafkaProducer;

		try(KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props)) {
			log.info("Creating Producer");
			ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,"id-001","hello world");

			kafkaProducer.send(producerRecord);

			kafkaProducer.flush();
		}

	}
}
