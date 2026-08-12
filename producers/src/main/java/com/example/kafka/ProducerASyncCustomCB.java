package com.example.kafka;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerASyncCustomCB {
	private static final Logger log = LoggerFactory.getLogger(ProducerASyncCustomCB.class);

	public static   void  main(String[] args) throws InterruptedException {
		String topicName = "multipart-topic";

		Properties props = new Properties();

		/*
		 * broker set
		 * - bootstrap server
		 * - key.serializer.class
		 * - value.serializer.class
		 * */
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.50:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// KafkaProducer Object creation
		//KafkaProducer<String, String> kafkaProducer;

		try(KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props)) {
			// KafkaProducer message send
			for(int seq=0;seq < 20; seq++){
				ProducerRecord<Integer, String> dummy = createDummyRecord(topicName,seq);
				Callback callback = new CustomCallback(seq);

				log.info("seq: {}", seq);
				kafkaProducer.send(dummy,callback);
			}

			kafkaProducer.flush();
		}

	}

	private static ProducerRecord<Integer, String> createDummyRecord(String topicName, int seq){
		String value = "hello world" + seq;
		return new ProducerRecord<>(topicName,seq,value);
	}
}
