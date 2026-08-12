package com.example.kafka;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerASyncWithKey {
	private static final Logger log = LoggerFactory.getLogger(ProducerASyncWithKey.class);

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
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// KafkaProducer Object creation
		//KafkaProducer<String, String> kafkaProducer;

		try(KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {
			// KafkaProducer message send
			for(int seq=0;seq < 20; seq++){
				ProducerRecord<String, String> dummy = createDummyRecord(topicName,seq);
				log.info("#####");
				log.info("seq: {}", seq);
				kafkaProducer.send(dummy,(metadata, exception) -> {
					if(exception == null){
						log.info("partition : {} offset:{}key:{}", metadata.partition(), metadata.offset(),
							metadata.timestamp());
					}
				});
			}


			kafkaProducer.flush();
		}

	}

	private static ProducerRecord<String, String> createDummyRecord(String topicName, int seq){
		String key = "id-"+seq;
		String value = "hello world" + seq;
		return new ProducerRecord<>(topicName,key,value);
	}
}
