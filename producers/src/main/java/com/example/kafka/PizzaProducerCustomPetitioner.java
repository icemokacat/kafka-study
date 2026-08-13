package com.example.kafka;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Faker;

public class PizzaProducerCustomPetitioner {
	private static final Logger log = LoggerFactory.getLogger(PizzaProducerCustomPetitioner.class);

	public static void sendPizzaMessage(
			KafkaProducer<String, String> kafkaProducer,
			String topicName,
			int iterCount,
			int interIntervalMillis,
			int intervalMillis,
			int intervalCount,
			boolean sync
	)
	{
		PizzaMessage pizzaMessage = new PizzaMessage();
		int iterSeq = 0;

		long seed = 2022;
		Random random = new Random(seed);
		Faker faker = Faker.instance(random);

		while ( iterSeq++ != iterCount){
			HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker,random, iterSeq);
			ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,
				pMessage.get("key"), pMessage.get("message"));
			// 메세지 전송
			sendMessage(kafkaProducer, producerRecord, pMessage, sync);

			if(intervalCount > 0 && (iterSeq % intervalCount == 0)){
				try {
					//log.info("######### IntervalCount: {} intervalMillis: {} #############", intervalCount,intervalMillis);
					Thread.sleep(intervalMillis);
				} catch (InterruptedException e) {
					log.error(e.getMessage());
				}
			}

			if(interIntervalMillis > 0){
				try {
					//log.info("######### IntervalIntervalMillis: {} #############", interIntervalMillis);
					Thread.sleep(intervalMillis);
				} catch (InterruptedException e) {
					log.error(e.getMessage());
				}
			}
		} // end while
	}

	/**
	 * sendMessage
	 * */
	public static void sendMessage(
		KafkaProducer<String, String> kafkaProducer,
		ProducerRecord<String, String> producerRecord,
		HashMap<String, String> pMessage, boolean sync
	)
	{
		if(!sync){
			// 비동기 (async)
			kafkaProducer.send(producerRecord,(metadata, exception) -> {
				if(exception == null){
					log.info("async(비동기) message:{} partition:{} offset:{}", pMessage.get("message"), metadata.partition(),
						metadata.offset());
				}else{
					log.error("exception : {}", exception.getMessage());
				}
			});
		}else{
			try {
				RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
				log.info("sync(동기) message:{} partition:{} offset:{}", pMessage.get("message"), metadata.partition(),
					metadata.offset());
			} catch (ExecutionException e) {
				log.error("exception : {}", e.getMessage());
			} catch (InterruptedException e) {
				log.error("InterruptedException : {}", e.getMessage());
			}
		}// end if
	}

	public static   void  main(String[] args) {
		String topicName = "pizza-topic-partitioner";

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

		//props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG		, "50000");
		//props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"6");
		//props.setProperty(ProducerConfig.ACKS_CONFIG,"0");
		// props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

		props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.kafka.CustomPartitioner");
		props.setProperty("custom.specialKey","P001");

		// KafkaProducer Object creation
		//KafkaProducer<String, String> kafkaProducer;
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

		try(kafkaProducer){
			sendPizzaMessage(kafkaProducer, topicName, -1, 100, 0, 0, true);
		}

	}

}
