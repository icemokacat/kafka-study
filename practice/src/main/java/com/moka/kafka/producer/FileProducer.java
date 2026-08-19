package com.moka.kafka.producer;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moka.kafka.producer.fileloader.PizzaFileLoader;
import com.moka.kafka.producer.model.PizzaMessage;

public class FileProducer {
	private static final Logger log = LoggerFactory.getLogger(FileProducer.class);

	public static   void  main(String[] args) throws InterruptedException {
		String topicName = "file-topic";

		Properties props = new Properties();

		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.50:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// KafkaProducer Object creation
		//KafkaProducer<String, String> kafkaProducer;
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

		// 파일에서 list 추출
		String filePath = "/Users/moka/study/kafka-study/practice/src/main/resources/pizza_sample.txt";
		PizzaFileLoader pizzaFileLoader = new PizzaFileLoader();
		List<PizzaMessage> pizzaMessageList = pizzaFileLoader.getPizzaMessages(filePath);
		log.info("Pizza Message List Loaded size: {}", pizzaMessageList.size());
		log.info("##########################################");

		// kafkaProducer 객체 생성 -> 레코드 생성 -> send 동기/비동기 -> close
		for (PizzaMessage pizzaMessage : pizzaMessageList) {
			sendMessage(kafkaProducer, topicName, pizzaMessage);
		}

		kafkaProducer.close();
	}

	private static void sendMessage(KafkaProducer<String, String> kafkaProducer, String topicName, PizzaMessage pizzaMessage) {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pizzaMessage.key(), pizzaMessage.value());
		log.info("key: {}, value: {}", pizzaMessage.key(), pizzaMessage.value());

		kafkaProducer.send(producerRecord, (metadata, exception) -> {
			if (exception != null) {
				// 에러
				log.error("Error sending message {}", exception.getMessage());
			}else{
				// send 후 정상 callback
				log.info("\n ########## record metadata received ####### \npartition:{}\noffset:{}\ntimestamp:{}",
					metadata.partition(), metadata.offset(), metadata.timestamp());
			}
		});
	}

}
