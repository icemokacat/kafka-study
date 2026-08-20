package com.moka.kafka.producer;

import java.io.File;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moka.kafka.event.EventHandler;
import com.moka.kafka.event.FileEventHandler;
import com.moka.kafka.event.FileEventSource;

public class FileAppendProducer {

	public static Logger log = LoggerFactory.getLogger(FileAppendProducer.class);

	public static void main(String[] args) {
		String topicName = "file-topic";

		Properties props = new Properties();

		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.50:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// KafkaProducer Object creation
		//KafkaProducer<String, String> kafkaProducer;
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

		// 파일에서 list 추출
		String filePath = "/Users/moka/study/kafka-study/practice/src/main/resources/pizza_append.txt";
		File file = new File(filePath);

		boolean sync = false;
		// ms
		int updateInterval = 1000;
		EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
		FileEventSource fileEventSource = new FileEventSource(updateInterval, file, eventHandler);

		Thread fileEventSourceThread = new Thread(fileEventSource);
		fileEventSourceThread.start();

		try {
			fileEventSourceThread.join();
		}catch (InterruptedException e) {
			log.error(e.getMessage());
		}finally {
			kafkaProducer.close();
		}
	}
}
