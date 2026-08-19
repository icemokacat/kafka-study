package com.example.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerMTopicRebalance {

	static Logger log = LoggerFactory.getLogger(ConsumerMTopicRebalance.class);

	public static void main(String[] args) {

		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG		, "192.168.64.50:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG	, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		//props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG		, "earliest");

		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG				, "group-assign");
		props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
		//props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG		, "3");

		String topicName = "pizza-topic";

		try(KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props)){
			kafkaConsumer.subscribe(List.of("topic-p3-t1","topic-p3-t2"));

			Thread mainThread = Thread.currentThread();

			// main thread 종료시 별도의 thread 로 kafkaConsumer wakeUp() 메소드를 호출하게 됨
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				kafkaConsumer.wakeup();
				try{
					mainThread.join();
				}catch(InterruptedException e){
					log.error(e.getMessage());
				}
			}));

			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					log.info("topic {}, Key: {}, Partition: {}, Offset: {}, Value: {} ",record.topic(), record.key(),  record.partition(), record.offset() ,record.value());
				}
			}
		}catch (WakeupException e){
			log.error(e.getMessage());
		}

	}
}
