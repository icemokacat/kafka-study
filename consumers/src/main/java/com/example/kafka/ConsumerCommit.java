package com.example.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerCommit {

	static Logger log = LoggerFactory.getLogger(ConsumerCommit.class);

	public static void main(String[] args) {

		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG		, "192.168.64.50:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG	, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG				, "group_03");
		//props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG	, "6000");
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG		, "false");

		String topicName = "pizza-topic";

		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

		kafkaConsumer.subscribe(List.of(topicName));

		Thread mainThread = Thread.currentThread();

		// main thread 종료시 별도의 thread 로 kafkaConsumer wakeUp() 메소드를 호출하게 됨
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("Shutting down consumer thread and main program starts to exit by calling wakeup");
			kafkaConsumer.wakeup();
			try{
				mainThread.join();
			}catch(InterruptedException e){
				log.error(e.getMessage());
			}
		}));

		//pollAutoCommit(kafkaConsumer);
		//pollCommitSync(kafkaConsumer);
		pollCommitAsync(kafkaConsumer);

	}

	private static void pollCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
		int loopCnt = 0;

		try{
			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
				log.info("######## loopCnt: {}, ConsumerRecords Count : {}", loopCnt++, records.count());
				for (ConsumerRecord<String, String> record : records) {
					//log.info("Key: {}, Partition: {}, Offset: {}, Value: {} ", record.key(),  record.partition(), record.offset() ,record.value());
					log.info("Key: {}, Partition: {}, Offset: {}", record.key(),  record.partition(), record.offset() );
				}
				kafkaConsumer.commitAsync(new OffsetCommitCallback() {
					@Override
					public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
						// 비동기 commit 완료 후 callback 실행
						if(exception != null){
							// 오류 발생시 실행
							log.warn("############ Commit Async Failed ##############");
							log.error("offsets {} is not completed error: {}", offsets, exception.getMessage());
						}else{
							log.info("############ Commit Async Success offset {} ###############", offsets);
						}
					}
				});
			}
		}catch (WakeupException e){
			log.warn(e.getMessage());
		}catch (Exception e){
			log.error(e.getMessage());
		}finally {
			log.info("#### Commit sync before closing ####");
			kafkaConsumer.commitSync();
			log.info("#### Finally consumer is closing ####");
			kafkaConsumer.close();
		}

	}

	private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
		int loopCnt = 0;

		try{
			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
				log.info("######## loopCnt: {}, ConsumerRecords Count : {}", loopCnt++, records.count());
				for (ConsumerRecord<String, String> record : records) {
					//log.info("Key: {}, Partition: {}, Offset: {}, Value: {} ", record.key(),  record.partition(), record.offset() ,record.value());
					log.info("Key: {}, Partition: {}, Offset: {}", record.key(),  record.partition(), record.offset() );
				}
				try{
					if(records.count() > 0){
						kafkaConsumer.commitSync();
						log.info("######### commitSync have been called");
					}
				}catch (CommitFailedException e){
					log.info("############### CommitFailedException ###################");
					log.error(e.getMessage());
				}

			}
		}catch (WakeupException e){
			log.error(e.getMessage());
		}catch (Exception e){
			log.error(e.getMessage());
		}finally {
			log.info("finally consumer is closing");
			kafkaConsumer.close();
		}

	}
	public static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {
		int loopCnt = 0;

		try{
			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
				log.info("######## loopCnt: {}, ConsumerRecords Count : {}", loopCnt++, records.count());
				for (ConsumerRecord<String, String> record : records) {
					//log.info("Key: {}, Partition: {}, Offset: {}, Value: {} ", record.key(),  record.partition(), record.offset() ,record.value());
					log.info("Key: {}, Partition: {}, Offset: {}", record.key(),  record.partition(), record.offset() );
				}
				try {
					long millis = Duration.ofSeconds(10).toMillis();
					log.info("main thread is sleeping {} ms during while loop ", millis);
					Thread.sleep(millis);
				}catch(InterruptedException e){
					log.error(e.getMessage());
				}
			}
		}catch (WakeupException e){
			log.error(e.getMessage());
		}finally {
			log.info("finally consumer is closing");
			kafkaConsumer.close();
		}

	}
}
