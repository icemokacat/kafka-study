package com.moka.kafka.consumer;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moka.kafka.consumer.model.SyncMode;

public class FileToDBConsumer<K extends Serializable, V extends Serializable> {
	public static final Logger logger = LoggerFactory.getLogger(FileToDBConsumer.class.getName());

	protected KafkaConsumer<K, V> kafkaConsumer;
	protected List<String> topics;
	private final OrderDBHandler orderDBHandler;

	public FileToDBConsumer(Properties consumerProps, List<String> topics,
		OrderDBHandler orderDBHandler) {
		this.kafkaConsumer = new KafkaConsumer<K, V>(consumerProps);
		this.topics = topics;
		this.orderDBHandler = orderDBHandler;
	}
	public void initConsumer() {
		this.kafkaConsumer.subscribe(this.topics);
		shutdownHookToRuntime(this.kafkaConsumer);
	}

	private void shutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {
		//main thread
		Thread mainThread = Thread.currentThread();

		//main thread 종료시 별도의 thread로 KafkaConsumer wakeup()메소드를 호출하게 함.
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				logger.info(" main program starts to exit by calling wakeup");
				kafkaConsumer.wakeup();

				try {
					mainThread.join();
				} catch(InterruptedException e) {
					logger.error(" main program interrupted",e);
				}
			}
		});

	}

	private void processRecord(ConsumerRecord<K, V> record) throws Exception {
		OrderDTO orderDTO = makeOrderDTO(record);
		this.orderDBHandler.insertOrder(orderDTO);
	}

	private OrderDTO makeOrderDTO(ConsumerRecord<K,V> record) throws Exception {
		String messageValue = (String)record.value();
		logger.info("###### messageValue:{}", messageValue);

		String[] tokens = messageValue.split(",");
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

		return new OrderDTO(tokens[0], tokens[1], tokens[2], tokens[3],
			tokens[4], tokens[5], LocalDateTime.parse(tokens[6].trim(), formatter));
	}

	private void processRecords(ConsumerRecords<K, V> records) throws Exception{
		List<OrderDTO> orders = makeOrders(records);
		this.orderDBHandler.insertOrders(orders);
	}

	private List<OrderDTO> makeOrders(ConsumerRecords<K,V> records) throws Exception {
		List<OrderDTO> orders = new ArrayList<>();
		for (ConsumerRecord<K,V> record : records) {
			OrderDTO orderDTO = makeOrderDTO(record);
			orders.add(orderDTO);
		}
		return orders;
	}

	public void pollConsumes(long durationMillis, SyncMode commitMode) {
		if (commitMode == SyncMode.SYNC) {
			pollCommitSync(durationMillis);
		} else {
			pollCommitAsync(durationMillis);
		}
	}
	private void pollCommitAsync(long durationMillis) {
		try {
			while (true) {
				ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
				logger.info("consumerRecords count:" + consumerRecords.count());
				if(consumerRecords.count() > 0) {
					//  processRecords 문제가 생겨도 commit 보냄
					try {
						processRecords(consumerRecords);
					} catch(Exception e) {
						logger.error(e.getMessage());
					}
				}
				this.kafkaConsumer.commitAsync((offsets, exception) -> {
					if (exception != null) {
						logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
					}
				});
			}
		}catch(WakeupException e) {
			logger.error("wakeup exception has been called");
		}catch(Exception e) {
			logger.error(e.getMessage());
		}finally {
			logger.info("##### commit sync before closing");
			kafkaConsumer.commitSync();
			logger.info("finally consumer is closing");
			close();
		}
	}

	protected void pollCommitSync(long durationMillis) {
		try {
			while (true) {
				ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
				// records 처리시 오류 발생시 commit 하지 않음
				processRecords(consumerRecords);
				try {
					if (consumerRecords.count() > 0) {
						this.kafkaConsumer.commitSync();
						logger.info("commit sync has been called");
					}
				} catch (CommitFailedException e) {
					logger.error(e.getMessage());
				}
			}
		}catch(WakeupException e) {
			logger.error("wakeup exception has been called");
		}catch(Exception e) {
			logger.error(e.getMessage());
		}finally {
			logger.info("##### commit sync before closing");
			kafkaConsumer.commitSync();
			logger.info("finally consumer is closing");
			close();
		}
	}
	protected void close() {
		this.kafkaConsumer.close();
		this.orderDBHandler.close();
	}

	public static void main(String[] args) {
		String topicName = "file-topic";

		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.50:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		String url = "jdbc:postgresql://192.168.64.50:5432/postgres";
		String user = "postgres";
		String password = "postgres";
		OrderDBHandler orderDBHandler = new OrderDBHandler(url, user, password);

		FileToDBConsumer<String, String> fileToDBConsumer = new
			FileToDBConsumer<String, String>(props, List.of(topicName), orderDBHandler);
		fileToDBConsumer.initConsumer();
		SyncMode commitMode = SyncMode.ASYNC;

		fileToDBConsumer.pollConsumes(100, commitMode);

	}
}
