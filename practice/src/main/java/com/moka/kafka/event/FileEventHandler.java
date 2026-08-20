package com.moka.kafka.event;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileEventHandler implements EventHandler {

	public static Logger log = LoggerFactory.getLogger(FileEventHandler.class);
	private final KafkaProducer<String, String> kafkaProducer;
	private final String topicName;
	private final boolean sync;

	public FileEventHandler(KafkaProducer<String, String> producer,String topicName, boolean sync) {
		if(producer == null){
			throw new IllegalArgumentException("producer is required");
		}
		if(StringUtils.isBlank(topicName)){
			throw new IllegalArgumentException("topicName can not be empty");
		}
		this.kafkaProducer = producer;
		this.topicName = topicName;
		this.sync = sync;
	}

	@Override
	public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
		ProducerRecord<String,String> producerRecord = new ProducerRecord<>(this.topicName, messageEvent.key,messageEvent.value);

		// 동기
		if(this.sync){
			RecordMetadata recordMetadata = this.kafkaProducer.send(producerRecord).get();
			log.info("### Record metadata received ###");
			log.info("partition {}",recordMetadata.partition());
			log.info("offset {}",recordMetadata.offset());
			log.info("topic: {}",recordMetadata.topic());
			log.info("timestamp: {}",recordMetadata.timestamp());
		}
		// 비동기
		else{
			this.kafkaProducer.send(producerRecord, (metadata, exception) -> {
				log.info("### Record metadata received ###");
				if (exception != null) {
					log.error("### Error getting record metadata ###", exception);
				}else{
					log.info("### Success ###");
					log.info("partition {}, offset {}, timestamp {}",metadata.partition(),metadata.offset(),metadata.hasTimestamp());
				}
			});
		}
	}// end on message

	//FileEventHandler가 제대로 생성되었는지 확인을 위해 직접 수행.
	public static void main(String[] args) throws Exception {
		String topicName = "file-topic";

		Properties props  = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.64.50:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
		boolean sync = true;

		FileEventHandler fileEventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
		MessageEvent messageEvent = new MessageEvent("key00001", "this is test message");
		fileEventHandler.onMessage(messageEvent);
	}

}
