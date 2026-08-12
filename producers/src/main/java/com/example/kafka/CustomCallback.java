package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {
	private static final Logger log  = LoggerFactory.getLogger(CustomCallback.class);

	private final int seq;

	public CustomCallback(int seq) {
		this.seq = seq;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if(exception != null){
			log.error("Error occurred while processing record", exception);
		}else{
			long offset = metadata.offset();
			long timestamp = metadata.timestamp();
			long partition = metadata.partition();
			String topic = metadata.topic();

			log.info("seq:{}, offset: {}, partition: {} topic: {} timestamp:{}",seq,offset,partition,topic,timestamp);
		}
	}
}
