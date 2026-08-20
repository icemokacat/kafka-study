package com.moka.kafka.event;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moka.kafka.producer.model.PizzaMessage;
import com.moka.kafka.producer.parser.CsvPizzaParser;
import com.moka.kafka.producer.parser.LineParser;

public class FileEventSource implements Runnable {

	public boolean keepRunning = true;
	private final int updateInterval;
	private File file;
	private long filePointer = 0;
	private EventHandler eventHandler;
	LineParser<PizzaMessage> csvPizzaParser;

	public static Logger log = LoggerFactory.getLogger(FileEventSource.class);

	public FileEventSource(int updateInterval, File file, EventHandler eventHandler) {
		fileCheck(file);
		if(eventHandler == null){
			throw new NullPointerException("eventHandler is required");
		}
		if(updateInterval < 0){
			throw new IllegalArgumentException("updateInterval cannot be negative");
		}
		this.updateInterval = updateInterval;
		this.file = file;
		this.eventHandler = eventHandler;
		this.csvPizzaParser = new CsvPizzaParser();
	}

	private void fileCheck(File file){
		if(file == null || !file.exists()){
			throw new IllegalArgumentException("File does not exist");
		}
		if(!file.isFile()){
			throw new IllegalArgumentException("This File is not a file");
		}
		if(!file.canRead()){
			throw new IllegalArgumentException("This File is not readable");
		}
	}

	@Override
	public void run() {
		try {
			while (this.keepRunning) {
				Thread.sleep(this.updateInterval);
				// file의 크기를 계산
				long len = this.file.length();
				//
				if(len < this.filePointer) {
					log.info("file was reset as filePointer is longer than the file length");
					filePointer = len;
				}
				// 파일 내용이 추가 되었음을 감지
				else if(len > this.filePointer) {
					readAppendAndSend();
				}
			}
		}catch (InterruptedException e){
			log.error(e.getMessage());
		}catch (ExecutionException e){
			log.error(e.getMessage());
		}catch (Exception e){
			log.error(e.getMessage());
		}
	}

	private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
		// mode : r -> read
		RandomAccessFile raf = new RandomAccessFile(this.file, "r");
		// 파일포인터를 기준으로 찾는다
		raf.seek(this.filePointer);
		BufferedReader reader = new BufferedReader(
			new InputStreamReader(Channels.newInputStream(raf.getChannel()), StandardCharsets.UTF_8));
		String line;
		while ((line = reader.readLine()) != null) {
			sendMessage(line);
		}
		// file 이 변경되었으므로 file의 filePointer 를 현재 file의 마지막으로 재설정
		this.filePointer = raf.getFilePointer();
	}

	private void sendMessage(String line) throws ExecutionException, InterruptedException {
		PizzaMessage message = csvPizzaParser.parse(line);
		MessageEvent messageEvent = new MessageEvent(message.key(), message.value());
		this.eventHandler.onMessage(messageEvent);
	}
}
