package com.moka.kafka.producer.parser;

public interface LineParser<T> {
	public T parse(String line);
}
