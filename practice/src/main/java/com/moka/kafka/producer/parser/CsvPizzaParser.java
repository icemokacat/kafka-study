package com.moka.kafka.producer.parser;

import com.moka.kafka.producer.model.PizzaMessage;

public final class CsvPizzaParser implements LineParser<PizzaMessage> {

	private static final int EXPECTED_FIELD_COUNT = 2;

	public static final class InvalidTextDataException  extends RuntimeException {
		public InvalidTextDataException(String message) {
			super(message);
		}
	}

	@Override
	public PizzaMessage parse(String line) {
		String[] fields = line.split(",", -1);   // -1: trailing empty 필드 보존

		if (fields.length < EXPECTED_FIELD_COUNT) {
			throw new InvalidTextDataException(
				"expected " + EXPECTED_FIELD_COUNT + " fields but got " + fields.length + ": " + line);
		}

		String key = fields[0].trim();
		// fields[1] 부터 마지막 까지
		String[] remainder = new String[fields.length - 1];
		System.arraycopy(fields, 1, remainder, 0, remainder.length);
		String value = parseMessage(remainder);

		return new PizzaMessage(key, value);
	}

	private String parseMessage(String[] str){
		StringBuilder sb = new StringBuilder();
		for ( int i = 0; i < str.length; i++ ) {
			if(i != str.length-1){
				sb.append(str[i]).append(",");
			}else{
				sb.append(str[i]);
			}
		}
		return sb.toString();
	}

}
