package com.moka.kafka.producer.model;

import java.util.Objects;

public class PizzaMessage {
	public String key;
	public String value;

	public PizzaMessage(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public static PizzaMessage of(String key, String value) {
		return new PizzaMessage(key, value);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof PizzaMessage p)) return false;
		return Objects.equals(key, p.key)
			&& Objects.equals(value, p.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, value);
	}

}
