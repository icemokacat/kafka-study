package com.moka.kafka.producer.model;

import java.util.Objects;

public record PizzaMessage(String key, String value) {

	public static PizzaMessage of(String key, String value) {
		return new PizzaMessage(key, value);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof PizzaMessage p))
			return false;
		return Objects.equals(key, p.key)
			&& Objects.equals(value, p.value);
	}

}
