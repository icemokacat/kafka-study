package com.moka.kafka.producer.parser;


import static org.assertj.core.api.AssertionsForClassTypes.*;

import org.junit.jupiter.api.Test;

import com.moka.kafka.producer.model.PizzaMessage;

class CsvPizzaParserTest {
	private final CsvPizzaParser parser = new CsvPizzaParser();

	@Test
	void parsesValidLineIntoPerson() {
		// Arrange
		String line = "E001,ord1, E001, Cheese Garlic Pizza, Deandra Jast, (515) 810-8846, 93316 Mike Stream, 2022-07-14 12:09:33";

		// Act
		PizzaMessage result = parser.parse(line);

		// Assert
		assertThat(result).isEqualTo(PizzaMessage.of("E001", "ord1, E001, Cheese Garlic Pizza, Deandra Jast, (515) 810-8846, 93316 Mike Stream, 2022-07-14 12:09:33"));
	}
}