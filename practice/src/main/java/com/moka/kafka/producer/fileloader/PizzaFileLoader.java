package com.moka.kafka.producer.fileloader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moka.kafka.producer.model.PizzaMessage;
import com.moka.kafka.producer.parser.CsvPizzaParser;
import com.moka.kafka.producer.parser.LineParser;

public class PizzaFileLoader {

	private static final Logger log = LoggerFactory.getLogger(PizzaFileLoader.class);

	public List<PizzaMessage> getPizzaMessages(String filePath) {

		LineParser<PizzaMessage> csvPizzaParser = new CsvPizzaParser();
		List<PizzaMessage> pizzaMessages = new ArrayList<>();

		String line = "";
		try(FileReader fileReader = new FileReader(filePath)) {
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			while ( (line = bufferedReader.readLine()) != null ) {
				PizzaMessage pizzaMessage = csvPizzaParser.parse(line);
				pizzaMessages.add(pizzaMessage);
			}
		} catch (IOException e) {
			log.error(e.getMessage());
		}

		return pizzaMessages;
	}

}
