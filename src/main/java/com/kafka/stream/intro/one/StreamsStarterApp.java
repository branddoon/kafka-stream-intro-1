package com.kafka.stream.intro.one;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

  private static final Logger logger = LoggerFactory.getLogger(StreamsStarterApp.class);

  public static void main(String[] args) {

    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> wordCountInput = builder.stream("word-count-input");

    KTable<String, Long> wordCounts = wordCountInput
    .mapValues(textLine -> textLine.toLowerCase())
    .flatMapValues(lowerText -> Arrays.asList(lowerText.split(" ")))
    .selectKey((ignoredKey, value) -> value)
    .groupByKey()
    .count();

    wordCounts
    .toStream()
    .peek((key, value) -> logger.info("Word: {} | Count: {}", key, value))
    .to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

    KafkaStreams streams = new KafkaStreams(builder.build(), properties);
    streams.start();

    logger.info("Kafka Streams App started");

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Closing Kafka Streams...");
      streams.close();
      logger.info("Closed successfully");
    }));
  }
}
