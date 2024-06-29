package com.github.kk.kafka.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.StreamsConfig.*;

@Slf4j
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfiguration {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public org.springframework.kafka.config.KafkaStreamsConfiguration kStreamsConfigs() {
        return new org.springframework.kafka.config.KafkaStreamsConfiguration(Map.of(
                APPLICATION_ID_CONFIG, "testStreams",
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName(),
                DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName()
        ));
    }

    @Bean("firstMap")
    public Map<String, KStream<Integer, String>> myFirstStream(StreamsBuilder kStreamBuilder) {
        KStream<Integer, String> stream = kStreamBuilder.stream("task2");
        return stream.filter(this::notNull)
                .flatMapValues(value -> Arrays.asList(value.split("\\s+")))
                .selectKey((key, word) -> word.length())
                .peek((k, v) -> log.info("Before splitting { \"key\":{}, \"value\":\"{}}\" }", k, v))
                .split(Named.as("words-"))
                .branch((key, value) -> value.length() < 10, Branched.as("long"))
                .branch((key, value) -> value.length() > 9, Branched.as("short"))
                .defaultBranch();
    }

    @Bean("secondMap")
    public Map<String, KStream<Integer, String>> filterBean(@Qualifier("firstMap") Map<String, KStream<Integer, String>> branchedStream) {
        branchedStream.replaceAll((name, stream) -> stream.filter(
                (key, value) -> value.contains("a")));
        return branchedStream;
    }

    @Bean
    public KStream<Integer, String> mergeBean(@Qualifier("secondMap") Map<String, KStream<Integer, String>> branchedStream) {
        KStream<Integer, String> mergedStream = branchedStream.get("words-long")
                .merge(branchedStream.get("words-short"));

        mergedStream.foreach(((key, value) -> log.info("After merge { \"key\":{}, \"value\":\"{}}\" }", key, value)));
        return mergedStream;
    }

    private boolean notNull(Integer integer, String s) {
        return Objects.nonNull(s);
    }

    @Bean
    public NewTopic topic1() {
        return TopicBuilder.name("task2")
                .partitions(1)
                .replicas(1)
                .build();
    }
}