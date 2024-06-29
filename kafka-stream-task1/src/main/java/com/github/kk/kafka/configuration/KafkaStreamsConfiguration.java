package com.github.kk.kafka.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.*;

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

    @Bean
    public KStream<Integer, String> myFirstStream(StreamsBuilder kStreamBuilder) {
        KStream<Integer, String> stream = kStreamBuilder.stream("task1-1");
        stream.map(processRecord()).to("task1-2");
        return stream;
    }

    private static KeyValueMapper<Integer, String, KeyValue<Integer, String>> processRecord() {
        return (key, value) -> new KeyValue<>(key, value + "test2");
    }

    @Bean
    public NewTopic topic1() {
        return TopicBuilder.name("task1-1")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topic2() {
        return TopicBuilder.name("task1-2")
                .partitions(1)
                .replicas(1)
                .build();
    }
}