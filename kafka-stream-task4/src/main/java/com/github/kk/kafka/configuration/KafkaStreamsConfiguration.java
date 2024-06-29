package com.github.kk.kafka.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.kk.kafka.model.Developer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.StreamsConfig.*;

@Slf4j
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfiguration {

    ObjectMapper mapper = new ObjectMapper();

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public org.springframework.kafka.config.KafkaStreamsConfiguration kStreamsConfigs() {
        org.springframework.kafka.config.KafkaStreamsConfiguration testStreams = new org.springframework.kafka.config.KafkaStreamsConfiguration(Map.of(
                APPLICATION_ID_CONFIG, "testStreams",
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName(),
                DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomSerde.class.getName(),
                DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName()
        ));
        return testStreams;
    }

    @Bean
    public CustomSerde customSerde() {
        return new CustomSerde();
    }

    public static class CustomSerde extends Serdes.WrapperSerde<Developer> {
        public CustomSerde() {
            super(new DeveloperSerializer(), new DeveloperDeserializer());
        }
    }

    @Bean
    public KStream<Long, Developer> myFirstStream(StreamsBuilder kStreamBuilder) {
        KStream<Long, Developer> stream = kStreamBuilder.stream("task4");

        stream.filter((k, v) -> Objects.nonNull(v))
                .foreach((k, v) -> {
                    try {
                        log.info("Stream peek {}", mapper.writeValueAsString(v));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });

        return stream;
    }

    @Bean
    public NewTopic topic1() {
        return TopicBuilder.name("task4")
                .partitions(1)
                .replicas(1)
                .build();
    }

}