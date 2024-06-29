package com.github.kk.kafka.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.TopicBuilder;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.StreamsConfig.*;

@Slf4j
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfiguration {
    final JoinWindows oneMinuteWindowWithGrace = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30));;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public org.springframework.kafka.config.KafkaStreamsConfiguration kStreamsConfigs() {
        return new org.springframework.kafka.config.KafkaStreamsConfiguration(Map.of(
                APPLICATION_ID_CONFIG, "testStreams",
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName(),
                DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName()
        ));
    }

    @Bean
    public KStream<Long, String> myFirstStream(StreamsBuilder kStreamBuilder) {
        KStream<Long, String> stream1 = preprocessStream(kStreamBuilder.stream("task3-1"));
        KStream<Long, String> stream2 = preprocessStream(kStreamBuilder.stream("task3-2"));

        KStream<Long, String> join = stream1.join(stream2, (left, right) -> left + "<>" + right, oneMinuteWindowWithGrace);

        join.foreach((k, v) -> log.info("After join { \"key\":{}}, \"value\":\"{}}\" }", k, v));

        return join;
    }


    public KStream<Long, String> preprocessStream(KStream<Integer, String> stream) {
        return stream.filter(this::notNull)
                .filter(this::containsColon)
                .selectKey(mapKeyToLong())
                .peek((k, v) -> log.info("Before join { \"key\":{}}, \"value\":\"{}}\" }", k, v));
    }

    private static KeyValueMapper<Integer, String, Long> mapKeyToLong() {
        return (v, k) -> {
            String[] parts = k.split(":");
            return Long.parseLong(parts[0]);
        };
    }

    private boolean notNull(Integer integer, String s) {
        return Objects.nonNull(s);
    }

    private boolean containsColon(Integer integer, String s) {
        return s.contains(":");
    }

    @Bean
    public NewTopic topic1() {
        return TopicBuilder.name("task3-1")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topic2() {
        return TopicBuilder.name("task3-2")
                .partitions(1)
                .replicas(1)
                .build();
    }
}