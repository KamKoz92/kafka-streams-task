package com.github.kk.kafka.configuration;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class KafkaStreamsConfigurationTest {

    private ListAppender<ILoggingEvent> listAppender;
    private Logger logger;

    private TopologyTestDriver testDriver;
    private TestInputTopic<Integer, String> inputTopic;
    private TestOutputTopic<Integer, String> outputTopic;

    private KafkaStreamsConfiguration ksc = new KafkaStreamsConfiguration();

    @Before
    public void setup() {
        Topology topology = createTopology(); // your topology
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        testDriver = new TopologyTestDriver(topology, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic("task2", new IntegerSerializer(), new StringSerializer());
//        outputTopic = testDriver.createOutputTopic("output-topic", new LongDeserializer(), new DeveloperDeserializer());

        logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
    }

    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Map<String, KStream<Integer, String>> firstMap = ksc.myFirstStream(builder);
        Map<String, KStream<Integer, String>> secondMap = ksc.filterBean(firstMap);
        KStream<Integer, String> resultStream = ksc.mergeBean(secondMap);
        return builder.build();
    }

    @After
    public void cleanup() {
        testDriver.close();
    }

    @Test
    public void shouldProcessSuccessfully() {
        // Given
        String topicInput = "test kafka split eaglewoods obbligatos obediently";
        String[] split = topicInput.split("\\s+");
        List<String> beforeSplitLogs = Arrays.stream(split).map(this::formatBeforeSplit).toList();
        List<String> afterMergeLogs = Arrays.stream(split).filter(word -> word.contains("a")).map(this::formatAfterMerge).toList();

        // When
        inputTopic.pipeInput(1, topicInput);

        // Then
        assertEquals(9, listAppender.list.size());
        assertEquals(Level.INFO, listAppender.list.get(0).getLevel());

        List<String> logStrings = listAppender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        assertTrue(logStrings.containsAll(beforeSplitLogs));
        assertTrue(logStrings.containsAll(afterMergeLogs));
    }

    private String formatBeforeSplit(String word) {
        return String.format("Before splitting { \"key\":%d, \"value\":\"%s}\" }", word.length(), word);
    }

    private String formatAfterMerge(String word) {
        return String.format("After merge { \"key\":%d, \"value\":\"%s}\" }", word.length(), word);
    }
}