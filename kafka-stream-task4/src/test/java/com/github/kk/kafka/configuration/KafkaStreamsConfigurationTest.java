package com.github.kk.kafka.configuration;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.github.kk.kafka.model.Developer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class KafkaStreamsConfigurationTest {

    private ListAppender<ILoggingEvent> listAppender;
    private Logger logger;

    private TopologyTestDriver testDriver;
    private TestInputTopic<Long, Developer> inputTopic;
    private TestOutputTopic<Long, Developer> outputTopic;

    private KafkaStreamsConfiguration ksc = new KafkaStreamsConfiguration();

    @Before
    public void setup() {
        Topology topology = createTopology(); // your topology
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaStreamsConfiguration.CustomSerde.class.getName());
        testDriver = new TopologyTestDriver(topology, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic("task4", new LongSerializer(), new DeveloperSerializer());
//        outputTopic = testDriver.createOutputTopic("output-topic", new LongDeserializer(), new DeveloperDeserializer());

        logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

        listAppender = new ListAppender<>();
        listAppender.start();

        logger.addAppender(listAppender);
    }

    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        ksc.myFirstStream(builder);
        return builder.build();
    }

    @After
    public void cleanup() {
        testDriver.close();
    }

    @Test
    public void shouldProcessSuccessfully() {
        // Given
        String expectedLog = "Stream peek {\"name\":\"Danny\",\"company\":\"EPAM\",\"position\":\"Lead\",\"experience\":15}";
        Developer developer = new Developer();
        developer.setCompany("EPAM");
        developer.setName("Danny");
        developer.setExperience(15);
        developer.setPosition("Lead");

        // When
        inputTopic.pipeInput(1L, developer);

        // Then
        assertEquals(1, listAppender.list.size());
        assertEquals(Level.INFO, listAppender.list.get(0).getLevel());
        assertEquals(expectedLog, listAppender.list.get(0).getFormattedMessage());
    }
}