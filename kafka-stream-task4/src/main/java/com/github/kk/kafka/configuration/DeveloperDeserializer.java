package com.github.kk.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.kk.kafka.model.Developer;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;


public class DeveloperDeserializer implements Deserializer<Developer> {

    ObjectMapper mapper = new ObjectMapper();

    public DeveloperDeserializer() {
    }

    @SneakyThrows
    @Override
    public Developer deserialize(String topic, byte[] bytes) {
        return mapper.readValue(bytes, Developer.class);
    }
}