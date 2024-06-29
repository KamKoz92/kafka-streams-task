package com.github.kk.kafka.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.kk.kafka.model.Developer;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serializer;

public class DeveloperSerializer implements Serializer<Developer> {

    ObjectMapper mapper = new ObjectMapper();

    public DeveloperSerializer() {
    }

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, Developer data) {
        return mapper.writeValueAsBytes(data);
    }
}
