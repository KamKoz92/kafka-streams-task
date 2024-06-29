package com.github.kk.kafka.configuration;

import com.github.kk.kafka.model.Developer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class DeveloperSerde implements Serde<Developer> {

    DeveloperSerializer serializer = new DeveloperSerializer();
    DeveloperDeserializer deserializer = new DeveloperDeserializer();

    @Override
    public Serializer<Developer> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Developer> deserializer() {
        return deserializer;
    }
}