package com.github.kk.kafka.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@Slf4j
@RestController
class KafkaController {

    private KafkaTemplate<String, String> kafkaTemplate;
    private Random r = new Random();

    public KafkaController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/send")
    public void send(@RequestBody String str) {
        String topicName;
        if (r.nextDouble() < 0.5) {
            topicName = "task3-1";
        } else {
            topicName = "task3-2";
        }
        sendMessage(topicName, str);
    }

    public void sendMessage(String topic, String message) {
        log.info("Sending {} to topic {}", message, topic);
        kafkaTemplate.send(topic, message);
    }
}