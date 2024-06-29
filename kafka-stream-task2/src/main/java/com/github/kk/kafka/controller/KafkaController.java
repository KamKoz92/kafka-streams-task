package com.github.kk.kafka.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
class KafkaController {

    private KafkaTemplate<String, String> kafkaTemplate;

    public KafkaController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/send")
    public void send(@RequestBody String str) {
        sendMessage("task2", str);
    }

    public void sendMessage(String topic, String message) {
        kafkaTemplate.send(topic, message);
    }
}