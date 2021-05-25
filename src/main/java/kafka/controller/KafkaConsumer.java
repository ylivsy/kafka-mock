package kafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {
//    @KafkaListener(topics = "sessions", groupId = "1234")
//    public void listSessions(String message) {
//        ObjectMapper objectMapper  = new ObjectMapper();
//        System.out.println("Received Message in SESSIONS : " + message);
//    }

    @KafkaListener(topics = "actions_output", groupId = "8888")
    public void listActions(String message) {
        ObjectMapper objectMapper  = new ObjectMapper();
        System.out.println("Received Message in ACTIONS: " + message);
    }
}

