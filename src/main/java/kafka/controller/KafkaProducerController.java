package kafka.controller;

import com.googlecode.protobuf.format.JsonFormat;
import com.googlecode.protobuf.format.ProtobufFormatter;
import kafka.config.KafkaProducer;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.MessageFormat;
import java.util.Random;
import java.util.UUID;


@Controller
@RequestMapping("/produce")
@RestController
public class KafkaProducerController {

    @Autowired
    KafkaProducer producer;

    @GetMapping
    //@Scheduled(cron = "*/3 * * * * *")
    public String produceData() {
        Random ran = new Random();
        Integer x = 1 + (int)(Math.random() * ((10 - 1) + 1));

        String s = RandomStringUtils.randomAlphanumeric(15);


        //producer.sendMessageAsnc(String.format("{\"session_id\": %s,\"data\": \"%s\"}",x, s), "sessions");
        return "Hi";
    }

    @GetMapping("/events")
   // @Scheduled(cron = "*/5 * * * * *")
    public String produceEvents() {
        Random ran = new Random();
        Integer x = 1 + (int)(Math.random() * ((10 - 1) + 1));

        String s = RandomStringUtils.randomAlphanumeric(10);


        //producer.sendMessageAsnc(String.format("{\"event_id\": %s,\"data\": \"%s\"}",x, s), "events");
        return "Hi";
    }
}
