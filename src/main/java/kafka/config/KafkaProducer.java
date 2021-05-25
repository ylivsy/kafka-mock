package kafka.config;

import com.google.protobuf.GeneratedMessageV3;
import com.googlecode.protobuf.format.JsonFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class KafkaProducer {

//    @Autowired
//    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, byte[]> bytesTemplate;

//    public void sendMessage(String msg) {
//        kafkaTemplate.send("sessions", msg);
//    }
//
//    public void sendMessageAsnc(String message, String topic ) {
//
//        ListenableFuture<SendResult<String, String>> future =
//                kafkaTemplate.send(topic, message);
//
//        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
//
//            @Override
//            public void onSuccess(SendResult<String, String> result) {
//                System.out.println("Sent message=[" + message +
//                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
//            }
//            @Override
//            public void onFailure(Throwable ex) {
//                System.out.println("Unable to send message=["
//                        + message + "] due to : " + ex.getMessage());
//            }
//        });
//    }
//
//    public void sendProtoAsync(GeneratedMessageV3 message, String topic ) {
//        JsonFormat jsonFormat = new JsonFormat();
//        String kafkaMessage = jsonFormat.printToString(message);
//        ListenableFuture<SendResult<String, String>> future =
//                kafkaTemplate.send(topic, kafkaMessage);
//
//        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
//
//            @Override
//            public void onSuccess(SendResult<String, String> result) {
////                System.out.println("Sent message=[" + message +
////                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
//            }
//            @Override
//            public void onFailure(Throwable ex) {
//                System.out.println("Unable to send message=["
//                        + message + "] due to : " + ex.getMessage());
//            }
//        });
//    }

    public void sendProtoBytesAsync(byte[] message, String topic ) {
        ListenableFuture<SendResult<String, byte[]>> future =
                bytesTemplate.send(topic, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String,  byte[]>>() {

            @Override
            public void onSuccess(SendResult<String, byte[]> result) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=["
                        + message + "] due to : " + ex.getMessage());
            }
        });
    }
}
