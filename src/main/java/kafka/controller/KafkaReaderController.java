package kafka.controller;

import com.google.protobuf.GeneratedMessageV3;
import com.incapsula.visit.gen.*;
import kafka.config.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import com.incapsula.visit.gen.VisitProtos.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


@Controller
@RestController
public class KafkaReaderController {

    public long processedSize;
    public long sesAccountId;
    private Action actionToUpdate;
    @Autowired
    private KafkaProducer kafkaProducer;


    @GetMapping("/tst")
    @Scheduled(cron = "*/1 * * * * *")
    public String tst() throws FileNotFoundException, InterruptedException {
        File file = new File("/Users/yossi.levi/Documents/incap_events_us_994_00000038_00011811_2021-05-11_1624.incapd");
        FileInputStream filestream = new FileInputStream(file);
        DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(filestream));

        while (true) {
            TimeUnit.MILLISECONDS.sleep(100L);
            int messageType = -1, len = -1;
            try {
                messageType = dataInputStream.readInt();
                len = dataInputStream.readInt();
                incrementFileAndBatchPosition(8);
                VisitProtos.MessageType messageTypeValue = VisitProtos.MessageType.forNumber(messageType);
                List<MsgDesc> msgDescs = buildMsgDesc(messageType, len, dataInputStream, (processedSize), sesAccountId);

            } catch (EOFException e) {
                break;
            } catch (Exception e) {

            }
        }


        return "Hi";
    }

    private void incrementFileAndBatchPosition(int size) {
        processedSize += size; // 4 bytes message type, 4 bytes message length
    }

    private List<MsgDesc> buildMsgDesc(int messageType, int len, DataInputStream dis, long messageOffset, long sesAccountId) throws IOException {
        MsgDesc desc = new MsgDesc();
        List<MsgDesc> descList = new ArrayList<>();
        desc.msgType = messageType;
        desc.len = len;
        desc.msgBytes = new byte[len];
        desc.localityKey = null;
        desc.timestamp = -1L; //default value
        int readBytes = dis.read(desc.msgBytes);
        VisitProtos.MessageType msgType = VisitProtos.MessageType.forNumber(messageType);

        try {
            switch (msgType) {
                case ACTION:
                case SUPPRESSED_ACTION:
                    VisitProtos.Action action = VisitProtos.Action.parseFrom(desc.msgBytes);
                    long siteId = action.getSiteID();
                    desc.msg = action;
                    desc.objectId = action.getSessionID();
                    desc.localityKey = action.getSessionID();
                    desc.extraInfo = "session id:" + action.getSessionID();
                    desc.dataLogId = siteId;
                    kafkaProducer.sendProtoBytesAsync(desc.msgBytes, "actions_new");
                    descList.add(desc);
                    return descList;
                case SESSION:
                    VisitProtos.Session session = VisitProtos.Session.parseFrom(desc.msgBytes);
                    desc.msg = session;
                    desc.objectId = session.getId();
                    desc.localityKey = session.getId();
                    desc.dataLogId = session.getSiteID();
                    descList.add(desc);
                    kafkaProducer.sendProtoBytesAsync(desc.msgBytes, "sessions");
                    return descList;
                case ACTION_UPDATE:
                    desc.objectId = actionToUpdate.getSessionID();
                    desc.localityKey = actionToUpdate.getSessionID();
                    desc.dataLogId = actionToUpdate.getSiteID();
                    descList.add(desc);
                    return descList;
                case UNIFIED_EVENT:
                    UnifiedManagement.UnifiedEvent unifiedEvent = UnifiedManagement.UnifiedEvent.parseFrom(desc.msgBytes);
                    desc.msg = unifiedEvent;
                    desc.objectId = sesAccountId;
                    desc.localityKey = desc.objectId;
                    desc.timestamp = unifiedEvent.getEventInfo().getCreateTime();
                    desc.dataLogId = desc.objectId;
                    descList.add(desc);
                    return descList;
                default:
                    System.out.println("I am default");
            }
        } catch (Exception e) {
        }

        return null;
    }


    public static class MsgDesc implements Cloneable {
        Long localityKey;
        long objectId;
        int msgType;
        int len;
        GeneratedMessageV3 msg;
        public byte[] msgBytes;
        long timestamp;
        String extraInfo = "";
        long dataLogId = -1;

        public MsgDesc clone() throws CloneNotSupportedException {
            return (MsgDesc) super.clone();
        }

    }


}
