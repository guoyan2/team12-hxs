package pku;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * Created by yangxiao on 2017/11/14.
 */
public class Producer {

    public ByteMessage createBytesMessageToTopic(String topic, byte[] body) throws Exception {
        ByteMessage msg = new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC, topic);
        return msg;
    }

    public void send(ByteMessage defaultMessage) throws Exception {
        String topic = defaultMessage.headers().getString(MessageHeader.TOPIC);
        synchronized (DemoMessageStore.msgs) {
            DemoMessageStore.push(defaultMessage, topic);
        }
    }

    public void flush() throws Exception {
        synchronized (DemoMessageStore.msgs){
            DemoMessageStore.lastsave();
        }
    }

}
