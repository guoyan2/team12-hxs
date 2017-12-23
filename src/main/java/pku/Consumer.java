package pku;

import java.io.IOException;
import java.util.*;
import java.util.zip.DataFormatException;

/**
 * Created by yangxiao on 2017/11/14.
 */

public class Consumer {

    DemoMessageStore demoMessageStore = new DemoMessageStore();
    List<String> topics = new LinkedList<>();
    int readPos = 0;
    String queue;

    public void attachQueue(String queueName, Collection<String> t) throws Exception {
        if (queue != null) {
            throw new Exception("只允许绑定一次");
        }
        queue = queueName;
        topics.addAll(t);
    }

    public ByteMessage poll() throws IOException, DataFormatException {
        ByteMessage re = null;
        re = demoMessageStore.pull(topics.get(readPos));

        if(re==null){
            readPos++;
            if(readPos<topics.size()) return poll();
            return null;
        }else {
            return re;
        }

    }
}
