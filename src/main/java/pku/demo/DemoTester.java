package pku.demo;
//13331031619
import pku.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yangxiao on 2017/11/14.
 * 这个程序演示了测评程序的基本逻辑
 * 正式的测评程序会比这个更复杂
 */
public class DemoTester {
    //每个pusher向每个topic发送的消息数目
    static int PUSH_COUNT = 1000;
    //发送消息的线程数
    static int PUSH_THREAD_COUNT = 10;
    //发送线程往n个topic发消息
    static int PUSH_TOPIC_COUNT = 2;
    //消费消息的线程数
    static int PULL_THREAD_COUNT = 10;
    //每个消费者消费的topic数量
    static int PULL_TOPIC_COUNT = 4;
    //topic数量
    static int TOPIC_COUNT = 20;
    //每个queue绑定的topic数量
    static int ATTACH_COUNT = 2;

    //统计push/pull消息的数量
    static AtomicInteger pushCount = new AtomicInteger();
    static AtomicInteger pullCount = new AtomicInteger();

    static class PushTester implements Runnable {
        //随机向以下topic发送消息
        List<String> topics = new ArrayList<>();
        Producer producer = new Producer();
        int id;

        PushTester(List<String> t, int id) {
            topics.addAll(t);
            this.id = id;
            StringBuilder sb=new StringBuilder();
            sb.append(String.format("producer%d push to:",id));
            for (int i = 0; i <t.size() ; i++) {
                sb.append(t.get(i)+" ");
            }
            System.out.println(sb.toString());
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < topics.size(); i++) {
                    String topic = topics.get(i);
                    for (int j = 0; j < PUSH_COUNT; j++) {
                        //topic加j作为数据部分
                        //j是序号, 在consumer中会用来校验顺序
                        byte[] data = (topic +" "+id + " " + j).getBytes();
                        ByteMessage msg = producer.createBytesMessageToTopic(topics.get(i), data);
                        //设置一个header
                        msg.putHeaders(MessageHeader.SEARCH_KEY, "hello");
                        //发送消息
                        producer.send(msg);
                        pushCount.incrementAndGet();
                        //System.out.println(pushCount);
                    }
                    producer.flush();
                }
                //producer.flush();
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    static class PullTester implements Runnable {
        //拉消息
        String queue;
        List<String> topics = new ArrayList<>();
        Consumer consumer = new Consumer();
        int pc=0;
        public PullTester(String s, ArrayList<String> tops) throws Exception {
            queue = s;
            topics.addAll(tops);
            consumer.attachQueue(s, tops);

            StringBuilder sb=new StringBuilder();
            sb.append(String.format("queue%s attach:",s));
            for (int i = 0; i <topics.size() ; i++) {
                sb.append(topics.get(i)+" ");
            }
            System.out.println(sb.toString());
        }

        @Override
        public void run() {

            try {
                //检查顺序, 保存每个topic-producer对应的序号, 新获得的序号必须严格+1
                HashMap<String, Integer> posTable = new HashMap<>();
                while (true) {
                    ByteMessage msg = consumer.poll();
                    //print_ms(msg);
                    if (msg == null) {
                        System.out.println(String.format("thread pull %s",pc));
                        return;
                    } else {
                        byte[] data = msg.getBody();
                        String str = new String(data);
                        //System.out.println(str);
                        String[] strs = str.split(" ");
                        String topic = strs[0];
                        String prod = strs[1];
                        int j = Integer.parseInt(strs[2]);
                        String mapkey=topic+" "+prod;
                        //System.out.println("j==="+j);
                        if (!posTable.containsKey(mapkey)) {
                            posTable.put(mapkey, 0);
                        }
                        if (j != posTable.get(mapkey)) {
//                            System.out.println(mapkey);
//                            System.out.println(posTable.get(mapkey));
                            System.out.println(String.format("数据错误 topic %s 序号:%d", topic, j));
                            //System.exit(0);
                        }
                        if (!msg.headers().getString(MessageHeader.SEARCH_KEY).equals("hello")) {

                            //System.out.println("head");
                            System.out.println(String.format("header错误 topic %s 序号:%d", topic, j));
                            //System.exit(0);
                        }
                        posTable.put(mapkey, posTable.get(mapkey) + 1);
                        pullCount.incrementAndGet();
//                        System.out.println("=================");
                        pc++;
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }

        private void print_ms(ByteMessage msg) {
            byte[] body = msg.getBody();
            KeyValue headers = msg.headers();
            HashMap<String, Object> map = headers.getMap();
            Set<String> strings = map.keySet();
            for (String key:strings) {
                System.out.println(key+":"+map.get(key)+body);
            }

        }
    }


    public static void main(String args[]) {
        try {
            //topic的名字是topic+序号的形式
            DemoMessageStore store = DemoMessageStore.store;
            System.out.println("开始push");
            long time1 = System.currentTimeMillis();
            Random rand = new Random(0);
            Random rand1 = new Random(0);
            ArrayList<Thread> pushers = new ArrayList<>();
            for (int i = 0; i < PUSH_THREAD_COUNT; i++) {
                //随机选择连续的topic
                ArrayList<String> tops = new ArrayList<>();
                int start = rand.nextInt(TOPIC_COUNT);
                for (int j = 0; j < PUSH_TOPIC_COUNT; j++) {
                    int v = (start+j)%TOPIC_COUNT;
                    tops.add("topic"+Integer.toString(v));
                }
                Thread t = new Thread(new PushTester(tops, i));
                t.start();
                pushers.add(t);
            }
            for (int i = 0; i < pushers.size(); i++) {
                pushers.get(i).join();
            }
            long time2 = System.currentTimeMillis();
            System.out.println(String.format("push 结束 time cost %d push count %d", time2 - time1, pushCount.get()));


//            byte[] byteHeaderLength = null;
//            byte[] headerContent = null;
//            byte[] byteBodyLength = null;
//            byte[] bodyContent = null;
//            String header = null;
//            FileInputStream fis = new FileInputStream("data/topic0");
//            BufferedInputStream bufferedInputStream = new BufferedInputStream(fis);
//
//            while(bufferedInputStream.read()!=-1){
//                byteHeaderLength = new byte[4];
//                bufferedInputStream.read(byteHeaderLength);
//                int intHeaderLength = byteArrayToInt(byteHeaderLength);
//                headerContent = new byte[intHeaderLength];
//                bufferedInputStream.read(headerContent);
//                header = new String(headerContent);
//                byteBodyLength = new byte[4];
//                bufferedInputStream.read(byteBodyLength);
//                int intBodyLength = byteArrayToInt(byteBodyLength);
//                bodyContent = new byte[intBodyLength];
//                bufferedInputStream.read(bodyContent);
//                DefaultKeyValue keyValue = makeKeyValue(header);
//                DefaultMessage message = new DefaultMessage(bodyContent);
//                message.setHeaders(keyValue);
//                print_ms(message);
//            }







            System.out.println("开始pull");
            int queue = 0;
            ArrayList<Thread> pullers = new ArrayList<>();
            for (int i = 0; i < PULL_THREAD_COUNT; i++) {
                //随机选择topic
                ArrayList<String> tops = new ArrayList<>();
                int start = rand1.nextInt(TOPIC_COUNT);
                for (int j = 0; j < PULL_TOPIC_COUNT; j++) {
                    int v =(start+j)%TOPIC_COUNT;
                    tops.add("topic"+Integer.toString(v));
                }
                Thread t = new Thread(new PullTester(Integer.toString(queue), tops));
                queue++;
                t.start();
                pullers.add(t);
            }
            for (int i = 0; i < pullers.size(); i++) {
                pullers.get(i).join();
            }
            long time3 = System.currentTimeMillis();
            System.out.println(String.format("pull 结束 time cost %d pull count %d", time3 - time2, pullCount.get()));


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static DefaultKeyValue makeKeyValue(String header) {
        String[] split = header.split(",");
        DefaultKeyValue keyValue = new DefaultKeyValue();
        for(int i=0;i<split.length;i++){
            keyValue.put(split[i],split[i+1]);
            i++;
        }
        return keyValue;
    }

    private static void print_ms(ByteMessage msg) {
        byte[] body = msg.getBody();
        KeyValue headers = msg.headers();
        HashMap<String, Object> map = headers.getMap();
        Set<String> strings = map.keySet();
        for (String key:strings) {
            System.out.println(key+":"+map.get(key)+body);
        }

    }
}
