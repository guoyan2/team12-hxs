package pku;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Created by yangxiao on 2017/11/14.
 * 这是一个消息队列的内存实现
 */
public class DemoMessageStore {

    //暂存数据集合
    static HashMap<String, ArrayList<ByteMessage>> msgs = new HashMap<>();
    //维护buffer流
    static HashMap<String, BufferedInputStream> bufferInput = new HashMap<>();
    //push的次数
    static AtomicInteger count = new AtomicInteger(0);
    //是否有data文件夹
    static boolean Is_Dir = false;

    static void push(ByteMessage msg, String topic) throws Exception {

        //第一次进入判断是否有data文件夹
        if (!Is_Dir) {
            File file = new File("data");
            file.mkdirs();
            Is_Dir = true;
        }

        if(count.get()>50000){
            save();
            msgs.clear();
            count.set(0);
        }

        //没有topic索引创建
        if (!msgs.containsKey(topic)) {
            msgs.put(topic, new ArrayList<>(10000));
        }
        //加入消息
        msgs.get(topic).add(msg);

        count.incrementAndGet();
    }

    static ByteMessage pull(String topic) throws IOException, DataFormatException {

        String toc = topic + Thread.currentThread().getName();
        if (!bufferInput.containsKey(toc)) {
            FileInputStream fis = new FileInputStream("data/" + topic);
            BufferedInputStream bis = new BufferedInputStream(fis);
            bufferInput.put(toc, bis);
        }
        BufferedInputStream bufferedInputStream = bufferInput.get(toc);

        byte[] byteHeaderLength = null;
        byte[] headerContent = null;
        byte[] byteBodyLength = null;
        byte[] bodyContent = null;
        String header = null;

        byteHeaderLength = new byte[4];
        int ret = bufferedInputStream.read(byteHeaderLength);
        int intHeaderLength = byteArrayToInt(byteHeaderLength);

        if (intHeaderLength == 0 || ret == -1) {
            bufferedInputStream.close();
            return null;
        }

        headerContent = new byte[intHeaderLength];
        bufferedInputStream.read(headerContent);
        header = new String(headerContent);

        byteBodyLength = new byte[4];
        bufferedInputStream.read(byteBodyLength);
        int intBodyLength = byteArrayToInt(byteBodyLength);

        if (intBodyLength == 0) {
            return null;
        }

        bodyContent = new byte[intBodyLength];
        bufferedInputStream.read(bodyContent);

        DefaultKeyValue keyValue = makeKeyValue(header);
        DefaultMessage message = new DefaultMessage(bodyContent);

        message.setHeaders(keyValue);
        return message;

    }

    private static DefaultKeyValue makeKeyValue(String header) {

        String[] split = header.split(",");

        if (split.length != 16) {
            return null;
        }

        DefaultKeyValue defaultKeyValue = new DefaultKeyValue();

        if (!split[0].equals("0"))
            defaultKeyValue.put(MessageHeader.MESSAGE_ID, split[0]);

        if (!split[1].equals("0"))
            defaultKeyValue.put(MessageHeader.TOPIC, split[1]);

        if (!split[2].equals("0"))
            defaultKeyValue.put(MessageHeader.BORN_TIMESTAMP, split[2]);

        if (!split[3].equals("0"))
            defaultKeyValue.put(MessageHeader.BORN_HOST, split[3]);

        if (!split[4].equals("0"))
            defaultKeyValue.put(MessageHeader.STORE_TIMESTAMP, split[4]);

        if (!split[5].equals("0"))
            defaultKeyValue.put(MessageHeader.STORE_HOST, split[5]);

        if (!split[6].equals("0"))
            defaultKeyValue.put(MessageHeader.START_TIME, split[6]);

        if (!split[7].equals("0"))
            defaultKeyValue.put(MessageHeader.STOP_TIME, split[7]);

        if (!split[8].equals("0"))
            defaultKeyValue.put(MessageHeader.TIMEOUT, split[8]);

        if (!split[9].equals("0"))
            defaultKeyValue.put(MessageHeader.PRIORITY, split[9]);

        if (!split[10].equals("0"))
            defaultKeyValue.put(MessageHeader.RELIABILITY, split[10]);

        if (!split[11].equals("0"))
            defaultKeyValue.put(MessageHeader.SEARCH_KEY, split[11]);

        if (!split[12].equals("0"))
            defaultKeyValue.put(MessageHeader.SCHEDULE_EXPRESSION, split[12]);

        if (!split[13].equals("0"))
            defaultKeyValue.put(MessageHeader.SHARDING_KEY, split[13]);

        if (!split[14].equals("0"))
            defaultKeyValue.put(MessageHeader.SHARDING_PARTITION, split[14]);

        if (!split[15].equals("0"))
            defaultKeyValue.put(MessageHeader.TRACE_ID, split[15]);

        return defaultKeyValue;

    }

    private static void save() throws Exception {

        FileOutputStream fos;
        BufferedOutputStream bos = null;

        for (String topic : msgs.keySet()) {

            fos = new FileOutputStream("data/" + topic, true);
            bos = new BufferedOutputStream(fos);


            ArrayList<ByteMessage> byteMessages = msgs.get(topic);
            for (ByteMessage message : byteMessages) {

                byte[] header = header(message.headers());
                byte[] headerLength = intToByteArray(header.length);
                byte[] body = message.getBody();
                byte[] bodyLength = intToByteArray(body.length);

                bos.write(headerLength);
                bos.write(header);
                bos.write(bodyLength);
                bos.write(body);

            }
            byteMessages = null;
            bos.flush();
            fos.close();
            bos.close();
        }


    }

    public static void clean(final Object buffer) throws Exception {
        AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                try {
                    Method getCleanerMethod = buffer.getClass().getMethod("cleaner",new Class[0]);
                    getCleanerMethod.setAccessible(true);
                    sun.misc.Cleaner cleaner =(sun.misc.Cleaner)getCleanerMethod.invoke(buffer,new Object[0]);
                    cleaner.clean();
                } catch(Exception e) {
                    e.printStackTrace();
                }
                return null;}});

    }


    private static int byteArrayToInt(byte[] b) {
        return b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    private static byte[] header(KeyValue headers) {

        Map<String, Object> map = headers.getMap();
        String result = String.valueOf(map.getOrDefault(MessageHeader.MESSAGE_ID, "0")) + "," +
                map.getOrDefault(MessageHeader.TOPIC, "0") + "," +
                map.getOrDefault(MessageHeader.BORN_TIMESTAMP, "0") + "," +
                map.getOrDefault(MessageHeader.BORN_HOST, "0") + "," +
                map.getOrDefault(MessageHeader.STORE_TIMESTAMP, "0") + "," +
                map.getOrDefault(MessageHeader.STORE_HOST, "0") + "," +
                map.getOrDefault(MessageHeader.START_TIME, "0") + "," +
                map.getOrDefault(MessageHeader.STOP_TIME, "0") + "," +
                map.getOrDefault(MessageHeader.TIMEOUT, "0") + "," +
                map.getOrDefault(MessageHeader.PRIORITY, "0") + "," +
                map.getOrDefault(MessageHeader.RELIABILITY, "0") + "," +
                map.getOrDefault(MessageHeader.SEARCH_KEY, "0") + "," +
                map.getOrDefault(MessageHeader.SCHEDULE_EXPRESSION, "0") + "," +
                map.getOrDefault(MessageHeader.SHARDING_KEY, "0") + "," +
                map.getOrDefault(MessageHeader.SHARDING_PARTITION, "0") + "," +
                map.getOrDefault(MessageHeader.TRACE_ID, "0");
        return result.getBytes();
    }

    private static byte[] intToByteArray(int a) throws IOException {
        byte[] b = new byte[]{
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
        return b;
    }

    //最后当push没有到达次数的时候要序列化
    public static void lastsave() throws Exception {
        save();
        msgs.clear();
        count.set(0);
    }

    public static byte[] compressByte(byte[] input) throws IOException {

        Deflater compressor = new Deflater();
        compressor.setLevel(Deflater.BEST_COMPRESSION);
        compressor.setInput(input);
        compressor.finish();

        ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);

        byte[] buf = new byte[1024];
        while (!compressor.finished()) {
            int count = compressor.deflate(buf);
            bos.write(buf, 0, count);
        }
        bos.close();
        byte[] compressedData = bos.toByteArray();
        return compressedData;

    }

    public static byte[] decompressByte(byte[] compressedData) throws DataFormatException, IOException {
        Inflater decompressor = new Inflater();
        decompressor.setInput(compressedData);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(compressedData.length);
        byte[] buf = new byte[1024];
        while (!decompressor.finished()) {
            int count = decompressor.inflate(buf);
            bos.write(buf, 0, count);

        }
        bos.close();
        byte[] decompressedData = bos.toByteArray();
        return decompressedData;
    }
}
