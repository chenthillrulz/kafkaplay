import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
/**
 * Producer Example in Apache Kafka
 * @author www.tutorialkart.com
 */
public class SampleProducer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "SampleProducer";
    private String topic;
    private Boolean isAsync;
    Product product;

    public SampleProducer(String topic, Boolean isAsync) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(properties);
        this.topic = topic;
        this.isAsync = isAsync;
        ObjectMapper objectMapper = new ObjectMapper();
        File file = new File("C:\\Users\\VPalaC9\\IdeaProjects\\kafkaplay\\src\\main\\java\\sample.json");
        try {
            this.product = objectMapper.readValue(file, Product.class);
        } catch (Exception ex) {
            System.out.println("Unable to parse Json - " + ex);
        }
    }
    public void run() {
        int messageNo = 1;
        while (messageNo < 101) {
            String messageStr;
            product.samProductId =  Integer.toString(messageNo);
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                messageStr = objectMapper.writeValueAsString(product);
            } catch (Exception ex) {
                System.out.println("Unable to serialize product to string");
                return;
            }
//            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord<Integer, String>(topic,
                        messageNo,
                        messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously
                try {
                    producer.send(new ProducerRecord<Integer, String>(topic,
                            messageNo,
                            messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (Exception e) {
                    e.printStackTrace();
                    // handle the exception
                }
            }
            ++messageNo;
        }
    }
}

class DemoCallBack implements Callback {
    private final long startTime;
    private final int key;
    private final String message;
    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }
    /**
     * onCompletion method will be called when the record sent to the Kafka Server has been acknowledged.
     *
     * @param metadata  The metadata contains the partition and offset of the record. Null if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}