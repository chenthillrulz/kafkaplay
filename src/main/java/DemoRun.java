import org.apache.kafka.streams.KafkaStreams;

public class DemoRun {
    public static final String TOPIC = "test";
    public static void main(String[] args) {
        boolean isAsync = false;
        SampleProducer producerThread = new SampleProducer(TOPIC, isAsync);
        // start the producer
        producerThread.start();

        KafkaStreams kafkaStreams = new SampleStreams().getDSL();
        kafkaStreams.start();

        // Loop around
        while (true) {}
    }
}
