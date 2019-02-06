import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

import static org.apache.kafka.streams.kstream.Consumed.with;

public class SampleStreams {
    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "SampleKafkaStreamsPlay";
    Properties properties;

    public SampleStreams() {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-play");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    }

    public KafkaStreams getDSL () {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, String> products = builder.stream(
                "test",
                Consumed.with(Serdes.Integer(), Serdes.String())
        );

        products.mapValues((value) -> {
            return value + "----------- transformed ----------";
        }).to("output", Produced.with(Serdes.Integer(), Serdes.String()));

        return new KafkaStreams (builder.build(), this.properties);
    }

}
