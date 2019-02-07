import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

import static org.apache.kafka.streams.kstream.Consumed.with;

public class SampleStreams {
    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "SampleKafkaStreamsPlay";
    Properties properties;
    private boolean alternate = true;

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

        KStream<Integer, String> updatedStream =  products.mapValues((value) -> {
            ObjectMapper objectMapper = new ObjectMapper();
            Product product = null;

            try {
                product = objectMapper.readValue(value, Product.class);
            } catch (Exception ex) {}

            // REST Call to SAM
            product.pp2ProductId = product.samProductId;
            System.out.println(product.samProductId);
            if (alternate) {
                product.error = "some exception occurred boss";
                alternate = false;
            } else {
                alternate = true;
            }

            String transformedStr = value;
            try {
                transformedStr = objectMapper.writeValueAsString(product);
            } catch (Exception ex) {
                System.out.println("error converting to string");
            }

            return transformedStr;
        });

        updatedStream.filter(new Predicate<Integer, String>() {
            @Override
            public boolean test(Integer integer, String s) {
                ObjectMapper mapper = new ObjectMapper();
                Product product = null;

                try {
                    product = mapper.readValue(s, Product.class);
                } catch (Exception ex) {
                }

                if (product.error.isEmpty()) {
                    return true;
                } else {
                    return false;
                }
            }
        }).to ("output", Produced.with(Serdes.Integer(), Serdes.String()));

        return new KafkaStreams (builder.build(), this.properties);
    }
}
