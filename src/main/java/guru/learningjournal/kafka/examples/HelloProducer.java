package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        // Firast we need to do the configuration for the kafka producer
        //These are the minimum 4 for configuration which we need to do to  make the kafka producer
        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        // this will tell the or act as a client id for the brocker
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        //this configuration is to bootstrap the kafka brocker
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        // the format of the data require the key and the value
        // and the key and value need to serialized in order to seng them in the bytes over the network
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Secondlly we need to create the object of the kakfa producer
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        logger.info("Start sending messages...");
        for (int i = 1; i <= AppConfigs.numEvents; i++) {
            // Third we need to start sending the data
            producer.send(new ProducerRecord<>(AppConfigs.topicName, i, "Simple Message-" + i));
        }

        logger.info("Finished - Closing Kafka Producer.");
        // Lastly we need to close the connection of the kafka producer object
        producer.close();

    }
}
