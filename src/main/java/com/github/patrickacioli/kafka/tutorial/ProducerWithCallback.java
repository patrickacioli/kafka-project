package com.github.patrickacioli.kafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

        // Producer Config
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            // Create a Record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world " + i);

            // Send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("[*] Sended msg, with metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()
                        );
                    } else {
                        logger.error("[!] Error while producer msg", e);
                    }

                }
            });

        }
        // Flush data
        // If you trying run without call this methods, data never sended to broker
        // because .send() is async and program finished first that this methid.
        producer.flush();
        producer.close();

    }

}
