package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
    public static void main(String[] args) {
        log.info("Starting Main");
        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "localhost:9092");
//        properties.put("key.serializer", StringSerializer.class.getName());
//        properties.put("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put("batch.size", "400");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for(int j=0;  j<2; j++) {
            for (int i = 0; i < 10; i++) {//only running this loop will snend all msg to one partition due to sticky partitioner
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello World" + i;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            //record is successfully send
                            log.info("key : " + key + " | " + "Partition: " + metadata.partition() + "\n");
                        } else {
                            log.error("Error message : " + e.getMessage());
                        }
                    }
                });
            }
        }
        producer.flush();
        producer.close();


    }
}