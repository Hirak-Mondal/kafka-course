package org.example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallBack {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
    public static void main(String[] args) {
        log.info("Starting Main");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
//        properties.put("batch.size", "400");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for(int j=0; j<10; j++) {
            for (int i = 0; i < 30; i++) {//only running this loop will snend all msg to one partition due to sticky partitioner
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world" + i);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //Executed every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            //record is successfully send
                            log.info("Record sent \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n");
                        } else {
                            log.error("Error message : " + e.getMessage());
                        }
                    }
                });
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        producer.flush();
        producer.close();


    }
}