package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class ProducerDemo {
    private static final Logger log = Logger.getLogger(ProducerDemo.class.getName());
    public static void main(String[] args) {
        log.info("Starting Main");
        //Create producer properties
        Properties properties = new Properties();
        //connect to local host
//        properties.put("bootstrap.servers", "172.20.85.172:9092");
        properties.put("bootstrap.servers", "localhost:9092");

        //set producer properties
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //craete producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("demo_java",  "hi");
        producer.send(record);//send data

        producer.flush();   // ADD THIS
        producer.close();//flush close producer


    }
}