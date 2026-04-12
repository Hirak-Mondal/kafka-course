package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
    public static void main(String[] args) {
        log.info("Starting Consumer");
        String groupId = "java_application";
        String topic = "demo_java";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "earliest"); //latest = only from last offset, earliers = from begining
        //setting the assignor to cooperativeStickyAssignor
        properties.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //shutdown way
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutting down Consumer by calling wakeup method");
                consumer.wakeup();
                try{//join the main thread to allow the execution of the code in the main thread
                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });
        try {
            consumer.subscribe(Arrays.asList(topic)); //subscribe to topic
            while (true) {
//                log.info("polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key: " + record.key() + ", value: " + record.value());
                    log.info("partition: " + record.partition() + ", offset: " + record.offset());
                }
            }
        }catch(WakeupException e){
            log.info("Wakeup Exception");
        }catch (Exception e){
            log.error("Unexpected Exception");
        }finally {
            consumer.close(); //close the consumers, this also commits the offsets
        }

    }
}
