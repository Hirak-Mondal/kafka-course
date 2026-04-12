package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikiMediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServer = "localhost:9092";
        String topic = "wikimedia.recent.changes";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(chain -> {
                    Request request = chain.request().newBuilder()
                            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                            .build();
                    return chain.proceed(request);
                })
                .build();
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url)).client(client);
        EventSource eventSource = builder.build();

        //start the producer in another thread
        eventSource.start();

        //produce for 10min and block the program until then
        TimeUnit.MINUTES.sleep(10);

    }
}
