package com.vibranium.Producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.TimeUnit;


public class KafkaProducerService {
    private final Logger log = LoggerFactory.getLogger(KafkaProducerService.class.getSimpleName());

    public void produceData(KafkaProducer<String,String> producer, String url, String topic) throws InterruptedException {
        log.info("Inside produceData");

        EventHandler eventHandler = new ProducerEventHandler(producer,topic);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        //start the producer in another thread
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);

        log.info("Exited produceData");
    }
}
