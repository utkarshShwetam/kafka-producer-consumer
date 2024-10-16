package com.vibranium.Producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerEventHandler implements EventHandler {

    private final Logger log = LoggerFactory.getLogger(ProducerEventHandler.class.getSimpleName());
    private final KafkaProducer<String,String> kafkaProducer;
    private final String topic;

    public ProducerEventHandler(KafkaProducer<String,String> kafkaProducer, String topic) {
        this.kafkaProducer =kafkaProducer;
        this.topic=topic;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        // async send
        kafkaProducer.send(new ProducerRecord<>(topic,messageEvent.getData()));
        log.info("fetched event [%s]".formatted(messageEvent.getData().length()));
    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {
        log.error("Error occurred in Stream Reading");
    }
}
