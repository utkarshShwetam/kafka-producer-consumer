package com.vibranium;

import com.vibranium.Producer.KafkaProducerService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class KafkaProducerMain {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerMain.class.getSimpleName());
    private static Properties properties = null;

    public static KafkaProducer<String,String> getKafkaProducerClient(){

        log.info("Kafka Producer client init started");

        String boostrapServers=properties.getProperty("kafkaBoostrapServers");

        // creating consumer configs
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);

    }

    public static void main(String[] args) throws InterruptedException {

        //init
        properties = new Properties();

        try{
            properties.load(KafkaProducerMain.class.getClassLoader().getResourceAsStream("config.properties"));
        }catch (IOException ex){
            log.error(ex.getMessage());
        }

        KafkaProducerService kafkaProducer = new KafkaProducerService();

        //Produce message
        kafkaProducer.produceData(getKafkaProducerClient(),properties.getProperty("url"),properties.getProperty("topic"));

    }
}
