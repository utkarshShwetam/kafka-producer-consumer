package com.vibranium;

import com.vibranium.Consumer.KafkaConsumerService;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class KafkaConsumerMain {

    private final static Logger log = LoggerFactory.getLogger(KafkaConsumerMain.class.getSimpleName());
    private static Properties properties = null;

    public static RestHighLevelClient getOpenSearchClient(){
        log.info("OpenSearch client init started");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("", ""));

        String hostname = properties.getProperty("opensearch_hostname");
        int port = Integer.parseInt(properties.getProperty("opensearch_port"));
        String schema = properties.getProperty("opensearch_schema");

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, port, schema))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String,String> getKafkaConsumerClient(){

        log.info("Kafka Consumer client init started");

        String boostrapServers= properties.getProperty("kafkaBoostrapServers");
        String groupId = properties.getProperty("kafkaGroupId");

        // creating consumer configs
        Properties kafkaProperties = new Properties();

        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServers);
        kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.setProperty (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProperties.setProperty (ConsumerConfig.GROUP_ID_CONFIG,groupId);
        kafkaProperties.setProperty (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,properties.getProperty("AUTO_OFFSET_RESET_CONFIG"));
        kafkaProperties.setProperty (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,properties.getProperty("ENABLE_AUTO_COMMIT_CONFIG"));

        return new KafkaConsumer<>(kafkaProperties);

    }

    public static void main(String[] args) throws IOException {

        // init
        properties = new Properties();

        try {
            //load a properties file from class path, inside static method
            properties.load(KafkaConsumerMain.class.getClassLoader().getResourceAsStream("config.properties"));
        }
        catch (IOException ex) {
            log.error(ex.getMessage());
        }

        KafkaConsumerService kafkaConsumerService = new KafkaConsumerService();

        KafkaConsumer<String,String> consumer = getKafkaConsumerClient();

        // Referencing main thread
        final Thread mainThread = Thread.currentThread();

        // Adding shutdown hook
        Runtime.getRuntime().addShutdownHook( new Thread(()->{
            log.info("Detected a shutdown calling wakeup() method");
            consumer.wakeup();

            try{
                mainThread.join();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }));

        //Consume Message and send to OpenSearch
        kafkaConsumerService.consumeAndSendToOpenSearch(getOpenSearchClient(), consumer,properties.getProperty("topic"));

    }
}
