package com.vibranium.Consumer;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

public class KafkaConsumerService {

    private final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class.getSimpleName());

    public void consumeAndSendToOpenSearch(RestHighLevelClient openSearchRestClient,KafkaConsumer<String, String> consumer, String topic) throws IOException {

        log.info("Inside consumeAndSendToOpenSearch");

        // Create index if not exists
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
        try (openSearchRestClient; consumer) {
            // if already exists check
            boolean exists = openSearchRestClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!exists) {
                openSearchRestClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia index created successfully");
            } else
                log.info("Wikimedia index already existing");


            //Subscribe to kafka topic
            consumer.subscribe(Collections.singleton(topic));

            //Consume data from kafka
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                Integer recordCount = records.count();
                log.info("Records fetched [%s]".formatted(recordCount));

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {

                    String id = extractId(record.value());
                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), MediaTypeRegistry.JSON)
                            .id(id);

                    // Inserting one record at a time - Method 1
                    // IndexResponse indexResponse = openSearchRestClient.index(indexRequest, RequestOptions.DEFAULT);
                    // log.info("Inserted record into Opensearch with id: [%s]".formatted(indexResponse.getId()));

                    // adding in bulk - Method 2
                    bulkRequest.add(indexRequest);

                }

                // bulk insert handling for Method 2
                if (bulkRequest.numberOfActions() > 0) {

                    BulkResponse bulkItemResponses = openSearchRestClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted bulk data of [%s] record(s)".formatted(bulkItemResponses.getItems().length));

                    Thread.sleep(1000);

                    // manually committing offset after bulk batch processing
                    consumer.commitSync();
                    log.info("***************** Offset committed ********************");

                }


                // enable auto offset commit false
                // manually committing offset after batch processing
                // consumer.commitSync();
                // log.info("***************** Offset committed ********************");

            }
        } catch (IOException | InterruptedException | WakeupException e) {
            if (e instanceof WakeupException) {
                log.info("Wakeup called on consumer Shutting down");
            }
            log.error(e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            log.info("Shutting down consumer gracefully");
        }
    }

    //Getting out id from event streamed by producer to broker
    private String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}
