package com.sabre.ngp.ar.etfinalizationservice.config;

import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
@Slf4j
public class AppConfig {

    @Value("${spanner.database.id}")
    private String databaseId;
    @Value("${spanner.instance.id}")
    private String instanceId;
    @Value("${spanner.project.id}")
    private String projectId;

    @Value("${threadCount}")
    private Integer maxThreadCount;

    @Bean("databaseClient")
    public DatabaseClient getDatabaseClient() {
        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();
        DatabaseClient dbClient = null;

        try {
            // Creates a database client
            dbClient =
                    spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

        } catch (Exception ex) {
            log.info("exception while building database client {}",ex.getMessage());
        }
        return dbClient;
    }

    @Bean
    public Gson gson() {
        return new Gson();
    }


    @Bean("threadPoolExecutor")
    public ThreadPoolExecutor  pollerThreadExecutor(){
        log.info("Creating thread pool of size -> {}",maxThreadCount);
        ThreadPoolExecutor threadPoolExecutor =   new ThreadPoolExecutor(maxThreadCount, maxThreadCount,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(maxThreadCount));
        return threadPoolExecutor;
    }

    @Bean("pubsubBatchSetting")
    public BatchingSettings PubSubBatchConfiguration(){
        long requestBytesThreshold = 5000L; // default : 1 byte
        long messageCountBatchSize = 20L; // default : 1 message

         Duration publishDelayThreshold = Duration.ofMillis(1000); // default : 1 ms

        // Publish request get triggered based on request size, messages count & time since last
        // publish, whichever condition is met first.
        return
                BatchingSettings.newBuilder()
                        .setElementCountThreshold(messageCountBatchSize)
                        .setRequestByteThreshold(requestBytesThreshold)
                        .setDelayThreshold(publishDelayThreshold)
                        .build();


    }

    @Bean("pubsubPublisher")
    public Publisher getPublisher(BatchingSettings pubsubBatchSetting) throws IOException {
        String topicName="projects/sab-ors-poc-sbx-01-9096/topics/itinerary-topic";
        return Publisher.newBuilder(topicName)
                .setEndpoint("us-central1-pubsub.googleapis.com:443")
                .setEnableMessageOrdering(true).setBatchingSettings(pubsubBatchSetting)
                .build();
    }
}
