package com.sabre.ngp.ar.etfinalizationservice.component;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessagePublisher {
    private final Gson gson;



    @Value("${pubsubBatchSize}")
    private long pubsubBatchSize;

    @Value("${threadCount}")
    private Integer maxThreadCount;

   private final Publisher pubsubPublisher;

    public void publishMessage(List<OutboxEntity> entities) throws InterruptedException, IOException {

   //model.getDestinations().stream().forEach(topicName ->sendMessage(topicName,model) );
        sendMessage(null,entities);
    }



    private void sendMessage(String topicName,List<OutboxEntity> entities)  {
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();


        try{
        for(OutboxEntity entity: entities) {
            PubsubMessage pubsubMessage = getPubsubMessage(entity);
            ApiFuture<String> future = pubsubPublisher.publish(pubsubMessage);
            messageIdFutures.add(future);
        }
            } catch (Exception ex) {
                log.error("Exception occurred while sending message  Error Message  -> {} Error ->", ex.getMessage(),ex);

                throw new RuntimeException(ex.getMessage());
            } finally {
                try {
                   // List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();
                  // log.info("Published {} messages with batch settings.",messageIds.size());
                   if(pubsubPublisher!=null) {
                     //  pubsubPublisher.shutdown();
                       //pubsubPublisher.awaitTermination(1, TimeUnit.MINUTES);
                   }
                } catch (Exception ex) {
                    log.info("Exception occurred while shutdown the pubsub {}", ex.getMessage());
                    ex.printStackTrace();

                }
            }
        }


    private PubsubMessage getPubsubMessage(OutboxEntity entity) {
        ByteString data = ByteString.copyFromUtf8(convert(entity));
        return PubsubMessage.newBuilder()
                .setData(data)
              //  .setOrderingKey(entity.getLocator())
                .build();
    }


    public String convert(OutboxEntity entity) {
        String result = "";
        try {
            result = gson.toJson(entity);
        } catch (Exception e) {
            log.error("Exception while converting record to json {}", e);
        }
        return result;
    }




}
