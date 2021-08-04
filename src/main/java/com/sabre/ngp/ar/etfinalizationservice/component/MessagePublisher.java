package com.sabre.ngp.ar.etfinalizationservice.component;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
