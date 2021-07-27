package com.sabre.ngp.ar.etfinalizationservice.component;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.sabre.ngp.ar.etfinalizationservice.domainmodel.PNRModel;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import com.sabre.ngp.ar.etfinalizationservice.util.PublisherUtility;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessagePublisher {
    private final Gson gson;
private final PublisherUtility publisherUtility;
private final Publisher pubsubPublisher;

    public void publishMessage(List<OutboxEntity> entities) throws InterruptedException, IOException {

   //model.getDestinations().stream().forEach(topicName ->sendMessage(topicName,model) );
        sendMessage(null,entities);
    }

    private void sendMessage(String topicName,List<OutboxEntity> entities)  {
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        Publisher publisher =null;
        try{

        for(OutboxEntity entity: entities) {
            PNRModel model = entity.buildEntity();
            PubsubMessage pubsubMessage = getPubsubMessage(model);
            ApiFuture<String> future = pubsubPublisher.publish(pubsubMessage);
            log.info("message added into publisher");
            messageIdFutures.add(future);
        }
            } catch (Exception ex) {
                log.error("Exception occurred while sending message  Error -> {}", ex.getMessage());
                throw new RuntimeException(ex.getMessage());
            } finally {
                try {
                    List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

                   log.info("Published {} messages with batch settings.",messageIds.size());
                   if(publisher!=null) {
                       publisher.shutdown();
                       publisher.awaitTermination(1, TimeUnit.MINUTES);
                   }
                } catch (Exception ex) {
                    log.info("Exception occurred while shutdown the pubsub {}", ex.getMessage());
                    ex.printStackTrace();

                }
            }
        }


    private PubsubMessage getPubsubMessage(PNRModel model) {
        String message = convert(model);
        ByteString data = ByteString.copyFromUtf8(message);
        return PubsubMessage.newBuilder().putAttributes("locator",model.getLocator())
                .setData(data)
              //  .setOrderingKey(model.getLocator())
                .build();
    }


    public String convert(PNRModel model) {
        String result = "";
        try {
            result = gson.toJson(model);
        } catch (Exception e) {
            log.error("Exception while converting record to json {}", e);
        }
        return result;
    }
}
