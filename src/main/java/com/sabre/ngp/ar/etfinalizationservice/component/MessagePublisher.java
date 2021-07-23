package com.sabre.ngp.ar.etfinalizationservice.component;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.sabre.ngp.ar.etfinalizationservice.domainmodel.PNRModel;
import com.sabre.ngp.ar.etfinalizationservice.util.PublisherUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessagePublisher {

    private final Gson gson;

    public void publishMessage(PNRModel model) throws InterruptedException, IOException {

   model.getDestinations().stream().forEach(topicName ->sendMessage(topicName,model) );
    }

    private void sendMessage(String topicName,PNRModel model)  {
        topicName="projects/sab-ors-poc-sbx-01-9096/topics/itinerary-topic";
        PubsubMessage pubsubMessage = getPubsubMessage(model);
        Publisher publisher =null;

        try {
              publisher = PublisherUtil.getPublisher(topicName);
            ApiFuture<String> future = publisher.publish(pubsubMessage);
            String s = future.get();

        } catch (Exception ex){
            log.error("Exception occurred while sending message ->{} Error -> {}",model,ex.getMessage());
            throw new RuntimeException(ex.getMessage());
        }finally {
            try {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }catch (Exception ex){
                log.info("Exception occurred while shutdown the pubsub {}",ex.getMessage());
            }
        }
    }

    private PubsubMessage getPubsubMessage(PNRModel model) {
        String message = convert(model);
        ByteString data = ByteString.copyFromUtf8(message);
        return PubsubMessage.newBuilder().putAttributes("locator",model.getLocator())
                .setData(data)
                .setOrderingKey(model.getLocator())

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
