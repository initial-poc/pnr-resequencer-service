package com.infogain.gcp.poc.util;

import com.google.api.core.ApiFutureCallback;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class PublisherUtil {

    public static Publisher getPublisher(String topicName) throws IOException {
        return Publisher.newBuilder(topicName)
                .setEnableMessageOrdering(true)
                .build();
    }

    public static ApiFutureCallback<String> getCallback(PubsubMessage pubsubMessage) {
        return new ApiFutureCallback<String>() {

            @Override
            public void onFailure(Throwable throwable) {
                if (throwable instanceof ApiException) {
                    ApiException apiException = ((ApiException) throwable);
                    log.error("Exception code : {}",String.valueOf(apiException.getStatusCode().getCode()));
                    log.error("Exception while publishing the message : {} ", apiException.getMessage());
                }
            }

            @Override
            public void onSuccess(String messageId) {
                log.info("Message has been successfully published : {} ",pubsubMessage.getData() + " : " + messageId);
            }
        };
    }
}
