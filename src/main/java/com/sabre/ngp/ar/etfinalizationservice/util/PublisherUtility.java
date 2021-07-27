package com.sabre.ngp.ar.etfinalizationservice.util;

import com.google.api.core.ApiFutureCallback;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
@RequiredArgsConstructor
public class PublisherUtility {

    private final BatchingSettings pubsubBatchSetting;
    public   Publisher getPublisher(String topicName) throws IOException {
        return Publisher.newBuilder(topicName)
                .setEndpoint("us-central1-pubsub.googleapis.com:443")
                .setEnableMessageOrdering(true).setBatchingSettings(pubsubBatchSetting)
                .build();
    }

    public   ApiFutureCallback<String> getCallback(PubsubMessage pubsubMessage) {
        return new ApiFutureCallback<String>() {

            @Override
            public void onFailure(Throwable throwable) {
                if (throwable instanceof ApiException) {
                    ApiException apiException = ((ApiException) throwable);
                    log.error("Exception while publishing the message : {} ", apiException.getMessage());


                }
            }

            @Override
            public void onSuccess(String messageId) {
                log.info("Message has been successfully published : {} ",pubsubMessage.getData());
            }
        };
    }
}
