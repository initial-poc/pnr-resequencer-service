package com.sabre.ngp.ar.etfinalizationservice.component;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import com.sabre.ngp.ar.etfinalizationservice.util.OutboxRecordStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessagePublisher {
    private final Gson gson;
    @Resource(name = "pubsubPublisher")
    private final Map<String, Publisher> pubsubPublisher;
    private final ThreadPoolExecutor threadPoolExecutor;
    @Value("${pubsubBatchSize}")
    private long pubsubBatchSize;
    @Value("${threadCount}")
    private Integer maxThreadCount;

    public List<OutboxEntity> publishMessage(List<OutboxEntity> entities, Map<String, String> metadata) throws InterruptedException, IOException {
        return sendMessage(null, entities, metadata);
    }


    private List<OutboxEntity> sendMessage(String topicName, List<OutboxEntity> entities, Map<String, String> metadata) {
        Set<OutboxEntity> outboxEntitySet = new HashSet<>();
        Stopwatch stopWatch = Stopwatch.createStarted();

        try {
            for (OutboxEntity entity : entities) {
                CountDownLatch countDownLatch = new CountDownLatch(entity.getDestinations().size());

                PubsubMessage pubsubMessage = getPubsubMessage(entity);

                entity.getDestinations().forEach(destination -> threadPoolExecutor.execute(() -> {
                    ApiFuture<String> future = pubsubPublisher.get(destination).publish(pubsubMessage);
                    ApiFutures.addCallback(
                            future,
                            new ApiFutureCallback<String>() {

                                @Override
                                public void onFailure(Throwable throwable) {
                                    countDownLatch.countDown();
                                    outboxEntitySet.remove(entity);
                                    entity.setStatus(OutboxRecordStatus.FAILED.getStatusCode());
                                    outboxEntitySet.add(entity);

                                    if (throwable instanceof ApiException) {
                                        ApiException apiException = ((ApiException) throwable);
                                        log.error("Error publishing message for locator : {}, exception raised while publishing Message {}",
                                                entity.getLocator(), apiException.getStatusCode().getCode());
                                    }
                                }

                                @Override
                                public void onSuccess(String messageId) {
                                    // Once published, returns server-assigned message ids (unique within the topic)
                                    countDownLatch.countDown();
                                    entity.setStatus(OutboxRecordStatus.COMPLETED.getStatusCode());
                                    outboxEntitySet.add(entity);
                                    log.info("Published message ID: {} for locator {}", messageId, entity.getLocator());
                                }
                            },
                            MoreExecutors.directExecutor());
                }));
                countDownLatch.await();
            }
            stopWatch.stop();
            metadata.put("pubsub_time", stopWatch.toString());
            log.info("Published message time taken -> {} of size Message {}", stopWatch, entities.size());
        } catch (Exception ex) {
            log.error("Exception occurred while sending message  Error Message  -> {} Error ->", ex.getMessage(), ex);
            throw new RuntimeException(ex.getMessage());
        } finally {
            try {
                if (pubsubPublisher != null) {
                    //  pubsubPublisher.shutdown();
                    //pubsubPublisher.awaitTermination(1, TimeUnit.MINUTES);
                }
            } catch (Exception ex) {
                log.info("Exception occurred while shutdown the pubsub {}", ex.getMessage());
                ex.printStackTrace();
            }
        }

        return List.copyOf(outboxEntitySet);
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
