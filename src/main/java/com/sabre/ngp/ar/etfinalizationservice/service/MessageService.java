package com.sabre.ngp.ar.etfinalizationservice.service;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.sabre.ngp.ar.etfinalizationservice.component.MessagePublisher;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import com.sabre.ngp.ar.etfinalizationservice.repository.SpannerOutboxRepository;
import com.sabre.ngp.ar.etfinalizationservice.rule.TopicRule;
import com.sabre.ngp.ar.etfinalizationservice.util.OutboxRecordStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessageService {
    @Value("${threadCount}")
    private Integer maxThreadCount;
    @Value("${pubsubBatchSize}")
    private int pubsubBatchSize;
    private final SpannerOutboxRepository spannerOutboxRepository;
    private final MessagePublisher publisher;
    private final TopicRule topicRule;

    private final ThreadPoolExecutor threadPoolExecutor;

    public void handleMessage(List<OutboxEntity> entities, Map<String, String> metadata) {

        List<List<OutboxEntity>> subRecords = null;

        if (entities.size() > maxThreadCount) {

            subRecords = Lists.partition(entities, 100);
        } else {
            subRecords = List.of(entities);
        }

      //  log.info("Number of chunks {}  and available queue size {}  and active thread {}", subRecords.size(),threadPoolExecutor.getQueue().remainingCapacity(),threadPoolExecutor.getActiveCount());
        subRecords.forEach(entity -> threadPoolExecutor.execute(() -> doRelease(entity,metadata)));
    }


    private void doRelease(List<OutboxEntity> entities, Map<String, String> metadata) {
       // log.info("Thread -> {} Number of Records {}", Thread.currentThread().getName(), entities.size());
        Stopwatch doReleaseStopWatch = Stopwatch.createStarted();
        try {
            //  topicRule.processDestinations(model);
            Stopwatch stopWatch = Stopwatch.createStarted();
            publisher.publishMessage(entities);
            stopWatch.stop();
            metadata.put("pubsub_time",stopWatch.toString());
          //  log.info("Published message time taken -> {} of size Message {}", stopWatch, entities.size());
            spannerOutboxRepository.batchUpdate(entities, OutboxRecordStatus.COMPLETED, metadata);
        } catch (Exception ex) {
         //   log.info("exception occurred while publishing message Error-> {} and message ->  ", ex.getMessage());
            spannerOutboxRepository.batchUpdate(entities, OutboxRecordStatus.FAILED, metadata);
        }
        doReleaseStopWatch.stop();
      //  log.info("Total Time Taken -> {} to process record {}", doReleaseStopWatch, entities.size());
    }
}
