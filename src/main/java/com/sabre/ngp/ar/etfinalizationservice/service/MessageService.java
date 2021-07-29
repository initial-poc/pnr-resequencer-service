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

    public void handleMessage(List<OutboxEntity> entities) {

        List<List<OutboxEntity>> subRecords = null;

      /*  if (entities.size() > maxThreadCount) {

            subRecords = Lists.partition(entities, pubsubBatchSize);
        } else {
            subRecords = List.of(entities);
        }*/
        log.info("queue size {} and active count {]", threadPoolExecutor.getQueue().remainingCapacity(), threadPoolExecutor.getActiveCount());
        int remainingCapacity = threadPoolExecutor.getQueue().remainingCapacity();
        double ceil = Math.ceil((entities.size() + 1) / remainingCapacity);
        subRecords=Lists.partition(entities,(int)ceil);
        log.info("Number of chunks {} ", subRecords.size());

        subRecords.forEach(entity -> threadPoolExecutor.execute(() -> doRelease(entity)));


    }





    private void doRelease(List<OutboxEntity> entities) {
        log.info("Thread -> {} Number of Records {}", Thread.currentThread().getName(), entities.size());
        Stopwatch doReleaseStopWatch = Stopwatch.createStarted();
        try {
            //  topicRule.processDestinations(model);
            Stopwatch stopWatch = Stopwatch.createStarted();
            publisher.publishMessage(entities);
            stopWatch.stop();
            log.info("Published message time taken -> {} of size Message {}", stopWatch, entities.size());
            spannerOutboxRepository.batchUpdate(entities, OutboxRecordStatus.COMPLETED);
        } catch (Exception ex) {
            log.info("exception occurred while publishing message Error-> {} and message ->  ", ex.getMessage());
            spannerOutboxRepository.batchUpdate(entities, OutboxRecordStatus.FAILED);
        }
        doReleaseStopWatch.stop();
        log.info("Total Time Taken -> {} to process record {}", doReleaseStopWatch, entities.size());
    }
}
