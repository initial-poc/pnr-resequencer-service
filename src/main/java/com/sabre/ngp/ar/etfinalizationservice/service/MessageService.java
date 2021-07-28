package com.sabre.ngp.ar.etfinalizationservice.service;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.sabre.ngp.ar.etfinalizationservice.component.MessagePublisher;
import com.sabre.ngp.ar.etfinalizationservice.domainmodel.PNRModel;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import com.sabre.ngp.ar.etfinalizationservice.repository.SpannerOutboxRepository;
import com.sabre.ngp.ar.etfinalizationservice.rule.TopicRule;
import com.sabre.ngp.ar.etfinalizationservice.util.OutboxRecordStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessageService {
    @Value("${threadCount}")
    private Integer maxThreadCount;
    private final SpannerOutboxRepository spannerOutboxRepository;
    private final MessagePublisher publisher;
    private final TopicRule topicRule;

    private final ThreadPoolExecutor threadPoolExecutor;

    public void handleMessage(List<OutboxEntity> entities)  {

        List<List<OutboxEntity>> subRecords=null;
        if(entities.size()>=maxThreadCount) {

            double ceil = Math.ceil((entities.size() +1)/ (maxThreadCount*1.0));

            subRecords = Lists.partition(entities, (int)ceil);
        }else{
            subRecords=List.of(entities);
        }
      //  LinkedList<List<OutboxEntity>> records= Lists.newLinkedList(subRecords);
        log.info("Number of chunks {} ",subRecords.size());

            log.info("queue size {} and active count {]",threadPoolExecutor.getQueue().remainingCapacity(),threadPoolExecutor.getActiveCount());
          subRecords.forEach( entity->threadPoolExecutor.execute(() ->doRelease(entity)));


    }



    private void doRelease(List<OutboxEntity> entities) {
        log.info("Thread -> {} Number of Records {}",Thread.currentThread().getName(),entities.size());
        Stopwatch doReleaseStopWatch = Stopwatch.createStarted();
        try {
          //  topicRule.processDestinations(model);
            log.info("Publishing message of size {}", entities.size());
            Stopwatch stopWatch = Stopwatch.createStarted();
            publisher.publishMessage(entities);
            stopWatch.stop();
            log.info("Published message time taken -> {} of size Message {}",  stopWatch,entities.size());
            spannerOutboxRepository.batchUpdate(entities, OutboxRecordStatus.COMPLETED);
        } catch (Exception ex) {
            log.info("exception occurred while publishing message Error-> {} and message ->  ", ex.getMessage());
            spannerOutboxRepository.batchUpdate(entities, OutboxRecordStatus.FAILED);
        }
        doReleaseStopWatch.stop();
log.info("Total Time Taken -> {} to process record {}",doReleaseStopWatch,entities.size());
    }
}
