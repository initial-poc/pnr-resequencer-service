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

            subRecords = Lists.partition(entities, (entities.size() + 1) / maxThreadCount);
        }else{
            subRecords=List.of(entities);
        }
        log.info("Number of chunks {} ",subRecords.size());
        while(true){
          if(  threadPoolExecutor.getActiveCount()==0){
              log.info("Threads are available for processing records");
              subRecords.forEach( entity->threadPoolExecutor.execute(() ->publishRecord(entity)));
              break;
            }else{
              log.info("All threads are busy with task, waiting...");
          }
          try {
              TimeUnit.MILLISECONDS.sleep(200);
          }catch(Exception ex){
              log.error("error {}",ex.getMessage());
          }
        }


    }


    private  void publishRecord(List<OutboxEntity> entities){
        log.info("Thread -> {} Number of Records {}",Thread.currentThread().getName(),entities.size());
        entities.stream().forEach(this::doRelease);
    }

    private void doRelease(OutboxEntity entity) {
        Stopwatch doReleaseStopWatch = Stopwatch.createStarted();
        PNRModel model = entity.buildEntity();
        try {
            topicRule.processDestinations(model);
            log.info("Publishing message {}", model);
            Stopwatch stopWatch = Stopwatch.createStarted();
            publisher.publishMessage(model);
            stopWatch.stop();
            log.info("Published message time taken -> {} and Message {}",  stopWatch,model);
            updateOutboxStatus(entity, OutboxRecordStatus.COMPLETED);
        } catch (Exception ex) {
            log.info("exception occurred while publishing message Error-> {} and message ->  ", model,ex.getMessage());
            updateOutboxStatus(entity, OutboxRecordStatus.FAILED);
        }
        doReleaseStopWatch.stop();
log.info("Total Time Taken -> {} to process record {}",doReleaseStopWatch,entity);
    }

    private void updateOutboxStatus(OutboxEntity outboxEntity, OutboxRecordStatus outboxRecordStatus) {
        spannerOutboxRepository.update(outboxEntity,outboxRecordStatus);
    }
}
