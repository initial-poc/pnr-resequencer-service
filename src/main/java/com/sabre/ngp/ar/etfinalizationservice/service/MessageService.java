package com.sabre.ngp.ar.etfinalizationservice.service;

import com.google.common.base.Stopwatch;
import com.sabre.ngp.ar.etfinalizationservice.component.MessagePublisher;
import com.sabre.ngp.ar.etfinalizationservice.domainmodel.PNRModel;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import com.sabre.ngp.ar.etfinalizationservice.repository.SpannerOutboxRepository;
import com.sabre.ngp.ar.etfinalizationservice.rule.TopicRule;
import com.sabre.ngp.ar.etfinalizationservice.util.OutboxRecordStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessageService {

    private final SpannerOutboxRepository spannerOutboxRepository;
    private final MessagePublisher publisher;
    private final TopicRule topicRule;

    private final Executor pollerThreadExecutor;

    public void handleMessage(List<OutboxEntity> entities){
        entities.forEach( entity->pollerThreadExecutor.execute(() ->doRelease(entity)));
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
