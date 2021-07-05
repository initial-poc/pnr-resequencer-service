package com.sabre.ngp.ar.etfinalizationservice.service;

import com.google.common.base.Stopwatch;
import com.sabre.ngp.ar.etfinalizationservice.component.MessagePublisher;
import com.sabre.ngp.ar.etfinalizationservice.domainmodel.PNRModel;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import com.sabre.ngp.ar.etfinalizationservice.entity.PNREntity;
import com.sabre.ngp.ar.etfinalizationservice.repository.SpannerOutboxRepository;
import com.sabre.ngp.ar.etfinalizationservice.util.OutboxRecordStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class PNRService {

    private final SpannerOutboxRepository spannerOutboxRepository;
    private final MessagePublisher publisher;
    private static final String SEPARATOR = ":";


    public void processRecord(OutboxEntity outboxEntity) {
        log.info("processing started for outboxEntity {}",outboxEntity);
        Stopwatch stopWatch = Stopwatch.createStarted();


        String[] payloadArray =outboxEntity .getData().split(SEPARATOR);
        Arrays.stream(payloadArray).forEach(destination->doRelease(outboxEntity.buildModel(destination)));
        stopWatch.stop();
        outboxEntity.setProcessing_time_millis(stopWatch.elapsed(TimeUnit.MILLISECONDS));
        updateOutboxStatus( outboxEntity, OutboxRecordStatus.COMPLETED);
        log.info("processing done for outboxEntity {} and time taken {}",outboxEntity,stopWatch);
    }

    private void doRelease(PNRModel pnrRecord) {
        PNREntity pnrEntity = pnrRecord.buildEntity();
        try {
            log.info("Publishing message {}", pnrEntity);
            Stopwatch stopWatch = Stopwatch.createStarted();
            publisher.publishMessage(pnrRecord.buildEntity());
            stopWatch.stop();
            log.info("Published message {} and total time taken {}", pnrEntity, stopWatch);
        } catch (Exception ex) {
            log.info("exception occurred while publishing message {} ", pnrEntity);
            log.error("Exception -> {} ", ex);
        }
    }

    private void updateOutboxStatus(OutboxEntity outboxEntity, OutboxRecordStatus outboxRecordStatus){
        outboxEntity.setStatus(outboxRecordStatus.getStatusCode());
        spannerOutboxRepository.save(outboxEntity);
    }
}
