package com.infogain.gcp.poc.poller.service;

import com.google.common.base.Stopwatch;
import com.infogain.gcp.poc.component.MessagePublisher;
import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.entity.PNREntity;
import com.infogain.gcp.poc.poller.entity.OutboxEntity;
import com.infogain.gcp.poc.poller.repository.SpannerOutboxRepository;
import com.infogain.gcp.poc.poller.util.OutboxRecordStatus;
import com.infogain.gcp.poc.service.PNRSequencingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class APIGatewayService {

    private final SpannerOutboxRepository outboxRepository;
    private final PNRSequencingService pnrSequencingService;
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
        updateOutboxStatus( outboxEntity,OutboxRecordStatus.COMPLETED);
        log.info("total processing time {} for outboxEntity {}",stopWatch,outboxEntity);
    }

    private void doRelease(PNRModel pnrRecord) {
      //  pnrSequencingService.processPNR(pnrRecord);
        PNREntity pnrEntity = pnrRecord.buildEntity();
        try {
            publisher.publishMessage(pnrRecord.buildEntity());
        }catch(Exception ex){
            log.info("exception occurred while publishing message {} ",pnrEntity);
            log.error("Exception -> {} ",ex);
        }
    }

    private void updateOutboxStatus(OutboxEntity outboxEntity, OutboxRecordStatus outboxRecordStatus){
        outboxEntity.setStatus(outboxRecordStatus.getStatusCode());
        spannerOutboxRepository.save(outboxEntity);
    }
}
