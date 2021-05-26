package com.infogain.gcp.poc.poller.service;

import com.google.cloud.Timestamp;
import com.google.common.base.Stopwatch;
import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.poller.entity.OutboxEntity;
import com.infogain.gcp.poc.poller.repository.SpannerOutboxRepository;
import com.infogain.gcp.poc.poller.sequencer.SequencerOps;
import com.infogain.gcp.poc.util.PNRModelUtil;
import com.infogain.gcp.poc.poller.util.RecordStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class APIGatewayService {

    private final SpannerOutboxRepository outboxRepository;
    private final SequencerOps sequencerOps;

    private static final String SEPARATOR = ":";
    private static final String SKIPPED = "skipped";

    public void processRecord(OutboxEntity outboxEntity) {
        Stopwatch stopWatch = Stopwatch.createStarted();
        if (outboxEntity.getStatus() == RecordStatus.IN_PROGRESS.getStatusCode()) {
            updateRecord(outboxEntity, RecordStatus.FAILED.getStatusCode());
            return;
        }

        updateRecord(outboxEntity, RecordStatus.IN_PROGRESS.getStatusCode());

        String[] playloadArray = PNRModelUtil.convert(outboxEntity.buildModel()).getPayload().split(SEPARATOR);
        log.info("total payloads recieved of size : {}", playloadArray.length);

        /*
        Check for how many destinations are present in payload based on that create multiple records
        and start processing all records separately, however currently we are processing them in parellel.
         */
        log.info("Processing all PNRs for outbox value : {} ", outboxEntity);
        Arrays.stream(playloadArray)
                .map(destination -> PNRModelUtil.convert(outboxEntity.buildModel(), destination))
                .forEach(pnrRecord -> doAsyncPNRProcess(outboxEntity,pnrRecord,stopWatch));
    }

    private void updateRecord(OutboxEntity entity, int status) {
        if (status == RecordStatus.FAILED.getStatusCode()) {
            entity.setRetry_count(entity.getRetry_count() + 1);
        }
        entity.setStatus(status);
        entity.setUpdated(Timestamp.now());
        log.info("Going to update status for the record {}", entity);
        outboxRepository.save(entity);
    }

    /*
    This method calls method of PNRSequencingService in async manner.
     */
    private void doAsyncPNRProcess(OutboxEntity outboxEntity, PNRModel pnrModel, Stopwatch stopWatch) {

        CompletableFuture.supplyAsync(() -> sequencerOps.callProcessPNR(pnrModel))
                .handle((res, exp) -> {

                    if(exp != null) {

                        postOperations(outboxEntity,stopWatch,RecordStatus.FAILED);
                        log.info("Exception thrown from callProcessPNR method. {}", exp.getMessage());
                    } else {
                        log.info("Got the response -> {}", res);

                        if(res.equals(SKIPPED)) {

                            log.info("This record has been skipped to marking this to 0 status again.");
                            postOperations(outboxEntity,stopWatch,RecordStatus.CREATED);
                        } else {
                            postOperations(outboxEntity,stopWatch,RecordStatus.COMPLETED);
                        }
                    }
                    return "";
                });
    }

    private void postOperations(OutboxEntity outboxEntity, Stopwatch stopWatch, RecordStatus recordStatus) {
        stopWatch.stop();
        outboxEntity.setProcessing_time_millis(stopWatch.elapsed(TimeUnit.MILLISECONDS));
        updateRecord(outboxEntity, recordStatus.getStatusCode());
    }

}
