package com.infogain.gcp.poc.poller.service;

import com.google.cloud.spanner.Statement;
import com.google.common.base.Stopwatch;
import com.infogain.gcp.poc.poller.entity.OutboxEntity;
import com.infogain.gcp.poc.poller.repository.SpannerOutboxRepository;
import com.infogain.gcp.poc.poller.util.OutboxRecordStatus;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.data.spanner.core.SpannerOperations;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.util.List;

@Service
@Slf4j
public class OutboxRecordProcessorService {
    private final APIGatewayService outboxStatusService;
    private final SpannerOutboxRepository spannerOutboxRepository;
    private final String ip;
    private static final long POLLER_WAIT_TIME_FOR_NEXT_INTERVAL_IN_MILI_SEC= 2000;
    private static final long POLLER_IMMEDIATE_EXECUTION_INTERVAL_IN_MILI_SEC= 1;

    private static final String OUTBOX_SQL = "SELECT * FROM OUTBOX WHERE STATUS =0 order by updated desc limit 100";
    private static final String GRP_MSG_STORE_FAILED_SQL =
            "SELECT * FROM group_message_store WHERE STATUS =4 and retry_count<=3";
    private static final String OUTBOX_STUCK_RECORD_SQL =
            "SELECT * FROM OUTBOX WHERE STATUS =1 and TIMESTAMP_DIFF(CURRENT_TIMESTAMP,updated, MINUTE)>5";

    @Autowired
    @SneakyThrows
    public OutboxRecordProcessorService(APIGatewayService outboxStatusService, SpannerOutboxRepository spannerOutboxRepository) {
        this.outboxStatusService = outboxStatusService;
        this.spannerOutboxRepository = spannerOutboxRepository;
        ip = InetAddress.getLocalHost().getHostAddress();
    }

    public long processRecords() {
        List<OutboxEntity> outboxEntities = getRecord(OUTBOX_SQL);
        if(outboxEntities.isEmpty()){
            log.info("=========   Record not found for processing =========");
            return POLLER_WAIT_TIME_FOR_NEXT_INTERVAL_IN_MILI_SEC;
        }
        doProcess(outboxEntities);
        return POLLER_IMMEDIATE_EXECUTION_INTERVAL_IN_MILI_SEC;
    }

    public void processFailedRecords() {
        doProcess(getRecord(GRP_MSG_STORE_FAILED_SQL));
    }

    private void doProcess(List<OutboxEntity> recordToProcess) {
        log.info("Total record -> {} to process by application->  {}", recordToProcess.size(), ip);
        log.info("RECORD {}", recordToProcess);
        recordToProcess.forEach(outboxStatusService::processRecord);
    }

    private List<OutboxEntity> getRecord(String sql) {
        log.info("Getting record to process by application->  {}", ip);
        Stopwatch stopWatch = Stopwatch.createStarted();
        SpannerOperations spannerTemplate = spannerOutboxRepository.getSpannerTemplate();
        List<OutboxEntity> recordToProcess = spannerTemplate.query(OutboxEntity.class,
                Statement.of(sql), null);
        stopWatch.stop();
        log.info("Total time taken to fetch the records {}", stopWatch);
        log.info("Total record fetched from db {}",recordToProcess.size());
        return recordToProcess;

    }


}
