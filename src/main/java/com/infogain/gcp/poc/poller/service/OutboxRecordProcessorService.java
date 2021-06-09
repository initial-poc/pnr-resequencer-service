package com.infogain.gcp.poc.poller.service;

import com.google.cloud.spanner.Statement;
import com.google.common.base.Stopwatch;
import com.infogain.gcp.poc.poller.entity.OutboxEntity;
import com.infogain.gcp.poc.poller.repository.SpannerOutboxRepository;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

    //@Value(value = "${limit}")
    private static int recordLimit = 10000;

    private static final String OUTBOX_SQL = "SELECT * FROM OUTBOX WHERE STATUS =0 order by updated desc LIMIT %s";
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

    public void processRecords() {
        doProcess(getRecord(OUTBOX_SQL));
    }

    public void processFailedRecords() {
        doProcess(getRecord(GRP_MSG_STORE_FAILED_SQL));
    }

    private void doProcess(List<OutboxEntity> recordToProcess) {
        log.info("total record -> {} to process by application->  {}", recordToProcess.size(), ip);
        log.info("RECORD {}", recordToProcess);
        process(recordToProcess);
    }

    private List<OutboxEntity> getRecord(String sql) {
        log.info("Getting record to process by application->  {}", ip);
        Stopwatch stopWatch = Stopwatch.createStarted();
        SpannerOperations spannerTemplate = spannerOutboxRepository.getSpannerTemplate();
        List<OutboxEntity> recordToProcess = spannerTemplate.query(OutboxEntity.class,
                Statement.of(String.format(sql, recordLimit)), null);
        stopWatch.stop();
        log.info("Total time taken to fetch the records {}", stopWatch);
        return recordToProcess;

    }

    private void process(List<OutboxEntity> outboxEntities) {
        outboxEntities.stream().forEach(outboxStatusService::processRecord);
    }
}
