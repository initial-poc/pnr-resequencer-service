package com.infogain.gcp.poc.poller.service;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Stopwatch;
import com.infogain.gcp.poc.poller.entity.GroupMessageStoreEntity;
import com.infogain.gcp.poc.poller.repository.SpannerGroupMessageStoreRepository;
import com.infogain.gcp.poc.poller.util.RecordStatus;
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
public class MessageGroupRecordProcessorService {

    private final SpannerGroupMessageStoreRepository groupMessageStoreRepository;
    private final String ip;

    //@Value(value = "${limit}")
    private int recordLimit=10;

    private static final String GRP_MSG_STORE_FAILED_SQL =
            "SELECT * FROM group_message_store WHERE STATUS =3 and retry_count<=3";
    private static final String GRP_MSG_STORE_STUCK_RECORD_SQL =
            "SELECT * FROM group_message_store WHERE STATUS in(1,2) and TIMESTAMP_DIFF(CURRENT_TIMESTAMP,updated, MINUTE)>5";

    @Autowired
    @SneakyThrows
    public MessageGroupRecordProcessorService(SpannerGroupMessageStoreRepository spannerGroupMessageStoreRepository) {
        this.groupMessageStoreRepository = spannerGroupMessageStoreRepository;
        ip = InetAddress.getLocalHost().getHostAddress();
    }

   public void processFailedRecords() {
        doProcessFailedRecords(getRecord(GRP_MSG_STORE_FAILED_SQL));
    }

    public void processStuckRecords() {
        doProcessStuckRecords(getRecord(GRP_MSG_STORE_STUCK_RECORD_SQL));
    }

    public void doProcessFailedRecords(List<GroupMessageStoreEntity> recordToProcess) {
        log.info("total record -> {} to process by application->  {}", recordToProcess.size(), ip);
        log.info("RECORD {}", recordToProcess);
        recordToProcess.stream().forEach(x->updateRecord(x, RecordStatus.IN_PROGRESS.getStatusCode()));
    }

    public void doProcessStuckRecords(List<GroupMessageStoreEntity> recordToProcess) {
        log.info("total record -> {} to process by application->  {}", recordToProcess.size(), ip);
        log.info("RECORD {}", recordToProcess);
        recordToProcess.stream().forEach(x->updateRecord(x, RecordStatus.FAILED.getStatusCode()));
    }

    private List<GroupMessageStoreEntity> getRecord(String sql) {
        log.info("Getting record to process by application->  {}", ip);
        Stopwatch stopWatch = Stopwatch.createStarted();
        SpannerOperations spannerTemplate = groupMessageStoreRepository.getSpannerTemplate();
        List<GroupMessageStoreEntity> recordToProcess = spannerTemplate.query(GroupMessageStoreEntity.class,
                Statement.of(String.format(sql, recordLimit)), null);
        stopWatch.stop();
        log.info("Total time taken to fetch the records {}", stopWatch);
        return recordToProcess;
    }

    private void updateRecord(GroupMessageStoreEntity entity, int status) {
        if(status==RecordStatus.FAILED.getStatusCode()) {
            entity.setRetry_count(entity.getRetry_count()+1);
        }
        entity.setStatus(status);
        entity.setUpdated(Timestamp.now());
        log.info("Going to update status for the record {}", entity);
        groupMessageStoreRepository.save(entity);
    }

}
