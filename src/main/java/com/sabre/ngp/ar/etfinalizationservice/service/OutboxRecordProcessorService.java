package com.sabre.ngp.ar.etfinalizationservice.service;

import com.google.common.collect.Lists;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import com.sabre.ngp.ar.etfinalizationservice.repository.SpannerOutboxRepository;
import com.sabre.ngp.ar.etfinalizationservice.util.OutboxRecordStatus;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.util.List;

@Service
@Slf4j
public class OutboxRecordProcessorService {
    private final MessageService outboxStatusService;
    private final SpannerOutboxRepository spannerOutboxRepository;
    private final String ip;
    private static final long POLLER_WAIT_TIME_FOR_NEXT_INTERVAL_IN_MILI_SEC= 1000;
    private static final long POLLER_IMMEDIATE_EXECUTION_INTERVAL_IN_MILI_SEC= 20;

    @Value("${batch.size.limit}")
    private Integer batchUpdateLimit;

    @Autowired
    @SneakyThrows
    public OutboxRecordProcessorService(MessageService outboxStatusService, SpannerOutboxRepository spannerOutboxRepository) {
        this.outboxStatusService = outboxStatusService;
        this.spannerOutboxRepository = spannerOutboxRepository;
        ip = InetAddress.getLocalHost().getHostAddress();
    }

    public long processRecords() {
        List<OutboxEntity> outboxEntities = spannerOutboxRepository.getRecords();
        if(outboxEntities.isEmpty()){
            log.info("=========   Record not found for processing =========");
            return POLLER_IMMEDIATE_EXECUTION_INTERVAL_IN_MILI_SEC;
        }
        doProcess(outboxEntities);
        return POLLER_IMMEDIATE_EXECUTION_INTERVAL_IN_MILI_SEC;
    }


    private void doProcess(List<OutboxEntity> recordToProcess) {
        log.info("Total record -> {} to process by application->  {}", recordToProcess.size(), ip);
        batchUpdateRecords(recordToProcess);
        outboxStatusService.handleMessage(recordToProcess);
    }


private void batchUpdateRecords(List<OutboxEntity> recordToProcess){
    if(recordToProcess.size()>=batchUpdateLimit){
        List<List<OutboxEntity>> subSets = Lists.partition(recordToProcess, batchUpdateLimit);
        log.info("Record split the records {} ",subSets.size());
        subSets.stream().forEach(entities -> spannerOutboxRepository.batchUpdate(entities, OutboxRecordStatus.IN_PROGRESS));
    }else{
        spannerOutboxRepository.batchUpdate(recordToProcess, OutboxRecordStatus.IN_PROGRESS);
    }
}


}
