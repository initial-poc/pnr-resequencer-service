package com.sabre.ngp.ar.etfinalizationservice.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.Map;

@Service
@Slf4j
public class OutboxRecordProcessorService {
    private final MessageService outboxStatusService;
    private final SpannerOutboxRepository spannerOutboxRepository;
    private final String ip;
    private static final long POLLER_WAIT_TIME_FOR_NEXT_INTERVAL_IN_MILI_SEC= 20;
    private static final long POLLER_IMMEDIATE_EXECUTION_INTERVAL_IN_MILI_SEC= 0;

    @Value("${batch.size.limit}")
    private Integer batchUpdateLimit;

    @Autowired
    @SneakyThrows
    public OutboxRecordProcessorService(MessageService outboxStatusService, SpannerOutboxRepository spannerOutboxRepository) {
        this.outboxStatusService = outboxStatusService;
        this.spannerOutboxRepository = spannerOutboxRepository;
        ip = InetAddress.getLocalHost().getHostAddress();
    }

    public long processRecords() throws Exception{
        Map<String,String>medata= Maps.newHashMap();
        List<OutboxEntity> outboxEntities = spannerOutboxRepository.getRecords(medata);
        if(outboxEntities.isEmpty()){
            log.info("=========   Record not found for processing =========");
            return POLLER_WAIT_TIME_FOR_NEXT_INTERVAL_IN_MILI_SEC;
        }
        doProcess(outboxEntities,medata);
        return POLLER_IMMEDIATE_EXECUTION_INTERVAL_IN_MILI_SEC;
    }


    private void doProcess(List<OutboxEntity> recordToProcess,Map<String,String>metadata) {
        log.info("Total record -> {} to process by application->  {}", recordToProcess.size(), ip);
        spannerOutboxRepository.batchUpdate(recordToProcess, OutboxRecordStatus.IN_PROGRESS,metadata);
        outboxStatusService.handleMessage(recordToProcess,metadata);
    }
}
