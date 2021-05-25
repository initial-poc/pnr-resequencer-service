package com.infogain.gcp.poc.component;

import com.infogain.gcp.poc.poller.service.OutboxRecordProcessorService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalTime;

@Slf4j
@Component
public class OutboxPollerExecutor {

    @Autowired
    private OutboxRecordProcessorService pollerOutboxRecordProcessorService;

    @Scheduled(cron = "*/5 * * * * *")
    //@Scheduled(cron = "0 */5 * ? * *")
    public void process() {
        log.info("poller started at {}", LocalTime.now());
        pollerOutboxRecordProcessorService.processRecords();
        log.info("poller completed at {}", LocalTime.now());
    }

}
