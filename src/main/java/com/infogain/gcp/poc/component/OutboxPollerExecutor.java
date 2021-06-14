package com.infogain.gcp.poc.component;

import com.infogain.gcp.poc.poller.service.MessageGroupRecordProcessorService;
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

    @Autowired
    MessageGroupRecordProcessorService messageGroupRecordProcessorService;

   @Scheduled(fixedDelay = 1000)
   // @Scheduled(cron = "0 */5 * ? * *")
    public void process() {
        log.info("poller started at {}", LocalTime.now());
        pollerOutboxRecordProcessorService.processRecords();
        log.info("poller completed at {}", LocalTime.now());
    }

  //  @Scheduled(cron = "*/10 * * * * *")
    public void processFailedRecords() {
        log.info("Failed Record poller started at {}", LocalTime.now());
        messageGroupRecordProcessorService.processFailedRecords();
        log.info("Failed Record poller completed at {}", LocalTime.now());
    }

}
