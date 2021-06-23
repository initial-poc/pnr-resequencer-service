package com.infogain.gcp.poc.component;

import com.infogain.gcp.poc.poller.service.MessageGroupRecordProcessorService;
import com.infogain.gcp.poc.poller.service.OutboxRecordProcessorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPollerExecutor {

     private final OutboxRecordProcessorService pollerOutboxRecordProcessorService;

    public void process() {
       while(true) {
           log.info("poller started at {}", LocalTime.now());
           long nextPollerExecutionInterval = pollerOutboxRecordProcessorService.processRecords();
           log.info("poller completed at {}", LocalTime.now());
           try {
               Thread.sleep(nextPollerExecutionInterval);
           } catch (InterruptedException e) {
               e.printStackTrace();
           }
       }
   }


}
