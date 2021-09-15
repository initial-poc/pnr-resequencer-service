package com.sabre.ngp.ar.etfinalizationservice.component;

import com.sabre.ngp.ar.etfinalizationservice.service.OutboxRecordProcessorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPollerExecutor {

     private final OutboxRecordProcessorService pollerOutboxRecordProcessorService;

    public void process() throws Exception {
       while(true) {
           log.info("Poller started at {}", LocalTime.now());
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
