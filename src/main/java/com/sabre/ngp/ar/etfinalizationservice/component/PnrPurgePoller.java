package com.sabre.ngp.ar.etfinalizationservice.component;

import com.sabre.ngp.ar.etfinalizationservice.repository.SpannerOutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PnrPurgePoller {
private final SpannerOutboxRepository spannerOutboxRepository;
   @Scheduled(fixedRate = 300000)
    public void purgePnr(){
      log.info("Purge Poller Started ....");
        spannerOutboxRepository.delete();
    }
}
