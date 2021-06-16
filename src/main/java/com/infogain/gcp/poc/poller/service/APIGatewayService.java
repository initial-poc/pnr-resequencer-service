package com.infogain.gcp.poc.poller.service;

import com.google.cloud.Timestamp;
import com.google.common.base.Stopwatch;
import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.poller.entity.OutboxEntity;
import com.infogain.gcp.poc.poller.repository.SpannerOutboxRepository;
import com.infogain.gcp.poc.poller.util.RecordStatus;
import com.infogain.gcp.poc.service.PNRSequencingService;
import com.infogain.gcp.poc.util.PNRModelUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
@Slf4j
@RequiredArgsConstructor
public class APIGatewayService {

    private final SpannerOutboxRepository outboxRepository;
    private final PNRSequencingService pnrSequencingService;

    private static final String SEPARATOR = ":";

    public void processRecord(OutboxEntity outboxEntity) {
        Stopwatch stopWatch = Stopwatch.createStarted();

        String[] playloadArray = PNRModelUtil.convert(outboxEntity.buildModel()).getPayload().split(SEPARATOR);
        for(String destination : playloadArray){
            doRelease( PNRModelUtil.convert(outboxEntity.buildModel(), destination));

        }
    }

    private void doRelease(PNRModel pnrRecord) {
        pnrSequencingService.processPNR(pnrRecord);
    }
}
