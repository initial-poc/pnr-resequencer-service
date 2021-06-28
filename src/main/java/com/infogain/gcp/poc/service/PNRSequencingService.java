package com.infogain.gcp.poc.service;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.base.Stopwatch;
import org.springframework.stereotype.Service;

import com.infogain.gcp.poc.component.MessagePublisher;
import com.infogain.gcp.poc.component.PNRMessageGroupStore;
import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.entity.PNREntity;
import com.infogain.gcp.poc.util.GroupMessageRecordStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class PNRSequencingService {

    private final PNRMessageGroupStore messageGroupStore;
    private final MessagePublisher messagePublisher;

    private final ReleaseStrategyService releaseStrategyService;

    public void processPNR(PNRModel pnrModel) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        log.info("processing pnr {}",pnrModel);
        PNREntity pnrEntity = messageGroupStore.getMessageById(pnrModel);
        log.info("record fetched from group message store table {} {}",pnrModel,stopwatch);
        if (shouldProcess(pnrEntity)) {
            log.info("in shouldProcess method {}",pnrModel);
            pnrEntity = messageGroupStore.addMessage(pnrModel);
            List<PNREntity> toReleaseMessage = releaseStrategyService.release(pnrEntity);
log.info("got released messages {} {}",pnrEntity,stopwatch);
            if(toReleaseMessage!=null) {
                Stopwatch pubsubStopWatch = Stopwatch.createStarted();
                publishMessage(toReleaseMessage);
                log.info("Total time taken to publish message {}",pubsubStopWatch);
                pnrEntity.setStatus(GroupMessageRecordStatus.RELEASED.getStatusCode());
            } else {
                messageGroupStore.updateStatus(pnrEntity, GroupMessageRecordStatus.CREATED.getStatusCode());
            }
        } else {
            log.info("***** Duplicate Pnr Id ******: {}", pnrEntity.getPnrid());
        }
        log.info("processing done for pnr {} and total time {}",pnrModel,stopwatch);
    }

    private boolean shouldProcess(PNREntity pnrEntity) {
        return pnrEntity == null || pnrEntity.getStatus().equals(GroupMessageRecordStatus.FAILED.getStatusCode())
                || pnrEntity.getStatus().equals(GroupMessageRecordStatus.CREATED.getStatusCode());
    }

    private void publishMessage(List<PNREntity> toReleaseMessage) {
        toReleaseMessage.forEach(this:: sendMessage);


    }

    private void sendMessage(PNREntity entity) {
        messageGroupStore.updateStatus(entity, GroupMessageRecordStatus.RELEASED.getStatusCode());

        try {
            log.info("publishing message {} ",entity);
            messagePublisher.publishMessage(entity);
            log.info("published message {} ",entity);
            messageGroupStore.updateStatus(entity, GroupMessageRecordStatus.COMPLETED.getStatusCode());

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
            messageGroupStore.updateStatus(entity, GroupMessageRecordStatus.FAILED.getStatusCode());
        }
    }

}
