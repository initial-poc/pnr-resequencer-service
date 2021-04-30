package com.infogain.gcp.poc.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.infogain.gcp.poc.component.MessagePublisher;
import com.infogain.gcp.poc.component.PNRMessageGroupStore;
import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.entity.PNREntity;
import com.infogain.gcp.poc.util.RecordStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class PNRSequencingService {

    private final PNRMessageGroupStore messageGroupStore;
    private final MessagePublisher messagePublisher;


    private final ReleaseStrategyService releaseStrategyService;

    public String processPNR(PNRModel pnrModel) {

        PNREntity pnrEntity = messageGroupStore.getMessageById(pnrModel);
        if (pnrEntity == null || pnrEntity.getStatus().equals(RecordStatus.FAILED.getStatusCode())) {
            pnrEntity = messageGroupStore.addMessage(pnrModel);
            List<PNREntity> toReleaseMessage = releaseStrategyService.release(pnrEntity);
            publishMessage(toReleaseMessage);
            if(pnrEntity.getPnrid().startsWith("STK")) {
                log.info("received STK record {}", pnrEntity.getPnrid());
                pnrEntity.setStatus(RecordStatus.IN_PROGRESS.getStatusCode());
            } else if(pnrEntity.getPnrid().startsWith("ERR")) {
                log.info("received ERR record {}", pnrEntity.getPnrid());
                pnrEntity.setStatus(RecordStatus.FAILED.getStatusCode());
                throw new IllegalStateException("Failed ERR record..." + pnrEntity.getPnrid());
            } else {
                pnrEntity.setStatus(RecordStatus.RELEASED.getStatusCode());
            }
        } else {
            log.info("record is already in db and its status is {}", pnrEntity.getStatus());
        }

        log.info("process completed");
        return RecordStatus.getStatusMessage(pnrEntity.getStatus()).toString();
    }

    private void publishMessage(List<PNREntity> toReleaseMessage) {

        log.info("Going to update status for messages {}", toReleaseMessage);
        toReleaseMessage.stream().forEach(entity -> {
            messageGroupStore.updateStatus(entity, RecordStatus.RELEASED.getStatusCode());
            messagePublisher.publishMessage(entity);
            messageGroupStore.updateStatus(entity, RecordStatus.COMPLETED.getStatusCode());

        });

    }

}
