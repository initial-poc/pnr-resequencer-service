package com.infogain.gcp.poc.service;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

        PNREntity pnrEntity = messageGroupStore.getMessageById(pnrModel);
        if (shouldProcess(pnrEntity)) {
            pnrEntity = messageGroupStore.addMessage(pnrModel);
            List<PNREntity> toReleaseMessage = releaseStrategyService.release(pnrEntity);
            log.info("Data : {}", String.valueOf(toReleaseMessage));

            if(toReleaseMessage!=null) {
                publishMessage(toReleaseMessage);
                pnrEntity.setStatus(GroupMessageRecordStatus.RELEASED.getStatusCode());
            } else {
                messageGroupStore.updateStatus(pnrEntity, GroupMessageRecordStatus.CREATED.getStatusCode());
            }
        } else {
            log.info("***** Duplicate Record Start ******* ");
            log.info("***** Pnr Id ******: {}", pnrEntity.getPnrid());
            log.info("***** Duplicate Record End ******* ");
        }
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
            messagePublisher.publishMessage(entity);
            messageGroupStore.updateStatus(entity, GroupMessageRecordStatus.COMPLETED.getStatusCode());
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
            messageGroupStore.updateStatus(entity, GroupMessageRecordStatus.FAILED.getStatusCode());
        }
    }

}
