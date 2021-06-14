package com.infogain.gcp.poc.service;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

        log.info("Inside processPNR() {}",pnrModel);

        PNREntity pnrEntity = messageGroupStore.getMessageById(pnrModel);
        if (shouldProcess(pnrEntity)) {
            pnrEntity = messageGroupStore.addMessage(pnrModel);
            Map<String,List<PNREntity>> toReleaseMessage = releaseStrategyService.release(pnrEntity);
            log.info("Data : {}", String.valueOf(toReleaseMessage));

            if(toReleaseMessage!=null) {
                publishMessage(toReleaseMessage);
                pnrEntity.setStatus(RecordStatus.RELEASED.getStatusCode());
            } else {
                messageGroupStore.updateStatus(pnrEntity, RecordStatus.CREATED.getStatusCode());
                return "skipped";
            }
        } else {
            log.info("***** Duplicate Record Start ******* ");
            log.info("***** Pnr Id ******: {}", pnrEntity.getPnrid());
            log.info("***** Duplicate Record End ******* ");
        }

        log.info("process completed");
        return RecordStatus.getStatusMessage(pnrEntity.getStatus()).toString();
    }

    private boolean shouldProcess(PNREntity pnrEntity) {
        return pnrEntity == null || pnrEntity.getStatus().equals(RecordStatus.FAILED.getStatusCode())
                || pnrEntity.getStatus().equals(RecordStatus.CREATED.getStatusCode());
    }

    private void publishMessage(Map<String,List<PNREntity>> toReleaseMessage) {

        Collection<List<PNREntity>> values = toReleaseMessage.values();
        for (List<PNREntity> l: values) {

            for (PNREntity entity : l) {
                long updatedRowCount = messageGroupStore.updateStatus(entity, RecordStatus.RELEASED.getStatusCode());
                if(updatedRowCount==0){
                    log.info("Got zero row count so returning");
                    return;
                }
                //  messageGroupStore.updateStatus(entity, RecordStatus.COMPLETED.getStatusCode());
                try {
                    messagePublisher.publishMessage(entity);
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                    messageGroupStore.updateStatus(entity, RecordStatus.FAILED.getStatusCode());
                }
                messageGroupStore.updateStatus(entity, RecordStatus.COMPLETED.getStatusCode());
            }
        }
    }

}
