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
import reactor.core.publisher.ConnectableFlux;

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

            ConnectableFlux<Object> flux = releaseStrategyService.release(pnrEntity);
            flux.subscribe(entity-> {
                publishMessage((PNREntity) entity);
            });
            flux.connect();
            // this below commented line can only happen once release strategy has finished work...
            // so we can't have it here below.
            //pnrEntity.setStatus(RecordStatus.RELEASED.getStatusCode());
        } else {
            log.info("record is already in db and its status is {}", pnrEntity.getStatus());
        }
        log.info("process completed");
        return RecordStatus.getStatusMessage(pnrEntity.getStatus()).toString();
    }

    private void publishMessage(PNREntity entity) {
        messageGroupStore.updateStatus((PNREntity) entity, RecordStatus.RELEASED.getStatusCode());
        messagePublisher.publishMessage((PNREntity)entity);
        messageGroupStore.updateStatus((PNREntity)entity, RecordStatus.COMPLETED.getStatusCode());
    }

}
