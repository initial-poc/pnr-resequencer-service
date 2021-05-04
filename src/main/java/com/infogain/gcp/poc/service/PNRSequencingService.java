package com.infogain.gcp.poc.service;

import org.springframework.stereotype.Service;

import com.infogain.gcp.poc.component.MessagePublisher;
import com.infogain.gcp.poc.component.PNRMessageGroupStore;
import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.entity.PNREntity;
import com.infogain.gcp.poc.util.RecordStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class PNRSequencingService {

    private final PNRMessageGroupStore messageGroupStore;
    private final MessagePublisher messagePublisher;

    private final ReleaseStrategyService releaseStrategyService;

    public Mono<String> processPNR(PNRModel pnrModel) {
        Mono<PNREntity> pnrEntity = messageGroupStore.getMessageById(pnrModel);
        pnrEntity.subscribe(entity->doProcess(pnrModel, entity));
        return Mono.just("success");
    }

	private void doProcess(PNRModel pnrModel, PNREntity pnrEntity) {
		if (pnrEntity == null || pnrEntity.getStatus().equals(RecordStatus.FAILED.getStatusCode())) {
            pnrEntity = messageGroupStore.addMessage(pnrModel);

            ConnectableFlux<Object> flux = releaseStrategyService.release(pnrEntity);
            flux.subscribe(entity-> {
                publishMessage((PNREntity) entity);
            });
            flux.connect();
            
        } else {
            log.info("record is already in db and its status is {}", pnrEntity.getStatus());
        }
        log.info("process completed");
	}

    private void publishMessage(PNREntity entity) {
        messageGroupStore.updateStatus((PNREntity) entity, RecordStatus.RELEASED.getStatusCode());
        messagePublisher.publishMessage((PNREntity)entity);
        messageGroupStore.updateStatus((PNREntity)entity, RecordStatus.COMPLETED.getStatusCode());
    }

}
