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
        if (shouldProcess(pnrEntity)) {
            pnrEntity = messageGroupStore.addMessage(pnrModel);
            if(isMarkRecordToFailure(pnrEntity)) {
            	 pnrEntity.setStatus(RecordStatus.FAILED.getStatusCode());
            	 pnrEntity.setRetry_count(pnrEntity.getRetry_count()+1);
            	  messageGroupStore.updateStatus(pnrEntity, RecordStatus.FAILED.getStatusCode());
                 throw new IllegalStateException("Failed ERR record..." + pnrEntity.getPnrid());
            }
            
            List<PNREntity> toReleaseMessage = releaseStrategyService.release(pnrEntity);
            publishMessage(toReleaseMessage);
               
                pnrEntity.setStatus(RecordStatus.RELEASED.getStatusCode());
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

    private void publishMessage(List<PNREntity> toReleaseMessage) {

        log.info("Going to update status for messages {}", toReleaseMessage);
        toReleaseMessage.stream().forEach(entity -> {
            messageGroupStore.updateStatus(entity, RecordStatus.RELEASED.getStatusCode());
            messagePublisher.publishMessage(entity);
            messageGroupStore.updateStatus(entity, RecordStatus.COMPLETED.getStatusCode());

        });

    }
    
    
    private boolean isMarkRecordToFailure(PNREntity entity) {
    	boolean result=false;
    	
    	if(entity.getPnrid().equalsIgnoreCase("ERR001") && entity.getMessageseq().equals(1) && entity.getRetry_count()<3) {
    		result=true;
    	}
    	
    	return result;
    }

}
