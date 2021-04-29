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

		PNREntity entity = messageGroupStore.getMessageById(pnrModel);
		if (entity == null || entity.getStatus().equals(RecordStatus.FAILED.getStatusCode())) {

			PNREntity pnrEntity = messageGroupStore.addMessage(pnrModel);
			List<PNREntity> toReleaseMessage = releaseStrategyService.release(pnrEntity);
			publishMessage(toReleaseMessage);
		} else {
			log.info("record is already in db and its status is {}",entity.getStatus());
		}
 
		log.info("process completed");
		return RecordStatus.getStatusMessage(entity.getStatus()).toString();
	}
	
	private void publishMessage(List<PNREntity> toReleaseMessage) {

		log.info("Going to update status for messages {}", toReleaseMessage);
		toReleaseMessage.stream().forEach(entity -> {
			messageGroupStore.updateStatus(entity,RecordStatus.RELEASED.getStatusCode());
			messagePublisher.publishMessage(entity);
			messageGroupStore.updateStatus(entity,RecordStatus.COMPLETED.getStatusCode());
			
		});
	
	}
 
}
