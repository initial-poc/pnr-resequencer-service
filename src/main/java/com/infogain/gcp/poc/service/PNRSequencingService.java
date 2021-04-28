package com.infogain.gcp.poc.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.infogain.gcp.poc.component.PNRMessageGroupStore;
import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.entity.PNREntity;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class PNRSequencingService {
	
	private final PNRMessageGroupStore messageGroupStore;
	
	private final ReleaseStrategyService releaseStrategyService;

	//@Transactional(   rollbackFor = RuntimeException.class)
	public void processPNR(PNRModel pnrModel) {
		PNREntity pnrEntity = messageGroupStore.addMessage(pnrModel);
		List<PNREntity> toReleaseMessage = releaseStrategyService.release(pnrEntity);
		messageGroupStore.releaseMessage(toReleaseMessage);
		log.info("process completed");
	}
	
	
	
	
}
