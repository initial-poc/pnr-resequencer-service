package com.infogain.gcp.poc.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.infogain.gcp.poc.component.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
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

	@Autowired
	private MessageConverter messageConverter;

	private final ReleaseStrategyService releaseStrategyService;

	//@Transactional(   rollbackFor = RuntimeException.class)
	public void processPNR(PNRModel pnrModel) {
		PNREntity pnrEntity = messageGroupStore.addMessage(pnrModel);
		List<PNREntity> toReleaseMessage = releaseStrategyService.release(pnrEntity);
		messageGroupStore.releaseMessage(toReleaseMessage);

		String messageJson = messageConverter.convert(pnrEntity);
		Map<String, String> attributes = new HashMap<>();
		messageGroupStore.publishMessage(messageJson, attributes);

		log.info("process completed");
	}
}
