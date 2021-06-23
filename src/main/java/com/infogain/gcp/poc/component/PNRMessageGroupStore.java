package com.infogain.gcp.poc.component;

import java.net.InetAddress;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Statement;
import org.springframework.beans.factory.annotation.Autowired;
//github.com/initial-poc/pnr-resequencer-service.git
import org.springframework.stereotype.Component;

import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.entity.PNREntity;
import com.infogain.gcp.poc.poller.repository.GroupMessageStoreRepository;
import com.infogain.gcp.poc.util.GroupMessageRecordStatus;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class PNRMessageGroupStore {
	private final GroupMessageStoreRepository groupMessageStoreRepository;
	private final String ip ;

	
	@Autowired
	@SneakyThrows
	public PNRMessageGroupStore(GroupMessageStoreRepository groupMessageStoreRepository) {
		super();
		this.groupMessageStoreRepository = groupMessageStoreRepository;
		ip= InetAddress.getLocalHost().getHostAddress();
		//this.client=client;
	}

	public PNREntity addMessage(PNRModel pnrModel) {
		PNREntity pnrEntity = pnrModel.buildEntity();
		pnrEntity.setStatus(GroupMessageRecordStatus.IN_PROGRESS.getStatusCode());
		pnrEntity.setInstance(ip);
		pnrEntity.setUpdated(Timestamp.now());
		log.info("saving message {}", pnrEntity);
		groupMessageStoreRepository.save(pnrEntity);
		return pnrEntity;
	}

	public void updateStatus(PNREntity entity, int status) {
		log.info("Going to update the status in table as  {} for record ->{} ", status,entity);
			entity.setStatus(status);
			entity.setInstance(ip);
			entity.setUpdated(Timestamp.now());
		groupMessageStoreRepository.save(entity);
	}

	public PNREntity getMessageById(PNRModel pnrModel) {
		PNREntity	entity=groupMessageStoreRepository.findByPnridAndMessageseqAndDestination(pnrModel.getPnrid(),String.valueOf(pnrModel.getMessageseq()), pnrModel.getDestination());
		log.info("record in DB {}",entity);
		return entity;
		
	}

}
