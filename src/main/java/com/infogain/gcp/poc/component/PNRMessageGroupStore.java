package com.infogain.gcp.poc.component;

import java.net.InetAddress;

import com.google.cloud.Timestamp;
import com.sun.tools.corba.se.idl.constExpr.Times;
import org.springframework.beans.factory.annotation.Autowired;
//github.com/initial-poc/pnr-resequencer-service.git
import org.springframework.stereotype.Component;

import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.entity.PNREntity;
import com.infogain.gcp.poc.poller.repository.GroupMessageStoreRepository;
import com.infogain.gcp.poc.util.RecordStatus;

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
	}

	public PNREntity addMessage(PNRModel pnrModel) {
		PNREntity pnrEntity = pnrModel.buildEntity();
		pnrEntity.setStatus(RecordStatus.IN_PROGRESS.getStatusCode());
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
			groupMessageStoreRepository.getSpannerTemplate().update(entity);
		 
	}

	public PNREntity getMessageById(PNRModel pnrModel) {
		PNREntity entity=null;
		entity=groupMessageStoreRepository.findByPnridAndMessageseq(pnrModel.getPnrid(),String.valueOf(pnrModel.getMessageseq()));
		log.info("record in DB {}",entity);
		return entity;
		
	}

}
