package com.infogain.gcp.poc.component;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.stereotype.Component;

import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.entity.PNREntity;
import com.infogain.gcp.poc.poller.repository.GroupMessageStoreRepository;
import com.infogain.gcp.poc.util.AppConstant;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class PNRMessageGroupStore {
	private final GroupMessageStoreRepository groupMessageStoreRepository;
	private final String ip ;

	@Value("${app.topic.name}")
	private String topicName;

	@Autowired
	private PubSubTemplate pubSubTemplate;
	
	@Autowired
	@SneakyThrows
	public PNRMessageGroupStore(GroupMessageStoreRepository groupMessageStoreRepository) {
		super();
		this.groupMessageStoreRepository = groupMessageStoreRepository;
		ip= InetAddress.getLocalHost().getHostAddress();
	}

	public PNREntity addMessage(PNRModel pnrModel) {
		PNREntity pnrEntity = pnrModel.buildEntity();
		pnrEntity.setStatus(AppConstant.IN_PROGRESS);
		pnrEntity.setInstance(ip);
		log.info("saving message {}", pnrEntity);

		groupMessageStoreRepository.getSpannerTemplate().insert(pnrEntity);
		return pnrEntity;
	}

	public void releaseMessage(List<PNREntity> pnrEntity) {
		log.info("Going to update table as released for messages {}", pnrEntity);
		pnrEntity.stream().forEach(entity -> {
			entity.setStatus(AppConstant.RELEASED);
			entity.setInstance(ip);
			groupMessageStoreRepository.getSpannerTemplate().update(entity);
		});
	}

	public void publishMessage(String message, Map<String, String> attributes) {
		log.info("publishing message {} to topic {}", message, topicName);
		pubSubTemplate.publish(topicName, message, attributes);
		log.info("published message {} to topic {}", message, topicName);
	}

}
