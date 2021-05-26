package com.infogain.gcp.poc.component;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.stereotype.Component;

import com.google.api.client.util.Maps;
import com.infogain.gcp.poc.entity.PNREntity;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessagePublisher {

	private final PubSubTemplate pubSubTemplate;
	private final MessageConverter messageConverter;
	
	@Value("${app.topic.name}")
	private String topicName;
	
	public void publishMessage(PNREntity entity) {

		log.info("Generating pubsub message.");
		PubsubMessage pubsubMessage = createPubSubMessage(entity);

		log.info("Publising message.");
		pubSubTemplate.publish(topicName, pubsubMessage, Maps.newHashMap());
		log.info("published message {} to topic {}", pubsubMessage, topicName);
	}

	private PubsubMessage createPubSubMessage(PNREntity entity) {
		return PubsubMessage.newBuilder()
				.setData(ByteString.copyFromUtf8(convertMessage(entity))).
						setOrderingKey(entity.getPnrid()).build();
	}
	
	private String convertMessage(PNREntity entity){
		return  messageConverter.convert(entity);
		 
	}
}
