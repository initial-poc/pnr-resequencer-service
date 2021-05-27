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

import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessagePublisher {

	private final PubSubTemplate pubSubTemplate;
	private final MessageConverter messageConverter;
	private static final String ORDERING_KEY = "ordering-key";
	private final CustomizedPubSubMessageConverter customizedPubSubMessageConverter;

	@Value("${app.topic.name}")
	private String topicName;
	
	public void publishMessage(PNREntity entity) {

		log.info("Generating pubsub message.");
		Map<String, String> dataMap = new HashMap<>();
		dataMap.put(ORDERING_KEY,entity.getPnrid());

		PubsubMessage pubsubMessage = customizedPubSubMessageConverter.toPubSubMessage(convertMessage(entity),dataMap);
		log.info("converted pubsubmessage : {}", pubsubMessage);

		log.info("Publising message.");
		pubSubTemplate.publish(topicName, pubsubMessage, Maps.newHashMap());
		log.info("published message {} to topic {}", pubsubMessage, topicName);
	}

	private String convertMessage(PNREntity entity){
		return  messageConverter.convert(entity);
		 
	}
}
