package com.infogain.gcp.poc.component;

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

		String message = convertMessage(entity);
		pubSubTemplate.publish(topicName, message, Maps.newHashMap());
		log.info("published message {} to topic {}", message, topicName);
	}

	
	private String convertMessage(PNREntity entity){
		return  messageConverter.convert(entity);
		 
	}
}
