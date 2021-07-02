package com.infogain.gcp.poc.component;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.infogain.gcp.poc.util.PublisherUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.stereotype.Component;

import com.infogain.gcp.poc.entity.PNREntity;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessagePublisher {

	private final MessageConverter messageConverter;
	
	@Value("${app.topic.name}")
	private String topicName;



	public void publishMessage(){

	}


	public void publishMessage(PNREntity entity) throws InterruptedException, IOException {

		PubsubMessage pubsubMessage = getPubsubMessage(entity);
		Publisher publisher = PublisherUtil.getPublisher(topicName);
		ApiFuture<String> future = publisher.publish(pubsubMessage);

		try {

			ApiFutures.addCallback(
					future, PublisherUtil.getCallback(pubsubMessage), MoreExecutors.directExecutor());
		} finally {
			publisher.shutdown();
			publisher.awaitTermination(1, TimeUnit.MINUTES);
		}

	}

	private PubsubMessage getPubsubMessage(PNREntity pnrEntity) {

		String message = convertMessage(pnrEntity);

		ByteString data = ByteString.copyFromUtf8(message);

		return PubsubMessage.newBuilder()
				.setData(data)
				.setOrderingKey(pnrEntity.getPnrid())
				.build();
	}
	
	private String convertMessage(PNREntity entity){
		return  messageConverter.convert(entity);
		 
	}
}
