package com.infogain.gcp.poc.component;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gcp.pubsub.support.converter.PubSubMessageConversionException;
import org.springframework.cloud.gcp.pubsub.support.converter.PubSubMessageConverter;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.util.Map;

@Slf4j
@Component
public class CustomizedPubSubMessageConverter implements PubSubMessageConverter {

    private static final String ORDERING_KEY = "ordering-key";
    private final Charset charset;

    public CustomizedPubSubMessageConverter() {
        this(Charset.defaultCharset());
    }

    public CustomizedPubSubMessageConverter(Charset charset) {
        this.charset = charset;
    }

    @Override
    public PubsubMessage toPubSubMessage(Object payload, Map<String, String> headers) {

        log.info("Converting message to customizedpubsub message with ordering key.");

        ByteString convertedPayload;

        if (payload instanceof String) {
            convertedPayload = ByteString.copyFrom(((String) payload).getBytes(this.charset));
        } else {
            throw new PubSubMessageConversionException("Unable to convert payload of type " +
                    payload.getClass().getName() + " to byte[] for sending to Pub/Sub.");
        }

        String orderingKeyValue = headers.get(ORDERING_KEY);

        return PubsubMessage.newBuilder()
                .setData(convertedPayload)
                .setOrderingKey(orderingKeyValue)
                .build();
    }

    @Override
    public <T> T fromPubSubMessage(PubsubMessage message, Class<T> payloadType) {
        return null;
    }
}
