package com.sabre.ngp.ar.etfinalizationservice.rule;

import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ItineraryTopicRule implements  Rule{
    private static final String ITINERARY_PAYLOAD_IDENTIFIER="itinerary";

    @Override
    public boolean accept(OutboxEntity pnrModel) {
        return pnrModel.getPayload().contains(ITINERARY_PAYLOAD_IDENTIFIER);
    }

    @Override
    public void execute(OutboxEntity pnrModel) {
        pnrModel.getDestinations().add(ITINERARY_PAYLOAD_IDENTIFIER);

    }
}
