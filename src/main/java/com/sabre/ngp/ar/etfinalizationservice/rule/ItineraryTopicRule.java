package com.sabre.ngp.ar.etfinalizationservice.rule;

import com.sabre.ngp.ar.etfinalizationservice.domainmodel.OutboxEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ItineraryTopicRule implements  Rule{
    private static final String ITINERARY_PAYLOAD_IDENTIFIER="itinerary";
    @Value("${topic.name.itinerary}")
    private String itineraryTopicName;
    @Override
    public boolean accept(OutboxEntity pnrModel) {

        return pnrModel.getPayload().contains(ITINERARY_PAYLOAD_IDENTIFIER);
    }

    @Override
    public void execute(OutboxEntity pnrModel) {
        pnrModel.getDestinations().add(itineraryTopicName);

    }
}
