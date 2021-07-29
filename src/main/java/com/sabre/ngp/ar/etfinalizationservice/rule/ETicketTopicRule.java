package com.sabre.ngp.ar.etfinalizationservice.rule;

import com.sabre.ngp.ar.etfinalizationservice.domainmodel.OutboxEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ETicketTopicRule implements  Rule{
    private static final String ETICKET_PAYLOAD_IDENTIFIER = "eticket";
    @Value("${topic.name.eticket}")
    private String eticketTopicName;



    @Override
    public boolean accept(OutboxEntity pnrModel) {
        return pnrModel.getPayload().contains(ETICKET_PAYLOAD_IDENTIFIER);
    }

    @Override
    public void execute(OutboxEntity pnrModel) {
        pnrModel.getDestinations().add(eticketTopicName);
    }
}
