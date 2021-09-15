package com.sabre.ngp.ar.etfinalizationservice.rule;

import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ETicketTopicRule implements  Rule{
    private static final String ETICKET_PAYLOAD_IDENTIFIER = "eticket";

    @Override
    public boolean accept(OutboxEntity pnrModel) {
        return pnrModel.getPayload().contains(ETICKET_PAYLOAD_IDENTIFIER);
    }

    @Override
    public void execute(OutboxEntity pnrModel) {
        pnrModel.getDestinations().add(ETICKET_PAYLOAD_IDENTIFIER);
    }
}
