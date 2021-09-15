package com.sabre.ngp.ar.etfinalizationservice.rule;

import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TeleTypeTopicRule implements Rule {
    private static final String TELETYPE_PAYLOAD_IDENTIFIER = "teleType";

    @Override
    public boolean accept(OutboxEntity pnrModel) {
        return pnrModel.getPayload().contains(TELETYPE_PAYLOAD_IDENTIFIER);
    }

    @Override
    public void execute(OutboxEntity pnrModel) {
        pnrModel.getDestinations().add(TELETYPE_PAYLOAD_IDENTIFIER);

    }
}
