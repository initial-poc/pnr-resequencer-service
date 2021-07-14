package com.sabre.ngp.ar.etfinalizationservice.rule;

import com.sabre.ngp.ar.etfinalizationservice.domainmodel.PNRModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TeleTypeTopicRule implements Rule {
    private static final String TELETYPE_PAYLOAD_IDENTIFIER = "teleType";
    @Value("${topic.name.teleType}")
    private String teleTypeTopicName;



    @Override
    public boolean accept(PNRModel pnrModel) {

        return pnrModel.getPayload().contains(TELETYPE_PAYLOAD_IDENTIFIER);
    }

    @Override
    public void execute(PNRModel pnrModel) {
        pnrModel.getDestinations().add(teleTypeTopicName);

    }
}
