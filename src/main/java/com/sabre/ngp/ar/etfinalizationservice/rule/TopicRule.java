package com.sabre.ngp.ar.etfinalizationservice.rule;

import com.sabre.ngp.ar.etfinalizationservice.domainmodel.PNRModel;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class TopicRule {
private final List<Rule> rules;
    public void processDestinations(PNRModel pnrModel){
        rules.stream().filter(rule -> rule.accept(pnrModel)).forEach(rule -> rule.execute(pnrModel));
    }
}
