package com.sabre.ngp.ar.etfinalizationservice.rule;

import com.sabre.ngp.ar.etfinalizationservice.domainmodel.OutboxEntity;

public interface Rule {
    boolean accept(OutboxEntity pnrModel);
    void execute(OutboxEntity pnrModel);
}
