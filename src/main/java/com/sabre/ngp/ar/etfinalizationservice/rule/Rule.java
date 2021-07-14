package com.sabre.ngp.ar.etfinalizationservice.rule;

import com.sabre.ngp.ar.etfinalizationservice.domainmodel.PNRModel;

public interface Rule {
    boolean accept(PNRModel pnrModel);
    void execute(PNRModel pnrModel);
}
