package com.infogain.gcp.poc.poller.sequencer;

import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.service.PNRSequencingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SequencerOps {

    @Autowired
    private PNRSequencingService pnrSequencingService;

    /*
    This call is a replacement of resequencer API.
     */
    public String callProcessPNR(PNRModel pnrModel) {

        String pnrResult = pnrSequencingService.processPNR(pnrModel);
        if (StringUtils.isBlank(pnrResult)) {
            //throw error
        }

        return pnrResult;
    }
}
