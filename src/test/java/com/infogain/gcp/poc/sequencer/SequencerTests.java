package com.infogain.gcp.poc.sequencer;

import com.infogain.gcp.poc.domainmodel.PNRModel;
import com.infogain.gcp.poc.poller.sequencer.SequencerOps;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class SequencerTests {

    @Autowired
    private SequencerOps sequencerOps;

    @Test
    public void testBasic() {

        //PNRModel pnrModel = new PNRModel();
        //pnrModel.setPnrid("anyid1");

        //sequencerOps.callProcessPNR();
    }
}
