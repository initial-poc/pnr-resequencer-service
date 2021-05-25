package com.infogain.gcp.poc.util;


import com.infogain.gcp.poc.poller.domainmodel.PNRModel;

public class PNRModelUtil {

    public static com.infogain.gcp.poc.domainmodel.PNRModel convert(PNRModel pnrModelReq) {

        com.infogain.gcp.poc.domainmodel.PNRModel pnrModelResequencer = new com.infogain.gcp.poc.domainmodel.PNRModel();
        pnrModelResequencer.setPnrid(pnrModelReq.getPnrid());
        pnrModelResequencer.setParentPnr(pnrModelReq.getParentPnr());
        pnrModelResequencer.setMessageseq(Integer.valueOf(pnrModelReq.getMessageseq()));
        pnrModelResequencer.setRetryCount(pnrModelReq.getRetry_count());
        pnrModelResequencer.setDestination(pnrModelReq.getPayload());
        pnrModelResequencer.setPayload(pnrModelReq.getPayload());
        pnrModelResequencer.setTimestamp(pnrModelReq.getTimestamp());

        return pnrModelResequencer;
    }

    public static com.infogain.gcp.poc.domainmodel.PNRModel convert(PNRModel pnrModelReq, String specificDestination) {

        com.infogain.gcp.poc.domainmodel.PNRModel pnrModelResequencer = new com.infogain.gcp.poc.domainmodel.PNRModel();
        pnrModelResequencer.setPnrid(pnrModelReq.getPnrid());
        pnrModelResequencer.setParentPnr(pnrModelReq.getParentPnr());
        pnrModelResequencer.setMessageseq(Integer.valueOf(pnrModelReq.getMessageseq()));
        pnrModelResequencer.setRetryCount(pnrModelReq.getRetry_count());
        pnrModelResequencer.setDestination(specificDestination);
        pnrModelResequencer.setPayload(pnrModelReq.getPayload());
        pnrModelResequencer.setTimestamp(pnrModelReq.getTimestamp());

        return pnrModelResequencer;
    }
}
