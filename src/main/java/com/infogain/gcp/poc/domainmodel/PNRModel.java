package com.infogain.gcp.poc.domainmodel;

import com.google.cloud.Timestamp;
import com.infogain.gcp.poc.entity.PNREntity;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;

@Getter
@Setter
@ToString
public class PNRModel {

	private String pnrid;
	private Integer messageseq;
	private String payload;
	private String timestamp;
	
	
	 @SneakyThrows
	    public PNREntity buildEntity(){
	        PNREntity pnrEntity = new PNREntity();
	        pnrEntity.setPnrid(pnrid);
	        pnrEntity.setMessageseq(messageseq);
	        pnrEntity.setPayload(payload);
	        pnrEntity.setTimestamp(Timestamp.parseTimestamp(timestamp));
	        return pnrEntity;
	    }

}
