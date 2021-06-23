package com.infogain.gcp.poc.domainmodel;

import com.fasterxml.jackson.annotation.JsonProperty;
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

	@JsonProperty("pnrid")
	private String pnrid;

	@JsonProperty("message")
	private String messageseq;

	@JsonProperty("payload")
	private String payload;

	@JsonProperty("timestamp")
	private String timestamp;

	@JsonProperty("destination")
	private String destination;

	@JsonProperty("retry_count")
	private Integer retryCount;

	@JsonProperty("parent_pnr")
	private String parentPnr;

	 @SneakyThrows
	    public PNREntity buildEntity(){
	        PNREntity pnrEntity = new PNREntity();
	        pnrEntity.setPnrid(pnrid);
	        pnrEntity.setMessageseq(Integer.parseInt(messageseq));
	        pnrEntity.setPayload(payload);
	        pnrEntity.setTimestamp(Timestamp.parseTimestamp(timestamp));
	        pnrEntity.setDestination(destination);
	        pnrEntity.setParentPnr(parentPnr);
	        return pnrEntity;
	    }

}
