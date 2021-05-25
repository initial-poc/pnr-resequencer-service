package com.infogain.gcp.poc.poller.domainmodel;

import com.google.cloud.Timestamp;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class PNRModel {

	private String pnrid;
	private String messageseq;
	private String payload;
	private String timestamp;
	private int retry_count;
	private Timestamp updated;
	//private String destination;
	private String parentPnr;
}
