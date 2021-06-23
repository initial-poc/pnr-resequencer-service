package com.infogain.gcp.poc.poller.util;

import java.util.HashMap;
import java.util.Map;

public enum OutboxRecordStatus {
	IN_PROGRESS(1, "Record is in progess"),   COMPLETED(2, "Record is completed"),
	FAILED(3, "Record process is faild"),
	CREATED(0, "Record is created")
	;

	private final int statusCode;
	private final String statusMessage;

	private OutboxRecordStatus(final int statusCode, final String statusMessage) {
		this.statusCode = statusCode;
		this.statusMessage = statusMessage;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public String getStatusMessage() {
		return statusMessage;
	}

	private static final Map<Integer, OutboxRecordStatus> lookUp = new HashMap<>();

	static {
		for (OutboxRecordStatus d : OutboxRecordStatus.values()) {
			lookUp.put(d.getStatusCode(), d);
		}
	}

	public static OutboxRecordStatus getStatusMessage(final int statusCode) {
		return lookUp.get(statusCode);
	}

}
