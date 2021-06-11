package com.infogain.gcp.poc.poller.util;

import java.util.HashMap;
import java.util.Map;

public enum RecordStatus {
	IN_PROGRESS(1, "Record is in progess"),   COMPLETED(2, "Record is completed"),
	FAILED(3, "Record process is faild"),
	CREATED(0, "Record is created")
	;

	private final int statusCode;
	private final String statusMessage;

	private RecordStatus(final int statusCode, final String statusMessage) {
		this.statusCode = statusCode;
		this.statusMessage = statusMessage;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public String getStatusMessage() {
		return statusMessage;
	}

	private static final Map<Integer, RecordStatus> lookUp = new HashMap<>();

	static {
		for (RecordStatus d : RecordStatus.values()) {
			lookUp.put(d.getStatusCode(), d);
		}
	}

	public static RecordStatus getStatusMessage(final int statusCode) {
		return lookUp.get(statusCode);
	}

}
