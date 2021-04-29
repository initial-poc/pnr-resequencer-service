package com.infogain.gcp.poc.util;

import java.util.HashMap;
import java.util.Map;

public enum RecordStatus {
	IN_PROGESS("1", "Record is in progess"), RELEASED("2", "Record is released"), COMPLETED("3", "Record is completed"),
	FAILED("4", "Record process is faild");

	private final String statusCode;
	private final String statusMessage;

	private RecordStatus(final String statusCode, final String statusMessage) {
		this.statusCode = statusCode;
		this.statusMessage = statusMessage;
	}

	public String getStatusCode() {
		return statusCode;
	}

	public String getStatusMessage() {
		return statusMessage;
	}

	private static final Map<String, RecordStatus> lookUp = new HashMap<>();

	static {
		for (RecordStatus d : RecordStatus.values()) {
			lookUp.put(d.getStatusCode(), d);
		}
	}

	public static RecordStatus getStatusMessage(final String statusCode) {
		return lookUp.get(statusCode);
	}

}
