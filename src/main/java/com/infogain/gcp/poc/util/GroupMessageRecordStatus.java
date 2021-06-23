package com.infogain.gcp.poc.util;

import java.util.HashMap;
import java.util.Map;

public enum GroupMessageRecordStatus {
	CREATED(0,"Record created"),
	IN_PROGRESS(1, "Record is in progess"),
	RELEASED(2, "Record is released"),
	COMPLETED(3, "Record is completed"),
	FAILED(4, "Record process is failed");

	private final int statusCode;
	private final String statusMessage;

	private GroupMessageRecordStatus(final int statusCode, final String statusMessage) {
		this.statusCode = statusCode;
		this.statusMessage = statusMessage;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public String getStatusMessage() {
		return statusMessage;
	}

	private static final Map<Integer, GroupMessageRecordStatus> lookUp = new HashMap<>();

	static {
		for (GroupMessageRecordStatus d : GroupMessageRecordStatus.values()) {
			lookUp.put(d.getStatusCode(), d);
		}
	}

	public static GroupMessageRecordStatus getStatusMessage(final int statusCode) {
		return lookUp.get(statusCode);
	}

}
