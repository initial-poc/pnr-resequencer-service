package com.sabre.ngp.ar.etfinalizationservice.domainmodel;

import com.google.api.client.util.Lists;
import com.google.cloud.Timestamp;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
public class PNRModel {
	private String locator;
	private long version;
	private String payloadType;
	private Timestamp created;
	private String payload;
	private long status;
	private int retry_count;
	private Timestamp updated;
	private long processing_time_millis;
	private String parentPnr;
	private List<String> destinations= Lists.newArrayList();

	@Override
	public String toString() {
		return "PNRModel{" +
				"locator='" + locator + '\'' +
				", version=" + version +
				", payloadType='" + payloadType + '\'' +
				", destinations=" + destinations +
				'}';
	}
}
