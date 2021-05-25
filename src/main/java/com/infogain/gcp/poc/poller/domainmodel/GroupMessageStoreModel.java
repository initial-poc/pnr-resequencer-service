package com.infogain.gcp.poc.poller.domainmodel;

import com.google.cloud.Timestamp;
import com.infogain.gcp.poc.poller.entity.GroupMessageStoreEntity;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;

@Getter
@Setter
@ToString
public class GroupMessageStoreModel {

	private String pnrid;
	private Integer messageseq;
	private String payload;
	private String timestamp;
	
	
	 @SneakyThrows
	    public GroupMessageStoreEntity buildEntity(){
		 GroupMessageStoreEntity groupMessageStoreEntity = new GroupMessageStoreEntity();
	        groupMessageStoreEntity.setPnrid(pnrid);
	        groupMessageStoreEntity.setMessageseq(messageseq);
	        groupMessageStoreEntity.setPayload(payload);
	        groupMessageStoreEntity.setTimestamp(Timestamp.parseTimestamp(timestamp));
	        return groupMessageStoreEntity;
	    }

}
