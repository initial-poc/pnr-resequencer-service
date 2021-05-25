package com.infogain.gcp.poc.poller.entity;

import com.google.cloud.Timestamp;
import com.infogain.gcp.poc.poller.domainmodel.GroupMessageStoreModel;
import lombok.*;
import org.springframework.beans.BeanUtils;
import org.springframework.cloud.gcp.data.spanner.core.mapping.Column;
import org.springframework.cloud.gcp.data.spanner.core.mapping.PrimaryKey;
import org.springframework.cloud.gcp.data.spanner.core.mapping.Table;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
@Table(name = "group_message_store")
public class GroupMessageStoreEntity implements Comparable<GroupMessageStoreEntity>{

    @PrimaryKey(keyOrder = 1)
    @Column(name = "pnrid")
    private String pnrid;

    @PrimaryKey(keyOrder = 2)
    @Column(name = "messageseq")
    private Integer messageseq;

    @Column(name = "status")
    private Integer status;

    @Column(name = "payload")
    private String payload;

    @Column(name = "timestamp")
    private Timestamp timestamp;

    @Column(name = "instance")
    private String instance;

    private int retry_count;

    private Timestamp updated;

    @SneakyThrows
    public GroupMessageStoreModel buildModel() {
        GroupMessageStoreModel pnrModel = new GroupMessageStoreModel();
        BeanUtils.copyProperties(pnrModel, this);
        return pnrModel;
    }

    @Override
    public int compareTo(GroupMessageStoreEntity o) {
       return this.getMessageseq().compareTo(((GroupMessageStoreEntity)o).getMessageseq());
    }
}