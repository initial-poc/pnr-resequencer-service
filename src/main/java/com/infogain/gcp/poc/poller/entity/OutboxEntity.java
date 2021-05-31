package com.infogain.gcp.poc.poller.entity;

import com.google.cloud.Timestamp;
import com.infogain.gcp.poc.poller.domainmodel.PNRModel;
import lombok.*;
import org.springframework.cloud.gcp.data.spanner.core.mapping.Column;
import org.springframework.cloud.gcp.data.spanner.core.mapping.PrimaryKey;
import org.springframework.cloud.gcp.data.spanner.core.mapping.Table;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Table(name = "outbox")
public class OutboxEntity {
    @PrimaryKey(keyOrder = 1)
    private String locator;
    @PrimaryKey(keyOrder = 2)
    private String version;
    //@Column(name = "parent_locator")
    //private String parentLocator;
    private Timestamp created;
    private String data;
    private int status;
    private int retry_count;
    private Timestamp updated;
    private long processing_time_millis;
    private String parentPnr;

    @SneakyThrows
    public PNRModel buildModel() {
        PNRModel pnrModel = new PNRModel();
        pnrModel.setMessageseq(this.getVersion());
        pnrModel.setPayload(this.getData());
        pnrModel.setPnrid(this.getLocator());
        pnrModel.setTimestamp(this.getCreated()==null?Timestamp.now().toString():this.getCreated().toString());
        pnrModel.setRetry_count(this.retry_count);
        pnrModel.setUpdated(this.updated);
        pnrModel.setParentPnr(this.parentPnr);
        return pnrModel;
    }

}
