package com.sabre.ngp.ar.etfinalizationservice.entity;

import com.google.cloud.Timestamp;
import com.sabre.ngp.ar.etfinalizationservice.domainmodel.PNRModel;
import lombok.*;


@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class OutboxEntity {
    private String locator;
    private long version;
    private Timestamp created;
    private String payload;
    private long status;
    private int retry_count;
    private Timestamp updated;
    private long processing_time_millis;
    private String parentPnr;


    public PNRModel buildEntity(){
        PNRModel model= new PNRModel();
        model.setPayload(this.getPayload());
        model.setVersion(this.getVersion());
        model.setParentPnr(this.getParentPnr());
        model.setCreated(this.getCreated());
        model.setStatus(this.getStatus());
        model.setLocator(this.getLocator());
        model.setUpdated(this.getUpdated());
    return model;
    }

    @Override
    public String toString() {
        return "OutboxEntity{" +
                "locator='" + locator + '\'' +
                ", version=" + version +
                '}';
    }
}
