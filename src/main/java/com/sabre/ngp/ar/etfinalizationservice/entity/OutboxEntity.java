package com.sabre.ngp.ar.etfinalizationservice.entity;

import com.google.cloud.Timestamp;
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
    private Timestamp updated;
    private long processing_time_millis;
    private String parentPnr;




    @Override
    public String toString() {
        return "OutboxEntity{" +
                "locator='" + locator + '\'' +
                ", version=" + version +
                '}';
    }
}
