package com.sabre.ngp.ar.etfinalizationservice.entity;

import com.google.cloud.Timestamp;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


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
    private List<String> destinations = new ArrayList<>();




    @Override
    public String toString() {
        return "OutboxEntity{" +
                "locator='" + locator + '\'' +
                ", status=" + status +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutboxEntity objEntity = (OutboxEntity) o;
        return version == objEntity.version && locator.equals(objEntity.locator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(locator, version);
    }
}
