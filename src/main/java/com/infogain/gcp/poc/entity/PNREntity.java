package com.infogain.gcp.poc.entity;

import org.springframework.beans.BeanUtils;
import org.springframework.cloud.gcp.data.spanner.core.mapping.Column;
import org.springframework.cloud.gcp.data.spanner.core.mapping.PrimaryKey;
import org.springframework.cloud.gcp.data.spanner.core.mapping.Table;

import com.google.cloud.Timestamp;
import com.infogain.gcp.poc.domainmodel.PNRModel;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import org.springframework.data.annotation.Id;

import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
@Table(name = "group_message_store")
public class PNREntity implements Comparable<PNREntity>{

    @PrimaryKey(keyOrder = 1)
    @Column(name = "pnrid")
    private String pnrid;

    @PrimaryKey(keyOrder = 2)
    @Column(name = "messageseq")
    private Integer messageseq;

    @PrimaryKey(keyOrder = 3)
    @Column(name = "destination")
    private String destination;

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

    private String parentPnr;

    @SneakyThrows
    public PNRModel buildModel() {
        PNRModel pnrModel = new PNRModel();
        pnrModel.setTimestamp(timestamp.toString());
        pnrModel.setPayload(this.getPayload());
        pnrModel.setDestination(this.getDestination());
        pnrModel.setRetryCount(this.getRetry_count());
        pnrModel.setPnrid(this.getPnrid());
        pnrModel.setMessageseq(String.valueOf(this.getMessageseq()));
        pnrModel.setParentPnr(this.getParentPnr());
      //  BeanUtils.copyProperties(pnrModel, this);
        return pnrModel;
    }

    @Override
    public int compareTo(PNREntity o) {
       return this.getMessageseq().compareTo(((PNREntity)o).getMessageseq());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PNREntity pnrEntity = (PNREntity) o;
        return Objects.equals(pnrid, pnrEntity.pnrid) && Objects.equals(messageseq, pnrEntity.messageseq) && Objects.equals(destination, pnrEntity.destination);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pnrid, messageseq, destination);
    }

    @Override
    public String toString() {
        return "PNREntity{" +
                "pnrid='" + pnrid + '\'' +
                ", messageseq=" + messageseq +
                ", destination='" + destination + '\'' +
                '}';
    }
}