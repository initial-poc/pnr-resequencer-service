package com.sabre.ngp.ar.etfinalizationservice.entity;

import com.google.cloud.Timestamp;
import lombok.Data;

@Data
public class OutboxLogEntity {
   private String locator ;
   private Long version ;
    private String  total_records ;
    private String query_to_dto ;
    private String pubsub_time;
    private String  query_time ;

    private Timestamp  created ;
    private Timestamp  updatedByPoller ;
    private Timestamp updated ;
    private long pnr_id;
}
