package com.sabre.ngp.ar.etfinalizationservice.repository;

import com.google.api.client.util.Lists;
import com.google.cloud.spanner.*;
import com.google.common.base.Stopwatch;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import com.sabre.ngp.ar.etfinalizationservice.util.OutboxRecordStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

@Component
@RequiredArgsConstructor
@Slf4j

public class SpannerOutboxRepository {
    private final ThreadPoolExecutor threadPoolExecutor;
    @org.springframework.beans.factory.annotation.Value("${queryLimit:50}")
    private int queryLimit;
    @org.springframework.beans.factory.annotation.Value("${threadCount}")
    private Integer maxThreadCount;

    @org.springframework.beans.factory.annotation.Value("${pubsubBatchSize}")
    private int pubsubBatchSize;

    private final DatabaseClient databaseClient;
    private static final String OUTBOX_SQL = "select  locator,version,payload from OUTBOX_300_MOCK  where status status in (0,3) order by created limit %s";

    public List<OutboxEntity> getRecords(Map<String,String> metaData) {
        Stopwatch stopwatch= Stopwatch.createStarted();
                /* log.info("remainingCapacity {}",threadPoolExecutor.getQueue().remainingCapacity());
                 if(threadPoolExecutor.getQueue().remainingCapacity()!=0){
                     queryLimit = threadPoolExecutor.getQueue().remainingCapacity();
                 }else{
                     log.info("in else");
                     queryLimit=0;
                 }*/

       // log.info("Going to perform query with limit {}",queryLimit);


        Stopwatch queryStopWatch= Stopwatch.createStarted();
        ResultSet rs = databaseClient.singleUse().executeQuery(Statement.of(String.format(OUTBOX_SQL, "300")));
        queryStopWatch=queryStopWatch.stop();
        metaData.put("query_time",queryStopWatch.toString());
        List<OutboxEntity> outboxEntities = Lists.newArrayList();
        while (rs.next()) {
            OutboxEntity entity = new OutboxEntity();
           // entity.setCreated(rs.getTimestamp("created"));
            entity.setVersion(rs.getLong("version"));
            entity.setLocator(rs.getString("locator"));
            entity.setPayload(rs.getString("payload"));
         /*   if (!rs.isNull("parent_locator")) {
                entity.setParentPnr(rs.getString("parent_locator"));
            }
            entity.setStatus(rs.getLong("status"));*/
            outboxEntities.add(entity);
        }
        stopwatch=    stopwatch.stop();
        metaData.put("query_to_dto",stopwatch.toString());
        metaData.put("total_records",String.valueOf(outboxEntities.size()));
        log.info("Query took {} to get records of {}",stopwatch,outboxEntities.size());
        return outboxEntities;
    }

    public void batchUpdate(List<OutboxEntity> entities, OutboxRecordStatus status, Map<String, String> metadata) {
        Stopwatch stopwatch= Stopwatch.createStarted();
        List<Mutation> mutations = Lists.newArrayList();
        for (OutboxEntity entity : entities) {
            Mutation.WriteBuilder builder = Mutation.newUpdateBuilder("OUTBOX_300_MOCK")
                    .set("status")
                    .to(status.getStatusCode())

                    .set("locator").to(entity.getLocator()).
                            set("version").to(entity.getVersion());
if(status.getStatusCode()==OutboxRecordStatus.COMPLETED.getStatusCode()){
    builder .set("UPDATED")
            .to(Value.COMMIT_TIMESTAMP)
            .set("query_to_dto").to(metadata.get("query_to_dto"))
            .set("pubsub_time").to(metadata.get("pubsub_time"))
            .set("query_time").to(metadata.get("query_time"))
            .set("total_records").to(metadata.get("total_records"));
}else{
    builder .set("updatedByPoller")
            .to(Value.COMMIT_TIMESTAMP);
}
            mutations.add(builder.build());
        }
        databaseClient.write(mutations);
          stopwatch = stopwatch.stop();
          metadata.put("batchUpdate"+status.getStatusCode(),stopwatch.toString());
        log.info("Batch Update took {} to update records of {}",stopwatch,entities.size());
    }

    public void update(OutboxEntity entity, OutboxRecordStatus outboxRecordStatus) {
        List<Mutation> mutations =
                Arrays.asList(
                        Mutation.newUpdateBuilder("OUTBOX_300_MOCK")
                                .set("status")
                                .to(outboxRecordStatus.getStatusCode())
                                .set("UPDATED")
                                .to(Value.COMMIT_TIMESTAMP)
                                .set("locator").to(entity.getLocator()).
                                set("version").to(entity.getVersion())
                                .build());
        databaseClient.write(mutations);


    }
}
