package com.sabre.ngp.ar.etfinalizationservice.repository;

import com.google.api.client.util.Lists;
import com.google.cloud.spanner.*;
import com.google.common.base.Stopwatch;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import com.sabre.ngp.ar.etfinalizationservice.util.OutboxRecordStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

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

    @org.springframework.beans.factory.annotation.Value("${table.name}")
    private String tableName;


    private final DatabaseClient databaseClient;
    private static final String OUTBOX_SQL = "select  locator,version,payload from %s  where status  in (0,3) order by created limit %s";

    private static final String DELETE_SQL="DELETE from %s where status =2 and  TIMESTAMP_DIFF (current_timestamp,  updated,minute)>=30";

    public List<OutboxEntity> getRecords(Map<String,String> metaData)throws Exception {


        Stopwatch stopwatch= Stopwatch.createStarted();
        log.info("remainingCapacity {}",threadPoolExecutor.getQueue().remainingCapacity());
        if(threadPoolExecutor.getQueue().remainingCapacity()!=0){
            queryLimit = threadPoolExecutor.getQueue().remainingCapacity();
        }else{
            log.info("in else");
            queryLimit=0;
        }

        // log.info("Going to perform query with limit {}",queryLimit);


        Stopwatch queryStopWatch= Stopwatch.createStarted();
        ResultSet rs = databaseClient.singleUse().executeQuery(Statement.of(String.format(OUTBOX_SQL,tableName, queryLimit)));
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
            Mutation.WriteBuilder builder = Mutation.newUpdateBuilder(tableName)
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

    public void batchUpdate(List<OutboxEntity> entities, Map<String, String> metadata) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        List<Mutation> mutations = Lists.newArrayList();
        for (OutboxEntity entity : entities) {
            Mutation.WriteBuilder builder = Mutation.newUpdateBuilder(tableName)
                    .set("status")
                    .to(entity.getStatus())
                    .set("locator").to(entity.getLocator()).
                            set("version").to(entity.getVersion());

            if (entity.getStatus() == OutboxRecordStatus.COMPLETED.getStatusCode()) {
                builder.set("UPDATED")
                        .to(Value.COMMIT_TIMESTAMP)
                        .set("query_to_dto").to(metadata.get("query_to_dto"))
                        .set("query_time").to(metadata.get("query_time"))
                        .set("pubsub_time").to(metadata.get("pubsub_time"))
                        .set("total_records").to(metadata.get("total_records"));
            } else {
                builder.set("updatedByPoller")
                        .to(Value.COMMIT_TIMESTAMP);
            }
            mutations.add(builder.build());
        }
        databaseClient.write(mutations);
        stopwatch = stopwatch.stop();
        metadata.put("batchUpdate", stopwatch.toString());
        log.info("Batch Update took {} to update records of {}", stopwatch, entities.size());
    }

    public void delete() {

        Stopwatch queryStopWatch= Stopwatch.createStarted();
        String sql = String.format(DELETE_SQL, tableName);

        databaseClient
                .readWriteTransaction()
                .run(transaction -> {
                    long rowCount = transaction.executeUpdate(Statement.of(sql));
                    log.info("Total rows deleted {}", rowCount);
                    return null;
                });


        queryStopWatch=queryStopWatch.stop();
        log.info("record delete is done with time taken {}",queryStopWatch);

    }
}
