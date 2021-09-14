package com.sabre.ngp.ar.etfinalizationservice.repository;

import com.google.cloud.spanner.*;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxLogEntity;
import com.sabre.ngp.ar.etfinalizationservice.util.OutboxRecordStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

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

    @org.springframework.beans.factory.annotation.Value("${log.table.name}")
    private String logTableName;

    @org.springframework.beans.factory.annotation.Value("${record.delete.limit}")
    private long recordDeleteLimit;

    @org.springframework.beans.factory.annotation.Value("${shard.value.start}")
    private String startShardValue;

    @org.springframework.beans.factory.annotation.Value("${shard.value.end}")
    private String endShardValue;

    @org.springframework.beans.factory.annotation.Value("${shard}")
    private String shardValue;


    private final DatabaseClient databaseClient;
    //private static final String OUTBOX_SQL = "select  locator,version,payload from %s  where status  in (0,3) order by created limit %s";
    private static final String OUTBOX_SQL= "SELECT *  FROM %s @{FORCE_INDEX=PNREventShardIndex}    WHERE Status IN (0,3)  AND PNREventShardId =%s     ORDER BY created DESC limit %s";
    private static final String DELETE_SQL = "DELETE FROM %s WHERE LOCATOR IN(SELECT LOCATOR FROM %s WHERE status =2 and  TIMESTAMP_DIFF (current_timestamp,  UPDATED,MINUTE)>=5  limit %d) AND status =2 AND  TIMESTAMP_DIFF (current_timestamp,  UPDATED,minute)>=5";
    private static final String SELECT_SQL = "SELECT locator, version,created ,total_records,updatedByPoller,updated ,query_to_dto,pubsub_time,query_time FROM %s WHERE status =2 and  TIMESTAMP_DIFF (current_timestamp,  UPDATED,MINUTE)>=1 ";

    public List<OutboxEntity> getRecords(Map<String, String> metaData) throws Exception {


        Stopwatch stopwatch = Stopwatch.createStarted();
        log.info("remainingCapacity {}", threadPoolExecutor.getQueue().remainingCapacity());
        if (threadPoolExecutor.getQueue().remainingCapacity() != 0) {
            queryLimit = threadPoolExecutor.getQueue().remainingCapacity();
        } else {
            log.info("in else");
            queryLimit = 0;
        }

        // log.info("Going to perform query with limit {}",queryLimit);


        Stopwatch queryStopWatch = Stopwatch.createStarted();
        String formattedSql = String.format(OUTBOX_SQL, tableName, shardValue,queryLimit);
        log.info("formatted sql {}",formattedSql);
        ResultSet rs = databaseClient.singleUse().executeQuery(Statement.of(formattedSql));
        queryStopWatch = queryStopWatch.stop();
        metaData.put("query_time", queryStopWatch.toString());
        List<OutboxEntity> outboxEntities = Lists.newArrayList();
        while (rs.next()) {
            OutboxEntity entity = new OutboxEntity();
             entity.setCreated(rs.getTimestamp("created"));
            entity.setVersion(rs.getLong("version"));
            entity.setLocator(rs.getString("locator"));
            entity.setPayload(rs.getString("payload"));
         /*   if (!rs.isNull("parent_locator")) {
                entity.setParentPnr(rs.getString("parent_locator"));
            }
            entity.setStatus(rs.getLong("status"));*/
            outboxEntities.add(entity);
        }
        stopwatch = stopwatch.stop();
        metaData.put("query_to_dto", stopwatch.toString());
        metaData.put("total_records", String.valueOf(outboxEntities.size()));
        log.info("Query took {} to get records of {}", stopwatch, outboxEntities.size());
        return outboxEntities;
    }

    public void batchUpdate(List<OutboxEntity> entities, OutboxRecordStatus status, Map<String, String> metadata) {
        List<List<OutboxEntity>> partition=null;
        if(entities.size()>4000){
            partition = Lists.partition(entities, 4000);
        }else{
       partition=     List.of(entities);
        }


        Stopwatch stopwatch = Stopwatch.createStarted();
        partition.stream().forEach(chunk -> update(chunk, status, metadata));
        stopwatch=stopwatch.stop();
        metadata.put("batchUpdate" + status.getStatusCode(), stopwatch.toString());
        log.info("Batch Update took {} to update records of {}", stopwatch, entities.size());
    }

    private void update(List<OutboxEntity> entities, OutboxRecordStatus status, Map<String, String> metadata) {
        List<Mutation> mutations = Lists.newArrayList();
        for (OutboxEntity entity : entities) {
            Mutation.WriteBuilder builder = Mutation.newUpdateBuilder(tableName)
                    .set("status")
                    .to(status.getStatusCode())

                    .set("locator").to(entity.getLocator())
                            .set("created").to(entity.getCreated()).
                            set("version").to(entity.getVersion());
            if (status.getStatusCode() == OutboxRecordStatus.COMPLETED.getStatusCode()) {
                builder.set("UPDATED")
                        .to(Value.COMMIT_TIMESTAMP)
                        .set("query_to_dto").to(metadata.get("query_to_dto"))
                        .set("pubsub_time").to(metadata.get("pubsub_time"))
                        .set("query_time").to(metadata.get("query_time"))
                        .set("total_records").to(metadata.get("total_records"));
            } else {
                builder.set("updatedByPoller")
                        .to(Value.COMMIT_TIMESTAMP);
            }
            mutations.add(builder.build());
        }
        databaseClient.write(mutations);
    }

    public void deleteRecords() {
        Stopwatch queryStopWatch = Stopwatch.createStarted();
        List<OutboxLogEntity> records = getRecords();
        long rowDeleted = 0,  recordSelectedCount=records.size();
        List<List<OutboxLogEntity>> partition = Lists.partition(records, 1000);

        for (List<OutboxLogEntity> entities : partition) {
            rowDeleted = rowDeleted + batchDeleteRecords(entities);
            insertLogs(entities);
        }
        queryStopWatch = queryStopWatch.stop();
        log.info("Total record selected {} deleted {} with time taken {} to complete process", recordSelectedCount, rowDeleted, queryStopWatch);

    }



    private List<OutboxLogEntity> getRecords() {
        List<OutboxLogEntity> records = new ArrayList<>();
        String sql = String.format(SELECT_SQL, tableName);
        log.info("select sql {}",sql);
        ResultSet rs = databaseClient.singleUse().executeQuery(Statement.of(sql));
        while (rs.next()) {
            OutboxLogEntity entity = new OutboxLogEntity();
            entity.setCreated(rs.getTimestamp("created"));
            entity.setTotal_records(rs.getString("total_records"));
            entity.setPubsub_time(rs.getString("pubsub_time"));
            entity.setQuery_time(rs.getString("query_time"));
            entity.setUpdated(rs.getTimestamp("updated"));
            entity.setQuery_to_dto(rs.getString("query_to_dto"));
            entity.setLocator(rs.getString("locator"));
            entity.setUpdatedByPoller(rs.getTimestamp("updatedByPoller"));
            entity.setVersion(rs.getLong("version"));
            records.add(entity);
        }
        log.info("Total Record found for delete {}", records.size());

        return records;
    }

    private void insertLogs(List<OutboxLogEntity> logs) {
        log.info("going to insert records of size {}", logs.size());
        List<Mutation> mutations = Lists.newArrayList();
        for (OutboxLogEntity log : logs) {
            Mutation build = Mutation.newInsertBuilder(logTableName)
                    .set("created").to(log.getCreated())
                    .set("total_records").to(log.getTotal_records())
                    .set("pubsub_time").to(log.getPubsub_time())
                    .set("query_time").to(log.getQuery_time())
                    .set("updated").to(log.getUpdated())
                    .set("query_to_dto").to(log.getQuery_to_dto())
                    .set("locator").to(log.getLocator())
                    .set("updatedByPoller").to(log.getUpdatedByPoller())
                    .set("version").to(log.getVersion())
                    .set("pnr_id").to(UUID.randomUUID().toString())
                            .build();

            mutations.add(build);

        }
    databaseClient.write(mutations);
}

    private long batchDeleteRecords(List<OutboxLogEntity> entities) {
        Type pnrType =
                Type.struct(
                        Arrays.asList(
                                Type.StructField.of("locator", Type.string()),
                                Type.StructField.of("version", Type.int64())));
        List<Struct> pnrList = entities.stream().map(entity -> Struct.newBuilder().set("locator").to(entity.getLocator()).set("version").to(entity.getVersion()).build()).collect(Collectors.toList());
        String deleteSql = "DELETE FROM %s WHERE STRUCT<locator STRING, version INT64>(locator, version) IN UNNEST(@names) ";

        Statement s =
                Statement.newBuilder(
                        String.format(deleteSql, tableName))
                        .bind("names")
                        .toStructArray(pnrType, pnrList)
                        .build();

      return   databaseClient
                .readWriteTransaction()
                .run(transaction -> {
                    return transaction.executeUpdate(s);
                });
    }

}
