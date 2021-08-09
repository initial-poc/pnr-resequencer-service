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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    private static final String OUTBOX_SQL = "select  * from OUTBOX_BATCH_PUBLISH  where status =0 order by created limit %s";

    public List<OutboxEntity> getRecords() {

                 log.info("remainingCapacity {}",threadPoolExecutor.getQueue().remainingCapacity());
                 if(threadPoolExecutor.getQueue().remainingCapacity()!=0){
                     queryLimit = threadPoolExecutor.getQueue().remainingCapacity();
                 }else{
                     log.info("in else");
                     queryLimit=0;
                 }

        log.info("Going to perform query with limit {}",queryLimit);

        Stopwatch stopwatch= Stopwatch.createStarted();
        ResultSet rs = databaseClient.singleUse().executeQuery(Statement.of(String.format(OUTBOX_SQL, queryLimit)));
        List<OutboxEntity> outboxEntities = Lists.newArrayList();
        while (rs.next()) {
            OutboxEntity entity = new OutboxEntity();
            entity.setCreated(rs.getTimestamp("created"));
            entity.setVersion(rs.getLong("version"));
            entity.setLocator(rs.getString("locator"));
            entity.setPayload(rs.getString("payload"));
            if (!rs.isNull("parent_locator")) {
                entity.setParentPnr(rs.getString("parent_locator"));
            }
            entity.setStatus(rs.getLong("status"));
            outboxEntities.add(entity);
        }
        stopwatch=    stopwatch.stop();
        log.info("Query took {} to get records of {}",stopwatch,outboxEntities.size());
        return outboxEntities;
    }

    public void batchUpdate(List<OutboxEntity> entities, OutboxRecordStatus status) {
        Stopwatch stopwatch= Stopwatch.createStarted();
        List<Mutation> mutations = Lists.newArrayList();
        for (OutboxEntity entity : entities) {
            mutations.add(Mutation.newUpdateBuilder("OUTBOX_BATCH_PUBLISH")
                    .set("status")
                    .to(status.getStatusCode())
                    .set("UPDATED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .set("locator").to(entity.getLocator()).
                            set("version").to(entity.getVersion())
                    .build());
        }
        databaseClient.write(mutations);
          stopwatch = stopwatch.stop();
        log.info("Batch Update took {} to update records of {}",stopwatch,entities.size());
    }

    public void update(OutboxEntity entity, OutboxRecordStatus outboxRecordStatus) {
        List<Mutation> mutations =
                Arrays.asList(
                        Mutation.newUpdateBuilder("OUTBOX_BATCH_PUBLISH")
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
