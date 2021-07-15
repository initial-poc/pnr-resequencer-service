package com.sabre.ngp.ar.etfinalizationservice.repository;

import com.google.api.client.util.Lists;
import com.google.cloud.spanner.*;
import com.sabre.ngp.ar.etfinalizationservice.entity.OutboxEntity;
import com.sabre.ngp.ar.etfinalizationservice.util.OutboxRecordStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j

public class SpannerOutboxRepository {


    private final DatabaseClient databaseClient;
    private static final String OUTBOX_SQL = "select * from outbox where created in (select  min(created) from OUTBOX  where status in (0,3) group by  locator limit 10000) limit 10000";

    public List<OutboxEntity> getRecords() {
        ResultSet rs = databaseClient.singleUse().executeQuery(Statement.of(OUTBOX_SQL));
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
        return outboxEntities;
    }

    public void batchUpdate(List<OutboxEntity> entities, OutboxRecordStatus status) {
        log.info("total records to update {}",entities.size());
        List<Mutation> mutations = Lists.newArrayList();
        for (OutboxEntity entity : entities) {
            mutations.add(Mutation.newUpdateBuilder("OUTBOX")
                    .set("status")
                    .to(status.getStatusCode())
                    .set("UPDATED")
                    .to(Value.COMMIT_TIMESTAMP)
                    .set("locator").to(entity.getLocator()).
                            set("version").to(entity.getVersion())
                    .build());
        }
        databaseClient.write(mutations);
    }

    public void update(OutboxEntity entity, OutboxRecordStatus outboxRecordStatus) {
        List<Mutation> mutations =
                Arrays.asList(
                        Mutation.newUpdateBuilder("OUTBOX")
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
