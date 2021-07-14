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
    private static final String OUTBOX_SQL = "select * from outbox where created in (select  min(created) from OUTBOX  where status in (0,3) group by  locator limit 500) ";

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
            if (!rs.isNull("payload_type")) {
                entity.setPayloadType(rs.getString("payload_type"));
            }

            //  entity.setRetry_count(Integer.parseInt(rs.getString("retry_count")));
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
                    .set("payload_type").to(entity.getPayloadType())
                    .build());
        }
        databaseClient.write(mutations);
        /*
        List<Statement> statementList=Lists.newArrayList();
        for(OutboxEntity entity :entities) {

            String sql = "update outbox set status=@status where locator=@locator and version=@version and payload_type=@payloadType";
            Statement statement = Statement.newBuilder(sql).bind("status").to(entity.getStatus()).
                    bind("locator").to(entity.getLocator()).
                    bind("version").to(entity.getVersion()).bind("payloadType").to(entity.getPayloadType()).build();
            statementList.add(statement);
        }
        databaseClient.readWriteTransaction().run((transactionContext) -> {
            long[] rowCounts = transactionContext.batchUpdate(statementList);
            log.info("batch row updated count -> {}",rowCounts);
            return null;
        });*/
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
                                .set("payload_type").to(entity.getPayloadType())
                                .build());
        databaseClient.write(mutations);

       /* String sql = "update outbox set status=@status , updated=@updated where locator=@locator and version=@version and payload_type=@payloadType";
        Statement statement = Statement.newBuilder(sql).bind("status").to(outboxRecordStatus.getStatusCode()).
                bind("updated").to().
                bind("locator").to(entity.getLocator()).
                bind("version").to(entity.getVersion()).bind("payloadType").to(entity.getPayloadType()).build();
          databaseClient.readWriteTransaction().run(transactionContext -> {long updatedRowCount=transactionContext.executeUpdate(statement);
          log.info("updated row count {}",updatedRowCount);
          return null;
    }
          );*/
    }

    public static void main(String[] args) {
        double x=45;
        double y =(x/10);
        System.out.println(Math.ceil(y));


    }

}
