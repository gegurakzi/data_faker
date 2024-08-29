package io.malachai.datafaker.repeater;

import io.malachai.datafaker.Column;
import io.malachai.datafaker.DataKindFaker;
import io.malachai.datafaker.DataSourceManager;
import io.malachai.datafaker.EntityManager;
import io.malachai.datafaker.PrimaryKeyManager;
import io.malachai.datafaker.TableReserve;
import io.malachai.datafaker.exception.TableNotInitializedException;
import io.malachai.datafaker.util.ParamMap;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InsertStatementRepeater extends StatementRepeater {

    private final Long tableKey;
    private final DataKindFaker faker = new DataKindFaker();

    public InsertStatementRepeater(TableReserve tableReserve, EntityManager entityManager,
        PrimaryKeyManager primaryKeyManager,
        DataSourceManager dataSourceManager) {
        super(tableReserve, entityManager, primaryKeyManager, dataSourceManager);

        this.tableKey = entityManager.getTableKey(tableReserve.getTable().getFullName());
    }

    @Override
    protected ParamMap<String, String, Object> prepareParameters() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        ParamMap<String, String, Object> params = new ParamMap<>();
        if (tableReserve.getPrimaryKey() != null) {
            Long value;
            try {
                value = primaryKeyManager.getPrimaryKey(tableKey);
            } catch (TableNotInitializedException e) {
                value = 0L;
            }
            params.put(
                tableReserve.getPrimaryKey().getName(),
                tableReserve.getPrimaryKey().getType(),
                value
            );
        }
        for (Column fk : tableReserve.getForeignKeys()) {
            Long key;
            while (true) {
                try {
                    key = primaryKeyManager.getPrimaryKey(
                        entityManager.getTableKey(fk.getReference().getTableFullName()));
                    break;
                } catch (TableNotInitializedException e) {
                    sleepWithoutException(1000L);
                }
            }
            params.put(fk.getName(), fk.getType(), key != 0L ? random.nextLong(key) : 0L);
        }
        for (Column timestamp : Stream.concat(tableReserve.getCreatedAt().stream(),
                tableReserve.getUpdatedAt().stream())
            .collect(Collectors.toList())) {
            Object value = null;
            switch (timestamp.getType()) {
                case "string":
                case "timestamp":
                    value = LocalDateTime.now().format(
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    break;
                case "date":
                    value = LocalDateTime.now().format(
                        DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    break;
                case "long":
                    value = LocalDateTime.now()
                        .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            }
            params.put(timestamp.getName(), timestamp.getType(), value);
        }
        for (Column field : tableReserve.getFieldColumns()) {
            params.put(field.getName(), field.getType(), faker.get(field.getKind()));
        }
        return params;
    }

    @Override
    protected String mapStatement(ParamMap<String, String, Object> params) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(tableReserve.getTable().getName());
        sb.append(" (");
        sb.append(String.join(",", params.keySet()));
        sb.append(") VALUES (");
        sb.append(params.values().stream()
            .map(p -> {
                switch (p.getType()) {
                    case "string":
                        return String.format("\"%s\"", p.getObj().toString());
                    case "timestamp":
                    case "date":
                        return String.format("'%s'", p.getObj().toString());
                    default:
                        return p.getObj().toString();
                }
            })
            .collect(Collectors.joining(",")));
        sb.append(")");
        return sb.toString();
    }

    @Override
    protected void postExecution() {
        primaryKeyManager.increasePrimaryKey(tableKey);
        sleepWithoutException(tableReserve.getTable().getInsertInterval());
    }

    private void sleepWithoutException(Long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            //connection.close();
            throw new RuntimeException(e);
        }
    }

}
