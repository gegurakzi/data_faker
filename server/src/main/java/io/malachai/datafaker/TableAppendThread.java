package io.malachai.datafaker;

import io.malachai.datafaker.exception.TableNotInitializedException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class TableAppendThread extends Thread {

    private final Long tableKey;
    private final Table table;
    private final Column primaryKey;
    private final List<Column> foreignKeys;
    private final List<Column> createdAt;
    private final List<Column> updatedAt;
    private final List<Column> fieldColumns;

    private final EntityManager entityManager;
    private final ConstraintManager constraintManager;
    private final DataSourceManager dataSourceManager;

    private final DataKindFaker faker = new DataKindFaker();

    public TableAppendThread(Table table, EntityManager entityManager,
        ConstraintManager constraintManager,
        DataSourceManager dataSourceManager) {
        this.table = table;
        this.entityManager = entityManager;
        this.constraintManager = constraintManager;
        this.dataSourceManager = dataSourceManager;

        this.tableKey = this.entityManager.getTableKey(table.getFullName());
        this.primaryKey = this.table.getColumns().stream()
            .filter(col -> col.getConstraint() == ConstraintType.PRIMARY)
            .findFirst().orElse(null);
        this.foreignKeys = this.table.getColumns().stream()
            .filter(col -> col.getConstraint() == ConstraintType.FOREIGN)
            .collect(Collectors.toList());
        this.createdAt = this.table.getColumns().stream()
            .filter(col -> col.getConstraint() == ConstraintType.CREATED_AT)
            .collect(Collectors.toList());
        this.updatedAt = this.table.getColumns().stream()
            .filter(col -> col.getConstraint() == ConstraintType.UPDATED_AT)
            .collect(Collectors.toList());
        this.fieldColumns = this.table.getColumns().stream()
            .filter(col -> col.getConstraint() == ConstraintType.NONE)
            .collect(Collectors.toList());
    }

    public void run() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (true) {
            ParamMap<String, String, Object> params = new ParamMap<>();
            if (primaryKey != null) {
                Long value;
                try {
                    value = constraintManager.getPrimaryKey(tableKey);
                } catch (TableNotInitializedException e) {
                    value = 0L;
                }
                params.put(primaryKey.getName(), primaryKey.getType(), value);
            }
            for (Column fk : foreignKeys) {
                Long key;
                while (true) {
                    try {
                        key = constraintManager.getPrimaryKey(
                            entityManager.getTableKey(fk.getReference().getTableFullName()));
                        break;
                    } catch (TableNotInitializedException e) {
                        sleepWithoutException(table.getSparsity());
                    }
                }
                params.put(fk.getName(), fk.getType(), key != 0L ? random.nextLong(key) : 0L);
            }
            for (Column created : createdAt) {
                switch (created.getType()) {
                    case "string":
                    case "timestamp":
                        params.put(created.getName(), created.getType(),
                            LocalDateTime.now().format(
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                        break;
                    case "date":
                        params.put(created.getName(), created.getType(),
                            LocalDateTime.now().format(
                                DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    case "long":
                        params.put(created.getName(), created.getType(), LocalDateTime.now()
                            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                }
            }
            for (Column updated : updatedAt) {
                switch (updated.getType()) {
                    case "string":
                    case "timestamp":
                        params.put(updated.getName(), updated.getType(),
                            LocalDateTime.now().format(
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                        break;
                    case "date":
                        params.put(updated.getName(), updated.getType(),
                            LocalDateTime.now().format(
                                DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    case "long":
                        params.put(updated.getName(), updated.getType(), LocalDateTime.now()
                            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
                }
            }
            for (Column field : fieldColumns) {
                params.put(field.getName(), field.getType(), faker.get(field.getKind()));
            }

            try {
                dataSourceManager.execute(entityManager.getSourceKey(table.getSourceName()),
                    mapStatement(params));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            constraintManager.increasePrimaryKey(tableKey);
            sleepWithoutException(table.getSparsity());
        }

    }

    private String mapStatement(ParamMap<String, String, Object> params) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(table.getName());
        sb.append(" (");
        sb.append(String.join(",", params.keySet()));
        sb.append(") VALUES (");
        sb.append(String.join(",",
            params.values().stream()
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
                .collect(Collectors.toList())));
        sb.append(")");
        return sb.toString();
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
