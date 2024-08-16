package io.malachai.datafaker;

import io.malachai.datafaker.exception.TableNotInitializedException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class TableUpdateThread extends Thread {

    private final Long tableKey;
    private final Table table;
    private final Column primaryKey;
    private final List<Column> fieldColumns;
    private final List<Column> updatedAt;

    private final EntityManager entityManager;
    private final ConstraintManager constraintManager;
    private final DataSourceManager dataSourceManager;

    private final DataKindFaker faker = new DataKindFaker();

    public TableUpdateThread(Table table, EntityManager entityManager,
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
        this.fieldColumns = this.table.getColumns().stream()
            .filter(col -> col.getConstraint() == ConstraintType.NONE)
            .collect(Collectors.toList());
        this.updatedAt = this.table.getColumns().stream()
            .filter(col -> col.getConstraint() == ConstraintType.UPDATED_AT)
            .collect(Collectors.toList());
    }

    public void run() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (true) {
            ParamMap<String, String, Object> params = new ParamMap<>();
            if (primaryKey != null) {
                Long key;
                try {
                    key = constraintManager.getPrimaryKey(tableKey);
                } catch (TableNotInitializedException e) {
                    key = 0L;
                }
                params.put(primaryKey.getName(), primaryKey.getType(),
                    key != 0L ? random.nextLong(key) : 0L);
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
            // field columns randomizer
            for (Column field : fieldColumns) {
                params.put(field.getName(), field.getType(), faker.get(field.getKind()));
            }

            try {
                dataSourceManager.execute(entityManager.getSourceKey(table.getSourceName()),
                    mapStatement(params));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            sleepWithoutException(table.getSparsity());
        }
    }

    private String mapStatement(ParamMap<String, String, Object> params) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        for (Map.Entry<String, ParamMap.ParamEntry<String, Object>> param : params.entrySet()) {
            if (param.getKey().equals(primaryKey.getName())) {
                continue;
            }
            sb.append(param.getKey());
            sb.append("=");
            switch (param.getValue().getType()) {
                case "string":
                    sb.append(String.format("\"%s\"", param.getValue().getObj().toString()));
                    break;
                case "timestamp":
                case "date":
                    sb.append(String.format("'%s'", param.getValue().getObj().toString()));
                    break;
                default:
                    sb.append(param.getValue().getObj().toString());
            }
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        sb.append(" WHERE ");
        sb.append(this.primaryKey.getName());
        sb.append(" = ");
        sb.append(params.get(this.primaryKey.getName()).getObj().toString());
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
