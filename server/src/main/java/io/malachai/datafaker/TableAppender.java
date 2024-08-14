package io.malachai.datafaker;

import io.malachai.datafaker.exception.TableNotInitializedException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class TableAppender extends Thread {


    private final Long tableKey;
    private final Table table;
    private final Column primaryKey;
    private final List<Column> foreignKeys;
    private final List<Column> fieldColumns;

    private final EntityManager entityManager;
    private final ConstraintManager constraintManager;
    private final DataSourceManager dataSourceManager;

    private final DataKindFaker faker = new DataKindFaker();

    public TableAppender(Table table, EntityManager entityManager,
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
        this.fieldColumns = this.table.getColumns().stream()
            .filter(col -> col.getConstraint() == ConstraintType.NONE)
            .collect(Collectors.toList());
    }

    public void run() {
        try (Connection connection = dataSourceManager.getConnection(
            entityManager.getSourceKey(table.getSourceName()))) {
            connection.setAutoCommit(false);
            ThreadLocalRandom random = ThreadLocalRandom.current();
            while (true) {
                Map<String, ParamEntry> params = new HashMap<>();
                if (primaryKey != null) {
                    Long value;
                    try {
                        value = constraintManager.getPrimaryKey(tableKey);
                    } catch (TableNotInitializedException e) {
                        value = 0L;
                    }
                    addParameter(
                        primaryKey.getName(),
                        primaryKey.getType(),
                        value,
                        params
                    );
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
                    addParameter(
                        fk.getName(),
                        fk.getType(),
                        key != 0L ? random.nextLong(key) : 0L,
                        params
                    );
                }
                for (Column field : fieldColumns) {
                    addParameter(
                        field.getName(),
                        field.getType(),
                        faker.get(field.getKind()),
                        params
                    );
                }
                PreparedStatement pstmt = connection.prepareStatement(mapStatement(params));
                pstmt.execute();
                connection.commit();
                //System.out.println(mapStatement(params));
                constraintManager.increasePrimaryKey(tableKey);
                sleepWithoutException(table.getSparsity());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private String mapStatement(Map<String, ParamEntry> params) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(table.getName());
        sb.append(" (");
        sb.append(String.join(",", params.keySet()));
        sb.append(") VALUES (");
        sb.append(String.join(",",
            params.values().stream()
                .map(p -> {
                    if (p.getType().equals("string")) {
                        return String.format("\"%s\"", p.getObj().toString());
                    }
                    return p.getObj().toString();
                })
                .collect(Collectors.toList())));
        sb.append(")");
        return sb.toString();
    }

    private void addParameter(String key, String type, Object value,
        Map<String, ParamEntry> params) {
        params.put(key, new ParamEntry(type, value));
    }

    private void sleepWithoutException(Long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            //connection.close();
            throw new RuntimeException(e);
        }
    }

    private final class ParamEntry {

        final String type;
        final Object obj;

        ParamEntry(final String type, final Object obj) {
            // check parameters
            this.type = type;
            this.obj = obj;
        }

        public String getType() {
            return type;
        }

        public Object getObj() {
            return obj;
        }
    }
}
