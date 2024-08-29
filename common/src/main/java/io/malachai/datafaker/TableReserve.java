package io.malachai.datafaker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableReserve {

    private final Table table;
    private final Column primaryKey;
    private final List<Column> foreignKeys = new ArrayList<>();
    private final List<Column> createdAt = new ArrayList<>();
    private final List<Column> updatedAt = new ArrayList<>();
    private final List<Column> finalColumns = new ArrayList<>();
    private final List<Column> nonFinalColumns = new ArrayList<>();

    public TableReserve(Table table) {
        this.table = table;
        this.primaryKey = table.getColumns().stream()
            .filter(col -> col.getConstraint() == ConstraintType.PRIMARY)
            .findFirst().orElse(null);
        for (Column column : table.getColumns()) {
            switch (column.getConstraint()) {
                case FOREIGN:
                    this.foreignKeys.add(column);
                    break;
                case CREATED_AT:
                    this.createdAt.add(column);
                    break;
                case UPDATED_AT:
                    this.updatedAt.add(column);
                    break;
                case FINAL:
                    this.finalColumns.add(column);
                    break;
                case NONE:
                    this.nonFinalColumns.add(column);
                    break;
            }
        }
    }

    public Table getTable() {
        return table;
    }

    public Column getPrimaryKey() {
        return primaryKey;
    }

    public List<Column> getForeignKeys() {
        return foreignKeys;
    }

    public List<Column> getCreatedAt() {
        return createdAt;
    }

    public List<Column> getUpdatedAt() {
        return updatedAt;
    }

    public List<Column> getFieldColumns() {
        return Stream.concat(finalColumns.stream(), nonFinalColumns.stream())
            .collect(Collectors.toList());
    }

    public List<Column> getNonFinalColumns() {
        return nonFinalColumns;
    }
}
