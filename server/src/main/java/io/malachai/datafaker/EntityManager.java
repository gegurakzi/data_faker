package io.malachai.datafaker;

import java.util.HashMap;
import java.util.Map;

public class EntityManager {

    private final Map<Long, Source> sources;
    private final Map<Long, Table> tables;
    private final Map<Long, Column> columns;

    private Long sourceId = 0L;
    private Long tableId = 0L;
    private Long columnId = 0L;

    public EntityManager() {
        sources = new HashMap<>();
        tables = new HashMap<>();
        columns = new HashMap<>();
    }

    public Map<Long, Source> getSources() {
        return sources;
    }

    public Map<Long, Table> getTables() {
        return tables;
    }

    public Map<Long, Column> getColumns() {
        return columns;
    }

    public void addSource(Source source) {
        sources.put(sourceId, source);
        source.getTables().forEach(table -> {
            tables.put(tableId, table);
            table.getColumns().forEach(column -> {
                columns.put(columnId, column);
                columnId++;
            });
            tableId++;
        });
        sourceId++;
    }

    public Source getSource(String sourceName) {
        for (Map.Entry<Long, Source> sourceEntry : sources.entrySet()) {
            if (sourceEntry.getValue().getName().equals(sourceName)) {
                return sourceEntry.getValue();
            }
        }
        return null;
    }

    public Table getTable(String tableFullName) {
        for (Table table : tables.values()) {
            if (table.getFullName().equals(tableFullName)) {
                return table;
            }
        }
        return null;
    }

    public Column getColumn(String columnFullName) {
        for (Column column : columns.values()) {
            if (column.getFullName().equals(columnFullName)) {
                return column;
            }
        }
        return null;
    }

    public Long getSourceKey(String sourceName) {
        for (Map.Entry<Long, Source> sourceEntry : sources.entrySet()) {
            if (sourceEntry.getValue().getName().equals(sourceName)) {
                return sourceEntry.getKey();
            }
        }
        return null;
    }

    public Long getTableKey(String tableFullName) {
        for (Map.Entry<Long, Table> tableEntry : tables.entrySet()) {
            if (tableEntry.getValue().getFullName().equals(tableFullName)) {
                return tableEntry.getKey();
            }
        }
        return null;
    }

}
