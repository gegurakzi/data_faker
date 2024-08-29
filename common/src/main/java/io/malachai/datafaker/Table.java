package io.malachai.datafaker;

import java.util.List;

public class Table {

    private String fullName;
    private Long insertInterval;
    private Long updateInterval;
    private String updateMode;
    private List<Column> columns;

    public Table(String fullName, Long insertInterval, String updateMode, Long updateInterval,
        List<Column> columns) {
        this.fullName = fullName;
        this.insertInterval = insertInterval;
        this.updateMode = updateMode;
        this.updateInterval = updateInterval;
        this.columns = columns;
    }

    public String getName() {
        return fullName.split("\\.")[1];
    }

    public String getFullName() {
        return fullName;
    }

    public Long getInsertInterval() {
        return insertInterval;
    }

    public String getUpdateMode() {
        return updateMode;
    }

    public Long getUpdateInterval() {
        return updateInterval;
    }

    public String getSourceName() {
        return fullName.split("\\.")[0];
    }

    public List<Column> getColumns() {
        return columns;
    }

}
