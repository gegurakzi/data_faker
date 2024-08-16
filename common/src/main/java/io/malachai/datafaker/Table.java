package io.malachai.datafaker;

import java.util.List;

public class Table {

    private String fullName;
    private Long sparsity;
    private String updateMode;
    private List<Column> columns;

    public Table(String fullName, Long sparsity, String updateMode, List<Column> columns) {
        this.fullName = fullName;
        this.sparsity = sparsity;
        this.updateMode = updateMode;
        this.columns = columns;
    }

    public String getName() {
        return fullName.split("\\.")[1];
    }

    public String getFullName() {
        return fullName;
    }

    public Long getSparsity() {
        return sparsity;
    }

    public String getUpdateMode() {
        return updateMode;
    }

    public String getSourceName() {
        return fullName.split("\\.")[0];
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void addColumn(Column column) {
        columns.add(column);
    }

}
