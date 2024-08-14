package io.malachai.datafaker;

import java.util.List;

public class Table {

    private String fullName;
    private Long sparsity;
    private List<Column> columns;

    public Table(String fullName, Long sparsity, List<Column> columns) {
        this.fullName = fullName;
        this.sparsity = sparsity;
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
