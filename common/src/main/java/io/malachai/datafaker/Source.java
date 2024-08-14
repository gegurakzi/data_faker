package io.malachai.datafaker;

import java.util.List;

public class Source {

    private String name;
    private SourceType type;
    private String url;
    private String user;
    private String password;
    private List<Table> tables;

    public Source(String name, SourceType type, String url, String user, String password,
        List<Table> tables) {
        this.name = name;
        this.type = type;
        this.url = url;
        this.user = user;
        this.password = password;
        this.tables = tables;
    }

    public Source(Source source) {
        this.name = source.name;
        this.type = source.type;
        this.url = source.url;
        this.user = source.user;
        this.password = source.password;
        this.tables = source.tables;
    }

    public String getName() {
        return name;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public List<Table> getTables() {
        return tables;
    }

    public SourceType getType() {
        return type;
    }

    public void addTable(Table table) {
        tables.add(table);
    }
}
