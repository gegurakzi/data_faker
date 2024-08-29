package io.malachai.datafaker.parser;

import io.malachai.datafaker.Column;
import io.malachai.datafaker.ConstraintType;
import io.malachai.datafaker.Source;
import io.malachai.datafaker.SourceType;
import io.malachai.datafaker.Table;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JSONConfigParser implements ConfigParser {

    private static final String SOURCES = "sources";
    private static final String SOURCE_NAME = "name";
    private static final String SOURCE_TYPE = "type";
    private static final String SOURCE_URL = "url";
    private static final String SOURCE_USER = "user";
    private static final String SOURCE_PASSWORD = "password";
    private static final String TABLES = "tables";
    private static final String TABLE_NAME = "name";
    private static final String TABLE_INSERT_INTERVAL = "insertInterval";
    private static final Long DEFAULT_TABLE_INSERT_INTERVAL = 10000L;
    private static final String TABLE_UPDATE_INTERVAL = "updateInterval";
    private static final Long DEFAULT_TABLE_UPDATE_INTERVAL = Long.MAX_VALUE;
    private static final String TABLE_UPDATE_MODE = "update";
    private static final String DEFAULT_TABLE_UPDATE_MODE = "random";
    private static final String TABLE_INITIAL_PRIMARY_KEY = "initialPrimaryKey";
    private static final Long DEFAULT_TABLE_INITIAL_PRIMARY_KEY = 0L;
    private static final String COLUMNS = "columns";
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_TYPE = "type";
    private static final String COLUMN_KIND = "kind";
    private static final String COLUMN_CONSTRAINT = "constraint";
    private static final ConstraintType DEFAULT_COLUMN_CONSTRAINT = ConstraintType.NONE;
    private static final String COLUMN_REFERS = "refers";


    public List<Source> parse(Reader reader) throws IOException {
        Map<String, Column> parsedColumns = new HashMap<>();
        try {
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(reader);
            final List<Source> sources = new ArrayList<>();
            JSONArray sourcesObj = (JSONArray) json.get(SOURCES);
            for (Object sObj : sourcesObj) {
                JSONObject sourceObj = (JSONObject) sObj;
                String sourceFullName = sourceObj.get(SOURCE_NAME).toString();
                final List<Table> tables = new ArrayList<>();
                JSONArray tablesObj = (JSONArray) sourceObj.get(TABLES);
                for (Object tObj : tablesObj) {
                    JSONObject tableObj = (JSONObject) tObj;
                    String tableFullName = String.format("%s.%s", sourceFullName,
                        tableObj.get(TABLE_NAME).toString());
                    List<Column> columns = new ArrayList<>();
                    JSONArray columnsObj = (JSONArray) tableObj.get(COLUMNS);
                    for (Object cObj : columnsObj) {
                        JSONObject columnObj = (JSONObject) cObj;
                        String columnFullName = String.format("%s.%s", tableFullName,
                            columnObj.get(COLUMN_NAME).toString());
                        final Column column = new Column(
                            columnFullName,
                            (String) columnObj.get(COLUMN_TYPE),
                            (String) columnObj.get(COLUMN_KIND),
                            getColumnConstraint(columnObj),
                            (String) columnObj.get(COLUMN_REFERS)
                        );
                        // TODO: validate column config
                        parsedColumns.put(columnFullName, column);

                        columns.add(column);
                    }
                    final Table table = new Table(
                        tableFullName,
                        getTableInsertInterval(tableObj),
                        getUpdateMode(tableObj),
                        getTableUpdateInterval(tableObj),
                        columns
                    );
                    tables.add(table);
                }
                String sourceType = (String) sourceObj.get(SOURCE_TYPE);
                final Source source = new Source(
                    sourceFullName,
                    SourceType.valueOf(sourceType.toUpperCase()),
                    (String) sourceObj.get(SOURCE_URL),
                    (String) sourceObj.get(SOURCE_USER),
                    (String) sourceObj.get(SOURCE_PASSWORD),
                    tables
                );
                sources.add(source);
            }

            for (Map.Entry<String, Column> parsedColumn : parsedColumns.entrySet()) {
                if (parsedColumn.getValue().getConstraint() != ConstraintType.FOREIGN) {
                    continue;
                }
                try {
                    parsedColumns.get(parsedColumn.getKey())
                        .setReference(parsedColumns.get(parsedColumn.getValue().getRefers()));
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new RuntimeException(
                        "column refers parsing exception: should be 'source.table.column' format");
                }
            }

            return sources;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public Long getTableInsertInterval(JSONObject jsonObj) {
        if (jsonObj.get(TABLE_INSERT_INTERVAL) == null) {
            return DEFAULT_TABLE_INSERT_INTERVAL;
        }
        return Long.parseLong(jsonObj.get(TABLE_INSERT_INTERVAL).toString());
    }

    public Long getTableUpdateInterval(JSONObject jsonObj) {
        if (jsonObj.get(TABLE_UPDATE_INTERVAL) == null) {
            return DEFAULT_TABLE_UPDATE_INTERVAL;
        }
        return Long.parseLong(jsonObj.get(TABLE_UPDATE_INTERVAL).toString());
    }

    public ConstraintType getColumnConstraint(JSONObject jsonObj) {
        if (jsonObj.get(COLUMN_CONSTRAINT) == null) {
            return DEFAULT_COLUMN_CONSTRAINT;
        }
        return ConstraintType.valueOf(jsonObj.get(COLUMN_CONSTRAINT).toString().toUpperCase());
    }

    public Long getInitialPrimaryKey(JSONObject jsonObj) {
        if (jsonObj.get(TABLE_INITIAL_PRIMARY_KEY) == null) {
            return DEFAULT_TABLE_INITIAL_PRIMARY_KEY;
        }
        return Long.parseLong(jsonObj.get(TABLE_INITIAL_PRIMARY_KEY).toString());
    }

    public String getUpdateMode(JSONObject jsonObj) {
        if (jsonObj.get(TABLE_UPDATE_MODE) == null) {
            return DEFAULT_TABLE_UPDATE_MODE;
        }
        return jsonObj.get(TABLE_UPDATE_MODE).toString();
    }

}
