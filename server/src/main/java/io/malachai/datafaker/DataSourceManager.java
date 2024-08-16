package io.malachai.datafaker;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

public class DataSourceManager {

    private final EntityManager entityManager;

    Map<Long, DataSource> dataSources = new HashMap<>();

    public DataSourceManager(EntityManager entityManager) {
        this.entityManager = entityManager;
        initialize(new ArrayList<>(entityManager.getSources().keySet()));
    }

    public void initialize(List<Long> keys) {
        keys.forEach(k -> {
            Source source = entityManager.getSources().get(k);
            switch (source.getType()) {
                case JDBC:
                    createDBCP(k);
                    break;
                case CONSOLE:

            }
        });
    }

    public void createDBCP(Long sourceKey) {
        Source source = entityManager.getSources().get(sourceKey);
        dataSources.put(sourceKey, DBCPDataSourceFactory.createDataSource(
            source.getUrl(),
            source.getUser(),
            source.getPassword()
        ));
    }

    public void execute(Long sourceKey, String statement) throws SQLException {
        Source source = entityManager.getSources().get(sourceKey);
        switch (source.getType()) {
            case JDBC:
                try (Connection connection = dataSources.get(sourceKey).getConnection()) {
                    connection.setAutoCommit(false);
                    PreparedStatement pstmt = connection.prepareStatement(statement);
                    pstmt.execute();
                    connection.commit();
                    return;
                } catch (SQLException e) {
                    throw e;
                }
            case CONSOLE:
                System.out.println(String.format("Source [%s]: %s", source.getName(), statement));
                return;
        }

    }

}
