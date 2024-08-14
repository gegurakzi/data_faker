package io.malachai.datafaker;

import java.sql.Connection;
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
        keys.forEach(this::createDBCP);
    }

    public void createDBCP(Long sourceKey) {
        Source source = entityManager.getSources().get(sourceKey);
        dataSources.put(sourceKey, DBCPDataSourceFactory.createDataSource(
            source.getUrl(),
            source.getUser(),
            source.getPassword()
        ));
    }

    public Connection getConnection(Long sourceKey) {
        try {
            return dataSources.get(sourceKey).getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
