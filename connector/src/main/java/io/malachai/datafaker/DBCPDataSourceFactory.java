package io.malachai.datafaker;

import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;

public class DBCPDataSourceFactory {

    public DBCPDataSourceFactory() {

    }

    public static BasicDataSource createDataSource(String jdbcUrl, String user, String password) {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl(jdbcUrl);
        ds.setUsername(user);
        ds.setPassword(password);
        return ds;
    }

}
